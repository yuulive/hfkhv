
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::ffi::OsStr;
use std::fs;
use std::iter::FromIterator;
use std::ops::Sub;
use std::path::PathBuf;
use anyhow;
use anyhow::Context;
use postgres;
use postgres_native_tls;
use native_tls;
use crate::project::DatabaseProject;
use crate::project::DatabaseObject;
use crate::project::DatabaseObjectType;
use crate::utils;



fn get_pg_client_from_connection_string(connection_string: &str) -> anyhow::Result<postgres::Client> {
    
    let root_cert_path = utils::read_env_var("PGFINE_ROOT_CERT")?;
    if root_cert_path == "" {
        let pg_client = postgres::Client::connect(&connection_string, postgres::NoTls)
            .context("failed to connect to db using no TLS")?;
        return Ok(pg_client);
    }


    let root_cert_path_buf = PathBuf::from(root_cert_path);
    let root_cert_b = fs::read(&root_cert_path_buf)
        .context(format!("failed to read cert file {:?}", root_cert_path_buf))?;

    let root_cert;
    
    match root_cert_path_buf.extension().and_then(OsStr::to_str) {
        Some("pem") => {
            root_cert = native_tls::Certificate::from_pem(&root_cert_b)?;
        },
        Some("der") => {
            root_cert = native_tls::Certificate::from_der(&root_cert_b)?;
        },
        _ => {
            match native_tls::Certificate::from_pem(&root_cert_b) {
                Ok(pem_cert) => {
                    root_cert = pem_cert;
                }
                Err(pem_err) => {
                    match native_tls::Certificate::from_der(&root_cert_b) {
                        Ok(der_cert) => {
                            root_cert = der_cert;
                        }
                        Err(der_err) => {
                            return Err(pem_err)
                            .context(der_err)
                            .context(format!("could not parse cert file neither as pem nor der format {:?}", root_cert_path_buf));
                        }
                    }
                }
            }
        }
    }
    
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(root_cert)
        .build()
        .context("TLS configuration error")?;

    let connector = postgres_native_tls::MakeTlsConnector::new(connector);
    
    let pg_client = postgres::Client::connect(&connection_string, connector)
        .context("failed to connect to db using TLS")?;
    return Ok(pg_client);
}

fn get_admin_pg_client() -> anyhow::Result<postgres::Client> {
    let admin_connection_string = utils::read_env_var("PGFINE_ADMIN_CONNECTION_STRING")
        .context("get_admin_pg_client error: failed to get connection string from env PGFINE_ADMIN_CONNECTION_STRING")?;
    let admin_pg_client = get_pg_client_from_connection_string(&admin_connection_string)
        .context("get_admin_pg_client error: failed to connect to database using PGFINE_ADMIN_CONNECTION_STRING")?;
    return Ok(admin_pg_client);
}

fn get_pg_client() -> anyhow::Result<postgres::Client> {
    let connection_string = utils::read_env_var("PGFINE_CONNECTION_STRING")
        .context("get_admin_pg_client error: failed to get connection string from env PGFINE_CONNECTION_STRING")?;
    let pg_client = get_pg_client_from_connection_string(&connection_string)
        .context("get_admin_pg_client error: failed to connect to database using PGFINE_CONNECTION_STRING")?;
    return Ok(pg_client);
}


fn update_pgfine_object(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject
) -> anyhow::Result<()> {
    let sql = "
        insert into pgfine_objects (
            po_id,
            po_type,
            po_md5,
            po_script,
            po_path,
            po_depends_on,
            po_required_by
        )
        select $1, $2, $3, $4, $5, $6, $7
        on conflict (po_id) do update set 
            po_type = excluded.po_type,
            po_md5 = excluded.po_md5,
            po_script = excluded.po_script,
            po_path = excluded.po_path,
            po_depends_on = excluded.po_depends_on,
            po_required_by = excluded.po_required_by;";

    let object_type_str: String = object.object_type.into();
    let path_str: String = object.path_buf.clone().into_os_string().to_str()
        .ok_or(anyhow!("object_id_from_path error: could not parse filename"))?
        .into();
    
    let depends_on_vec: Vec<&String> = Vec::from_iter(&object.depends_on);
    let required_by_vec: Vec<&String> = Vec::from_iter(&object.required_by);

    pg_client.execute(sql, &[
        &object.id, 
        &object_type_str, 
        &object.md5,
        &object.script,
        &path_str,
        &depends_on_vec,
        &required_by_vec
    ])?;
    return Ok(());
}

fn delete_pgfine_object(
    pg_client: &mut postgres::Client,
    object_id: &str
) -> anyhow::Result<()> {
    let sql = "delete from pgfine_objects where lower(po_id) = lower($1)";
    pg_client.execute(sql, &[&object_id])
        .context(format!("delete_pgfine_object failed {:?}", object_id))?;
    return Ok(());
}

fn exists_object(
    pg_client: &mut postgres::Client, 
    object: &DatabaseObject
) -> anyhow::Result<bool> {
    let sql = match object.object_type {
        DatabaseObjectType::Table => "select to_regclass($1) is not null;", // pg 9.4+,
        DatabaseObjectType::View => "select to_regclass($1) is not null;", // pg 9.4+,
        DatabaseObjectType::Function => "
            select exists (
                select 1
                from pg_proc p
                join pg_namespace n on n.oid = p.pronamespace
                where lower(n.nspname || '.' || p.proname) = lower($1)
            );",
        DatabaseObjectType::Constraint => "
            select exists (
                select 1
                from pg_constraint c
                join pg_class t on t.oid = c.conrelid
                join pg_namespace n on n.oid = t.relnamespace
                where lower(n.nspname || '.' || t.relname || '.' || c.conname) = lower($1)
            );",
        DatabaseObjectType::Role => "
            select exists (
                select 1
                from pg_roles
                where lower(rolname) = lower($1)
            );",
        DatabaseObjectType::Trigger => "
            select exists (
                select 1
                from pg_trigger t
                join pg_class c on c.oid = t.tgrelid
                join pg_namespace n on n.oid = c.relnamespace
                where lower(n.nspname || '.' || c.relname || '.' || t.tgname) = lower($1)
            );",
        DatabaseObjectType::Schema => "
            select exists (
                select 1
                from pg_namespace
                where nspname = $1
            );",
    };
    let row = pg_client.query_one(sql, &[&object.id])
        .context(format!("exists_object error quering {:?} {:?}", object.object_type, object.id))?;
    let exists: bool = row.try_get(0)
        .context(format!("exists_object error parsing {:?} {:?}", object.object_type, object.id))?;
    return Ok(exists);
}


fn drop_object(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject
) -> anyhow::Result<()> {
    println!("drop if exists {:?} {:?}", object.object_type, object.id);
    let exists = exists_object(pg_client, object)?;
    if exists {
        match object.object_type {
            DatabaseObjectType::Table => bail!("attempting to drop a table {:?}, \
                tables should be dropped manually or using migration scripts", object.id),
            DatabaseObjectType::View => {
                let sql = format!("drop view {};", object.id);
                pg_client.batch_execute(&sql)?;
            },
            DatabaseObjectType::Function => {
                let sql = format!("drop function {};", object.id);
                pg_client.batch_execute(&sql)?;
            },
            DatabaseObjectType::Constraint => {
                let schema = object.schema()?;
                let table = object.table()?;
                let constraint_name = object.name()?;
                let table_id = format!("{}.{}", schema, table);
                let drop_constraint_sql = format!("alter table {table_id} drop constraint {constraint_name};",
                    table_id=table_id,
                    constraint_name=constraint_name
                );

                pg_client.batch_execute(&drop_constraint_sql)?;
            },
            DatabaseObjectType::Role => {
                let pgfine_role = utils::get_role_name()?;
                let sql = format!("
                    grant {drop_role_name} to {pgfine_role};
                    reassign owned by {drop_role_name} to {pgfine_role};
                    drop owned by {drop_role_name};
                    drop role {drop_role_name};",
                    drop_role_name=object.id,
                    pgfine_role=pgfine_role,
                );
                
                pg_client.batch_execute(&sql)?;
                return Ok(());
            },
            DatabaseObjectType::Trigger => {
                let schema = object.schema()?;
                let table = object.table()?;
                let trigger_name = object.name()?;
                let table_id = format!("{}.{}", schema, table);
                let drop_trigger_sql = format!("drop trigger {trigger_name} ON {table_id};",
                    table_id=table_id,
                    trigger_name=trigger_name
                );

                pg_client.batch_execute(&drop_trigger_sql)?;
            },
            DatabaseObjectType::Schema => {
                let sql = format!("drop schema {};", object.id);
                pg_client.batch_execute(&sql)?;
            },
        };
    }

    delete_pgfine_object(pg_client, &object.id)?;
    return Ok(());
}

fn drop_object_with_deps(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject,
    objects: &HashMap<String, DatabaseObject>,
    dropped: &mut HashSet<String>,
    visited: &mut HashSet<String>,
) -> anyhow::Result<()> {
    if dropped.contains(&object.id) {
        return Ok(());
    }

    if visited.contains(&object.id) {
        bail!("drop_object_with_deps: cycle detected {:?}", object.id);
    }
    visited.insert(object.id.clone());

    for dep_id in object.required_by.iter() {
        let dep = objects.get(dep_id)
            .ok_or(anyhow!("object cannot be dropped because it depends on another object which cannot be droped {:?} {:?}", object.id, dep_id))?;
        drop_object_with_deps(pg_client, &dep, &objects, dropped, visited)?;
    }

    drop_object(pg_client, &object)?;
    dropped.insert(object.id.clone());
    return Ok(());
}


fn force_drop_role_if_exists(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject
) -> anyhow::Result<()> {
    if object.object_type != DatabaseObjectType::Role {
        panic!("force_drop_role: object.object_type != DatabaseObjectType::Role");
    }

    let exists = exists_object(pg_client, object)?;
    if !exists {
        return Ok(());
    }

    println!("force drop role {:?}", object.id);

    let role_name = utils::get_role_name()?;
    
    let sql = format!(
        "
        grant {drop_role_name} to {role_name};
        reassign owned by {drop_role_name} to {role_name};
        drop owned by {drop_role_name} cascade;
        drop role {drop_role_name};
        ", 
        drop_role_name=object.id,
        role_name=role_name,
    );

    pg_client.batch_execute(&sql)?;
    return Ok(());
}

fn exists_pgfine_object(
    pg_client: &mut postgres::Client,
    object_id: &str,
) -> anyhow::Result<bool> {
    let sql = "
        select exists (
            select 1
            from pgfine_objects
            where po_id = $1
        );";
    
    let row = pg_client.query_one(sql, &[&object_id])?;
    let result = row.try_get(0)?;
    return Ok(result);
}

fn create_if_missing(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject,
) -> anyhow::Result<()> {
    let exists = exists_object(pg_client, object)?;
    if exists {
        let pgfine_exists = exists_pgfine_object(pg_client, &object.id)?;
        if !pgfine_exists {
            println!("create missing pgfine_objects record {:?} {:?}", object.object_type, object.id);
            update_pgfine_object(pg_client, &object)?;
        }
        return Ok(());
    }
    println!("create {:?} {:?}", object.object_type, object.id);
    pg_client.batch_execute(&object.script)?;
    update_pgfine_object(pg_client, &object)?;
    return Ok(());
}


fn prepare_admin_script(template_str: &str) -> anyhow::Result<String> {
    let database_name = utils::get_database_name()?;
    let role_name = utils::get_role_name()?;
    let password = utils::get_password()?;
    let mut result = template_str.replace("{database_name}", &database_name);
    result = result.replace("{role_name}", &role_name);
    if let Some(p) = password {
        result = result.replace("{password}", &p);
    } else {
        if result.contains("{password}") {
            bail!("admin script expects password parameter to be provided");
        }
    }
    return Ok(result);
}

fn exists_database(
    admin_pg_client: &mut postgres::Client
) -> anyhow::Result<bool> {
    let sql = "select exists (select 1 
        from pg_database
        where datname = $1
    )";
    let database_name = utils::get_database_name()?;
    let row = admin_pg_client.query_one(sql, &[&database_name])?;
    let exists: bool = row.try_get(0)?;
    return Ok(exists);
}

fn create_database(
    admin_pg_client: &mut postgres::Client,
    database_project: &DatabaseProject
) -> anyhow::Result<()> {
    for (path_buf, script) in database_project.create_scripts.iter() {
        println!("create_database: executing {:?}", path_buf);
        let prepared_script = prepare_admin_script(&script)?;
        admin_pg_client.batch_execute(&prepared_script)
            .with_context(|| format!("create error: failed to execute script: {:?}", path_buf))?;
    }
    println!("create_database: fresh database created");
    return Ok(());
}

fn create_pgfine_tables(
    pg_client: &mut postgres::Client
) -> anyhow::Result<()> {
    let pgfine_objects_sql = "
        create table if not exists pgfine_objects (
            po_id text primary key,
            po_type text,
            po_md5 text,
            po_script text,
            po_path text,
            po_depends_on text[],
            po_required_by text[]
        );";

    pg_client.batch_execute(pgfine_objects_sql)
        .context("failed to create pgfine_objects table")?;

    let pgfine_version_sql = "
        create table if not exists pgfine_migrations (
            pm_id text primary key
        );";
    
    pg_client.batch_execute(pgfine_version_sql)?;

    return Ok(());
}


fn select_db_objects(
    pg_client: &mut postgres::Client
) -> anyhow::Result<HashMap<String, DatabaseObject>> {
    let mut result = HashMap::new();
    let sql = "select * from pgfine_objects;";
    let rows = pg_client.query(sql, &[])?;
    for row in rows {
        let object = DatabaseObject::from_db_row(&row)
            .context("failed to parse pgfine_objects row")?;
        result.insert(object.id.clone(), object);
    }
    return Ok(result);
}


fn collect_required_by(
    set: &mut HashSet<String>,
    objects: &HashMap<String, DatabaseObject>
) {
    let mut ll = LinkedList::from_iter(set.clone());
    while ll.len() > 0 {
        let object_id = ll.pop_front().unwrap();
        let object_opt = objects.get(&object_id);

        if let Some(object) = object_opt {
            for req_id in object.required_by.iter() {
                if !set.contains(req_id) {
                    ll.push_back(req_id.clone());
                }
            }
        }

        if !set.contains(&object_id) {
            set.insert(object_id.clone());
        }
    }
}


fn update_objects(
    pg_client: &mut postgres::Client,
    database_project: &DatabaseProject
) -> anyhow::Result<()> {

    let db_objects = select_db_objects(pg_client)?;
    
    let mut drop_set: HashSet<String> = HashSet::new();
    let mut dirty_tables_set: HashSet<String> = HashSet::new();

    for (db_object_id, db_object) in db_objects.iter() {
        if !database_project.objects.contains_key(db_object_id) {
            if db_object.object_type == DatabaseObjectType::Table {
                dirty_tables_set.insert(db_object_id.clone());
            } else {
                drop_set.insert(db_object_id.clone());
            }
        } else {
            let p_object = &database_project.objects[db_object_id];
            if p_object.md5 != db_object.md5 {
                if db_object.object_type == DatabaseObjectType::Table {
                    dirty_tables_set.insert(db_object_id.clone());
                } else if db_object.object_type == DatabaseObjectType::Schema {
                    println!("schema has changed but won't be updated, to alter schema you should use migrations {:?}", db_object_id);
                    continue;
                } else {
                    drop_set.insert(db_object_id.clone());
                }
            }
        }
    }


    // drop p_objects which are missing in pgfine_objects and still exist in database (except schemas)
    for (p_object_id, p_object) in database_project.objects.iter() {
        if db_objects.contains_key(p_object_id) {
            continue;
        }

        let exists = exists_object(pg_client, p_object)?;
        if exists && p_object.object_type != DatabaseObjectType::Schema {
            drop_set.insert(p_object_id.clone());
        }
    }


    // check tables
    let mut dirty_tables_sorted = Vec::from_iter(&dirty_tables_set);
    dirty_tables_sorted.sort();
    for dirty_table_id in dirty_tables_sorted {
        let object = &db_objects[dirty_table_id];
        let exists = exists_object(pg_client, object)?;
        let deleted = !database_project.objects.contains_key(dirty_table_id);
        if exists && deleted {
            bail!("table was deleted from project, but it still exists in database, \
            it should be dropped manually or using migrations scripts {:?}", dirty_table_id);
        } else if (!exists) && deleted {
            println!("deleting pgfine_objects record for table {:?}", dirty_table_id);
            delete_pgfine_object(pg_client, dirty_table_id)?;
        } else if exists && (!deleted) {
            println!("table script was modified, overwriting pgfine_objects record {:?}", dirty_table_id);
            let p_object = &database_project.objects[dirty_table_id];
            update_pgfine_object(pg_client, &p_object)?;
        }
        
        // else table will be created in later step
    }



    collect_required_by(&mut drop_set, &db_objects);

    let mut dropped: HashSet<String> = HashSet::new();
    let mut drop_list = Vec::from_iter(drop_set.clone());
    let mut last_error: Option<anyhow::Error> = None;
    while drop_set.len() > 0 {
        let dropped_len = dropped.len();
        
        drop_list.sort();
        for drop_object_id in drop_list {
            let mut visited: HashSet<String> = HashSet::new();
            
            let object;
            if db_objects.contains_key(&drop_object_id) {
                object = &db_objects[&drop_object_id];
            } else if database_project.objects.contains_key(&drop_object_id) {
                object = &database_project.objects[&drop_object_id];
            } else {
                println!("failed to drop, missing pgfine_objects {:?}", drop_object_id);
                last_error = Some(anyhow!("failed to drop, missing pgfine_objects {:?}", drop_object_id));
                continue;
            }
            

            let drop_result = drop_object_with_deps(pg_client, &object, &db_objects, &mut dropped, &mut visited);
            if drop_result.is_err() {
                println!("failed to drop {:?} {:?}", object.object_type, object.id);
                last_error = drop_result.err();
            }
        }

        drop_set = drop_set.sub(&dropped);
        drop_list = Vec::from_iter(drop_set.clone());
        
        if dropped.len() == dropped_len {
            if let Some(e) = last_error {
                return Err(e.context(format!("ubdate_objects error: could not drop these objects {:?}", drop_list)));
            } else {
                bail!("ubdate_objects error: could not drop these objects {:?}", drop_list);
            }
        }
        if drop_set.len() > 0 {
            println!("one more drop iteration will be attempted {:?}", drop_list);
        }
    }

    let create_order = database_project.get_create_order()
        .context("update_objects error: could not get create order")?;

    for object_id in create_order.iter() {
        let object = database_project.objects.get(object_id).unwrap();
        create_if_missing(pg_client, &object)
            .context(format!("update_objects error: could not create {:?} {:?}", object.object_type, object.id))?;
    }
    
    return Ok(());
}

fn insert_pgfine_migration(
    pg_client: &mut postgres::Client,
    migration: &str
) -> anyhow::Result<()> {
    let sql = "
        insert into pgfine_migrations (pm_id)
        select $1
        on conflict (pm_id) do nothing;";
    pg_client.execute(sql, &[&migration])?;
    return Ok(());
}


fn get_db_last_migration(pg_client: &mut postgres::Client) -> anyhow::Result<Option<String>> {
    let sql = "select max(pm_id) from pgfine_migrations;";
    let row = pg_client.query_one(sql, &[])?;
    let result = row.try_get(0)?;
    return Ok(result);
}


pub fn migrate(database_project: DatabaseProject) -> anyhow::Result<()> {

    let project_last_migration_opt = database_project.migration_scripts.last();
    let pg_client_result = get_pg_client();
    
    match pg_client_result {
        Err(_) => {
            println!("database was not found, will attempt to create a fresh one and create all database objects");
            
            let mut admin_pg_client = get_admin_pg_client()
                .context("migrate error: could not connect to database neither using PGFINE_CONNECTION_STRING nor PGFINE_ADMIN_CONNECTION_STRING")?;

            if exists_database(&mut admin_pg_client)? {
                bail!("migrate error: database exists but could not get connection to it, check PGFINE_CONNECTION_STRING");
            }

            create_database(&mut admin_pg_client, &database_project)
                .context("migrate error: could not create a new database")?;

            let mut pg_client = get_pg_client()
                .context("migrate error: could not connect to database after it was created")?;

            create_pgfine_tables(&mut pg_client)
                .context("migrate error: could not create pgfine tables in new database")?;

            update_objects(&mut pg_client, &database_project)
                .context("migrate error: failed to create database objects in new database")?;

            if let Some((project_last_migration, _)) = project_last_migration_opt {
                insert_pgfine_migration(&mut pg_client, &project_last_migration)
                    .context(format!("migrate error: could not insert the last migration {:?}", project_last_migration))?;
            } else {
                insert_pgfine_migration(&mut pg_client, "")
                    .context("migrate error: could not insert initial migration")?;
            }
        },
        Ok(mut pg_client) => {
            create_pgfine_tables(&mut pg_client)
                .context("migrate error: could not create pgfine tables")?;

            let db_last_migration_opt = get_db_last_migration(&mut pg_client)
                .context("migrate error: could not select the last migration")?;

            match db_last_migration_opt {
                Some(db_last_migration) => {
                    let mut db_last_migration_current = db_last_migration;
                    loop {
                        if let Some((next_migration_id, next_migration_script)) 
                            = database_project.get_next_migration(&db_last_migration_current) 
                        {
                            println!("execute migration script {:?}", next_migration_id);
                            pg_client.batch_execute(&next_migration_script)
                                .context(format!("migrate error: failed to execute migration script {:?}", next_migration_id))?;
                            
                            insert_pgfine_migration(&mut pg_client, &next_migration_id)
                                .context(format!("migrate error: failed to mark migration as executed, you should insert \
                                    migration into pgfine_migrations manually to fix possible issues {:?}", next_migration_id))?;

                            db_last_migration_current = get_db_last_migration(&mut pg_client)?
                                .ok_or(anyhow!("migrate error: failed to select latest migration after executing migration script {:?}", next_migration_id))?;

                        } else {
                            break;
                        }
                    }
                    update_objects(&mut pg_client, &database_project)
                        .context("migrate error: failed to update database objects")?;
                },
                None => {
                    println!("database has no initial migration, last migration found in pgfine project will be marked as executed.");
                    update_objects(&mut pg_client, &database_project)
                        .context("migrate error: failed to update database objects after no initial migration was found")?;

                    if let Some((project_last_migration, _)) = project_last_migration_opt {
                        insert_pgfine_migration(&mut pg_client, &project_last_migration)
                            .context(format!("migrate error: could not insert the last migration after no initial migration was found {:?}", project_last_migration))?;
                    } else {
                        insert_pgfine_migration(&mut pg_client, "")
                            .context("migrate error: could not insert initial migration after no initial migration was found")?;
                    }
                }
            }
        }
    }
    return Ok(());
}


pub fn drop(database_project: DatabaseProject) -> anyhow::Result<()> {

    let mut pg_admin_client = get_admin_pg_client()
        .context("drop error: failed to connect as admin")?;


    let pg_client_result = get_pg_client();

    match pg_client_result {
        Ok(mut pg_client) => {
            let db_objects = select_db_objects(&mut pg_client)?;
            for (db_object_id, db_object) in db_objects.iter() {
                if db_object.object_type == DatabaseObjectType::Role {
                    force_drop_role_if_exists(&mut pg_client, db_object)
                        .context(format!("drop error: failed to drop role, drop it manually or remove it from pgfine_objects table {:?}", db_object_id))?;
                }
            }
        
            for (p_object_id, p_object) in database_project.objects.iter() {
                if p_object.object_type == DatabaseObjectType::Role {
                    force_drop_role_if_exists(&mut pg_client, p_object)
                        .context(format!("drop error: failed to drop role, drop it manually or remove it from the project {:?}", p_object_id))?;
                }
            }

            pg_client.close()
                .context("drop error: could not properly close connection before dropping database, try again?")?;
        },
        Err(err) => {
            let exists = exists_database(&mut pg_admin_client)?;
            if exists {
                return Err(err)
                    .context("drop error: database exists but could not get connection to it, check PGFINE_CONNECTION_STRING");
            }
        }
    }

    for (path_buf, script) in database_project.drop_scripts {
        println!("drop database: executing {:?}", path_buf);
        let prepared_script = prepare_admin_script(&script)
            .context(format!("drop error: failed to prepare drop script {:?}", path_buf))?;
        pg_admin_client.batch_execute(&prepared_script)
            .context(format!("drop error: failed to execute drop script: {:?}", path_buf))?;
    }


    // check existing roles
    for (p_object_id, p_object) in database_project.objects.iter() {
        if p_object.object_type == DatabaseObjectType::Role {
            let role_exists = exists_object(&mut pg_admin_client, p_object)
                .context(format!("drop error: failed to check if role exists {:?}", p_object_id))?;
            if role_exists {
                println!("role still exists after executing all drop scripts, drop it manually or remove it from the project {:?}", p_object_id);
            }
        }
    }

    return Ok(());
}

