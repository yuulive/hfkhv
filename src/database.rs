
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::FromIterator;
use anyhow;
use anyhow::Context;
use postgres;
use crate::project::DatabaseProject;
use crate::project::DatabaseObject;
use crate::project::DatabaseObjectType;
use crate::utils;



fn get_admin_pg_client() -> anyhow::Result<postgres::Client> {
    let admin_connection_string = utils::read_env_var("PGFINE_ADMIN_CONNECTION_STRING")
        .context("get_admin_pg_client error: failed to get connection string from env PGFINE_ADMIN_CONNECTION_STRING")?;
    
    // FIXME match tlsMode
    let admin_pg_client = postgres::Client::connect(&admin_connection_string, postgres::NoTls)
        .context("get_admin_pg_client error: failed to connect to db using PGFINE_ADMIN_CONNECTION_STRING")?;

    return Ok(admin_pg_client);
}

fn get_pg_client() -> anyhow::Result<postgres::Client> {
    let connection_string = utils::read_env_var("PGFINE_CONNECTION_STRING")
        .context("get_pg_client error: failed to get connection string from env PGFINE_CONNECTION_STRING")?;
    
    // FIXME match tlsMode
    let pg_client = postgres::Client::connect(&connection_string, postgres::NoTls)
        .context("get_admin_pg_client error: failed to connect to db using PGFINE_CONNECTION_STRING")?;

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
                where lower(n.nspname) || '.' || lower(c.conname) = lower($1)
            );",
    };
    let row = pg_client.query_one(sql, &[&object.id])
        .context(format!("exists_object error quering {:?} {:?}", object.object_type, object.id))?;
    let exists: bool = row.try_get(0)
        .context(format!("exists_object error parsing {:?} {:?}", object.object_type, object.id))?;
    return Ok(exists);
}



fn update_table(pg_client: &mut postgres::Client, object: &DatabaseObject) -> anyhow::Result<()> {
    let exists = exists_object(pg_client, object)?;
    if !exists {
        println!("create table {:?}", object.id);
        pg_client.batch_execute(&object.script)?;
    } else {
        println!("table exists {:?}", object.id);
    }
    update_pgfine_object(pg_client, object)?;
    return Ok(());
}

fn check_hash(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject
) -> anyhow::Result<bool> {
    let sql = "
        select max(po_md5) as po_md5 
        from pgfine_objects 
        where lower(po_id) = lower($1)
    ";
    let row = pg_client.query_one(sql, &[&object.id])?;
    let md5_old_opt: Option<String> = row.try_get(0)?;
    if let Some(md5_old) = md5_old_opt {
        return Ok(md5_old == object.md5);
    }
    return Ok(false);
}


fn drop_object(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject
) -> anyhow::Result<()> {
    println!("drop {:?} {:?}", object.object_type, object.id);
    match object.object_type {
        DatabaseObjectType::Table => bail!("attempting to drop a table {:?}, \
            it could be that a table is dependent on a missing object, \
            tables should be dropped manually or using migration scripts", object),
        DatabaseObjectType::View => {
            let sql = format!("drop view {}", object.id);
            pg_client.batch_execute(&sql)?;
        },
        DatabaseObjectType::Function => {
            let sql = format!("drop function {}", object.id);
            pg_client.batch_execute(&sql)?;
        },
        DatabaseObjectType::Constraint => {
            // 1. select table name constraint belongs to.
            // 2. validate if table is one of constraints dependencies.
            // 3. build drop sql.
            // 4. execute.

            let select_table_sql = "
                select 
                    lower(n.nspname) || '.' || lower(t.relname) as table_object_id,
                    lower(c.conname) as constraint_name
                from pg_constraint c
                join pg_class t on t.oid = c.conrelid
                join pg_namespace n on n.oid = t.relnamespace
                where lower(n.nspname) || '.' || lower(c.conname) = lower($1)
            ";

            let row = pg_client.query_one(select_table_sql, &[&object.id])
                .context(format!("single table was expected for a given constrain {:?}; check sql: {}", object.id, select_table_sql))?;

            let table_object_id: String = row.try_get(0)
                .context(format!("could not parse table name for constraint {:?}", object.id))?;

            let constraint_name: String = row.try_get(1)
            .context(format!("could not parse constraint name {:?}", object.id))?;
            

            if !object.depends_on.contains(&table_object_id) {
                bail!("inconsistent constraint dependencies, it should always depend on associated table {:?} {:?}", object.id, table_object_id);
            }
            
            let drop_constraint_sql = format!("alter table {table_id} drop constraint {constraint_name};",
                table_id=table_object_id,
                constraint_name=constraint_name
            );

            pg_client.batch_execute(&drop_constraint_sql)?;
        },
    };
    
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

fn update_object_with_deps(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject,
    objects: &HashMap<String, DatabaseObject>
) -> anyhow::Result<()> {
    if object.object_type == DatabaseObjectType::Table {
        update_table(pg_client, &object)?;
        return Ok(());
    }

    let exists = exists_object(pg_client, object)?;
    if !exists {
        println!("does not exist, create {:?} {:?}", object.object_type, object.id);
        pg_client.batch_execute(&object.script)?;
        update_pgfine_object(pg_client, object)?;
        return Ok(());
    }

    let hash_matching = check_hash(pg_client, &object)?;
    if hash_matching {
        println!("object is up to date {:?} {:?}", object.object_type, object.id);
        return Ok(());
    }

    println!("hash mismatch, update {:?} {:?}", object.object_type, object.id);

    // constraints must be dropped and created again
    if object.object_type != DatabaseObjectType::Constraint {    
        let update_result = pg_client.batch_execute(&object.script);
        if update_result.is_ok() {
            update_pgfine_object(pg_client, object)?;
            return Ok(());
        }
        println!("update failed {:?} {:?}", object.object_type, object.id);
    }

    let drop_result = drop_object(pg_client, &object);
    if drop_result.is_ok() {
        println!("create {:?} {:?}", object.object_type, object.id);
        pg_client.batch_execute(&object.script)?;
        update_pgfine_object(pg_client, object)?;
        return Ok(());
    }

    println!("drop failed, attempting to drop dependencies {:?} {:?}", object.object_type, object.id);
    let mut dropped: HashSet<String> = HashSet::new();
    let mut visited: HashSet<String> = HashSet::new();
    drop_object_with_deps(pg_client, &object, &objects, &mut dropped, &mut visited)
        .context(format!("drop dependencies failed {:?}", object))?;
    
    println!("create {:?} {:?}", object.object_type, object.id);
    pg_client.batch_execute(&object.script)?;
    update_pgfine_object(pg_client, object)?;
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

fn update_objects(
    pg_client: &mut postgres::Client,
    database_project: &DatabaseProject
) -> anyhow::Result<()> {
    let execute_order = database_project.get_execute_order()
        .context("update_objects error: could not get execute order")?;
    for object_id in execute_order.iter() {
        let object = database_project.objects.get(object_id).unwrap();
        update_object_with_deps(pg_client, &object, &database_project.objects)
            .with_context(|| format!("update_objects error: {:?}", object))?;
    }
    drop_missing_objects(pg_client, &database_project)?;
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

fn drop_missing_objects(
    pg_client: &mut postgres::Client,
    database_project: &DatabaseProject
) -> anyhow::Result<()> {

    let sql_select_objects = "
        select 
            po_id,
            po_type,
            po_md5,
            po_script,
            po_path,
            po_depends_on,
            po_required_by
        from pgfine_objects";
    
    let rows = pg_client.query(sql_select_objects, &[])
        .context("drop_missing_objects failed to select pgfine_objects")?;
    
    
    let mut missing_objects: HashMap<String, DatabaseObject> = HashMap::new();
    for row in rows {
        let db_object = DatabaseObject::from_db_row(&row)
            .context("could not parse pgfine_objects row")?;
        
        if database_project.objects.contains_key(&db_object.id) {
            continue;
        }

        let exists = exists_object(pg_client, &db_object)?;
        if !exists {
            println!("delete_pgfine_object {:?}", db_object.id);
            delete_pgfine_object(pg_client, &db_object.id)?;
            continue;
        }

        if db_object.object_type == DatabaseObjectType::Table {
            bail!("pgfine_objects record exists but it is missing in project, tables should be dropped manually or using migration scripts {:?}", db_object.id);
        }
        
        missing_objects.insert(db_object.id.clone(), db_object);
    }

    let mut missing_objects_sorted: Vec<String> = Vec::from_iter(missing_objects.keys().cloned());
    missing_objects_sorted.sort();

    let mut dropped: HashSet<String> = HashSet::new();
    for missing_object_id in missing_objects_sorted {
        let missing_object = missing_objects.get(&missing_object_id).unwrap();
        let mut visited: HashSet<String> = HashSet::new();
        drop_object_with_deps(pg_client, &missing_object, &missing_objects, &mut dropped, &mut visited)?;
        println!("delete_pgfine_object {:?}", missing_object.id);
        delete_pgfine_object(pg_client, &missing_object.id)?;
    }
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
    let mut pg_client = get_admin_pg_client()
        .context("drop error: failed to get connection string")?;

    for (path_buf, script) in database_project.drop_scripts {
        let prepared_script = prepare_admin_script(&script)
            .context(format!("drop error: failed to prepare drop script {:?}", path_buf))?;
        pg_client.batch_execute(&prepared_script)
            .context(format!("drop error: failed to execute drop script: {:?}", path_buf))?;
    }
    return Ok(());
}

