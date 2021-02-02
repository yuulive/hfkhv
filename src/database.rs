
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
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
        insert into pgfine_objects (object_id, object_type, object_md5)
        select $1, $2, $3
        on conflict (object_id) do update set 
            object_type = excluded.object_type,
            object_md5 = excluded.object_md5;";

    let object_type_str: String = object.object_type.into();
    // do not write md5 for tables maybe?
    pg_client.execute(sql, &[&object.id, &object_type_str, &object.md5])?;
    return Ok(());
}

fn delete_pgfine_object(
    pg_client: &mut postgres::Client,
    object_id: &str
) -> anyhow::Result<()> {
    let sql = "delete from pgfine_objects where lower(object_id) = lower($1)";
    pg_client.execute(sql, &[&object_id])
        .context(format!("delete_pgfine_object failed {:?}", object_id))?;
    return Ok(());
}

fn exists_object(
    pg_client: &mut postgres::Client, 
    object: &DatabaseObject
) -> anyhow::Result<bool> {
    return exists_object_by_type_id(pg_client, &object.object_type, &object.id);
}

fn exists_object_by_type_id(
    pg_client: &mut postgres::Client, 
    object_type: &DatabaseObjectType,
    object_id: &str,
) -> anyhow::Result<bool> {
    let sql = match object_type {
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
    let row = pg_client.query_one(sql, &[&object_id])
        .context(format!("exists_object error quering {:?} {:?}", object_type, object_id))?;
    let exists: bool = row.try_get(0)
        .context(format!("exists_object error parsing {:?} {:?}", object_type, object_id))?;
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
        select max(object_md5) as object_md5 
        from pgfine_objects 
        where lower(object_id) = lower($1)
    ";
    let row = pg_client.query_one(sql, &[&object.id])?;
    let md5_old_opt: Option<String> = row.try_get(0)?;
    if let Some(md5_old) = md5_old_opt {
        return Ok(md5_old == object.md5);
    }
    return Ok(false);
}

// FIXME remove this function
fn drop_object_by_type_id(
    pg_client: &mut postgres::Client,
    object_type: &DatabaseObjectType,
    object_id: &str,
) -> anyhow::Result<()> {
    let sql = match object_type {
        DatabaseObjectType::Table => panic!("attempting to drop table {:?}", object_id),
        DatabaseObjectType::View => format!("drop view {}", object_id),
        DatabaseObjectType::Function => format!("drop function {}", object_id),
        DatabaseObjectType::Constraint => panic!("not implemented {:?}", object_id),
    };
    println!("drop {:?} {:?}", object_type, object_id);
    pg_client.batch_execute(&sql)?;
    return Ok(());
}

fn drop_object(
    pg_client: &mut postgres::Client,
    object: &DatabaseObject
) -> anyhow::Result<()> {
    println!("drop {:?} {:?}", object.object_type, object.id);
    match object.object_type {
        DatabaseObjectType::Table => panic!("attempting to drop a table {:?}", object),
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
    dropped: &mut HashSet<String>
) -> anyhow::Result<()> {
    if dropped.contains(&object.id) {
        return Ok(());
    }

    for dep_id in object.required_by.iter() {
        let dep = objects.get(dep_id).unwrap();
        drop_object_with_deps(pg_client, &dep, &objects, dropped)?;
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
    drop_object_with_deps(pg_client, &object, &objects, &mut dropped)
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
            object_id text primary key,
            object_type text not null,
            object_md5 text null
        );";

    pg_client.batch_execute(pgfine_objects_sql)?;

    let pgfine_version_sql = "
        create table if not exists pgfine_migrations (
            migration_id text primary key
        );";
    
    pg_client.batch_execute(pgfine_version_sql)?;

    return Ok(());
}

fn update_objects(
    pg_client: &mut postgres::Client,
    database_project: &DatabaseProject
) -> anyhow::Result<()> {
    for object_id in database_project.execute_order.iter() {
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
        insert into pgfine_migrations (migration_id)
        select $1
        on conflict (migration_id) do nothing;";
    pg_client.execute(sql, &[&migration])?;
    return Ok(());
}

fn drop_missing_objects(
    pg_client: &mut postgres::Client,
    database_project: &DatabaseProject
) -> anyhow::Result<()> {

    // FIXME store 'required_by' to satisfy dependencies tree
    let sql_select_objects = "select object_id, object_type from pgfine_objects";
    let rows = pg_client.query(sql_select_objects, &[])
        .context("drop_missing_objects failed to select pgfine_objects")?;
    
    for row in rows {
        let db_object_id: String = row.try_get(0)
            .context("parse error for object_id")?;
        let db_object_type_str: String = row.try_get(1)
            .context("parse error for object_type")?;
        let db_object_type = DatabaseObjectType::try_from(db_object_type_str.as_ref())
            .context(format!("convert object_type error {:?}", db_object_id))?;

        if !database_project.objects.contains_key(&db_object_id) {
            let exists = exists_object_by_type_id(pg_client, &db_object_type, &db_object_id)?;
            if exists {
                if db_object_type == DatabaseObjectType::Table {
                    bail!("pgfine_objects record exists but it is missing in project, tables should be dropped manually or using migration scripts {:?}", db_object_id);
                }
                println!("drop_missing_objects {:?} {:?}", db_object_type, db_object_id);
                drop_object_by_type_id(pg_client, &db_object_type, &db_object_id)
                    .context(format!("pgfine_objects record exists but it is missing in project, attempt to drop the object failed"))?;
            }
            println!("delete_pgfine_object {:?} {:?}", db_object_type, db_object_id);
            delete_pgfine_object(pg_client, &db_object_id)?;
        }
    }

    return Ok(());
}


fn get_db_last_migration(pg_client: &mut postgres::Client) -> anyhow::Result<Option<String>> {
    let sql = "select max(migration_id) from pgfine_migrations;";
    let row = pg_client.query_one(sql, &[])?;
    let result = row.try_get(0)?;
    return Ok(result);
}


pub fn migrate(database_project: DatabaseProject) -> anyhow::Result<()> {

    let project_last_migration_opt = database_project.migration_scripts.last();
    let pg_client_result = get_pg_client();
    
    match pg_client_result {
        Err(_) => {
            let mut admin_pg_client = get_admin_pg_client()
                .context("could not connect to database neither using PGFINE_CONNECTION_STRING nor PGFINE_ADMIN_CONNECTION_STRING")?;

            if exists_database(&mut admin_pg_client)? {
                bail!("database exists but could not get connection to it, check PGFINE_CONNECTION_STRING");
            }

            create_database(&mut admin_pg_client, &database_project)?;

            let mut pg_client = get_pg_client()?;
            create_pgfine_tables(&mut pg_client)?;
            update_objects(&mut pg_client, &database_project)?;

            if let Some((project_last_migration, _)) = project_last_migration_opt {
                insert_pgfine_migration(&mut pg_client, &project_last_migration)?;
            } else {
                insert_pgfine_migration(&mut pg_client, "")?;
            }
        },
        Ok(mut pg_client) => {
            create_pgfine_tables(&mut pg_client)?;
            let db_last_migration_opt = get_db_last_migration(&mut pg_client)?;
            match db_last_migration_opt {
                Some(db_last_migration) => {
                    let mut db_last_migration_current = db_last_migration;
                    loop {
                        if let Some((next_migration_id, next_migration_script)) 
                            = database_project.get_next_migration(&db_last_migration_current
                        ) {
                            pg_client.batch_execute(&next_migration_script)?;
                            insert_pgfine_migration(&mut pg_client, &next_migration_id)?; // should go in single tansaction
                            db_last_migration_current = get_db_last_migration(&mut pg_client)?.unwrap();
                        } else {
                            break;
                        }
                    }
                    update_objects(&mut pg_client, &database_project)?;
                },
                None => {
                    // assumes manually created empty database
                    update_objects(&mut pg_client, &database_project)?;

                    if let Some((project_last_migration, _)) = project_last_migration_opt {
                        insert_pgfine_migration(&mut pg_client, &project_last_migration)?;
                    } else {
                        insert_pgfine_migration(&mut pg_client, "")?;
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
        let prepared_script = prepare_admin_script(&script)?;
        pg_client.batch_execute(&prepared_script)
            .with_context(|| format!("drop error: failed to execute script: {:?}", path_buf))?;
    }
    return Ok(());
}

