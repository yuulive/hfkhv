

use anyhow;
use anyhow::Context;
use postgres;
use crate::project::DatabaseProject;
use crate::project::DatabaseObject;
use crate::project::DatabaseObjectType;
use crate::utils;



fn get_super_pg_client() -> anyhow::Result<postgres::Client> {
    let super_connection_string = utils::read_env_var("PGFINE_SUPER_CONNECTION_STRING")
        .context("get_super_pg_client error: failed to get connection string")?;
    
    // FIXME match tlsMode
    let pg_client = postgres::Client::connect(&super_connection_string, postgres::NoTls)
        .context("get_super_pg_client error: failed to connect to db using PGFINE_SUPER_CONNECTION_STRING")?;

    return Ok(pg_client);
}

fn update_table(object: &DatabaseObject) -> anyhow::Result<()> {

    // table exists
    // skip


    // table does not exist
    // create table


    

    return Ok(());
}

fn update_view(object: &DatabaseObject) -> anyhow::Result<()> {
    
    // view exists hash math
    // skip

    // view does not exist
    // create view
    // write hash
    
    // view exists hash mismatch
    // attempt alter view
    // else attempt drop-create view
    // else attempt drop required_by drop-create view
    // write new hash

    return Ok(());
}

fn update_function(object: &DatabaseObject) -> anyhow::Result<()> {

    // function exists hash math
    // skip

    // function does not exist
    // create function
    // write hash
    
    // function exists hash mismatch
    // attempt alter function
    // else attempt drop-create function
    // else attempt drop required_by drop-create function
    // write new hash

    return Ok(());
}

fn update_object(object: &DatabaseObject) -> anyhow::Result<()> {
    match object.object_type {
        DatabaseObjectType::Table => update_table(&object)?,
        DatabaseObjectType::View => update_view(&object)?,
        DatabaseObjectType::Function => update_function(&object)?,
    }

    return Ok(());
}

pub fn create(database_project: DatabaseProject) -> anyhow::Result<()> {
    let mut pg_client = get_super_pg_client()
        .context("create error: failed to get connection string")?;

    for (path_buf, script) in database_project.create_scripts {
        pg_client.batch_execute(&script)
            .with_context(|| format!("create error: failed to execute script: {:?}", path_buf))?;
    }

    for object_id in database_project.execute_order {
        let object = database_project.objects.get(&object_id).unwrap();
        update_object(&object)
            .with_context(|| format!("create error: failed to update object: {:?}", object_id))?;
    }


    return Ok(());
}


pub fn drop(database_project: DatabaseProject) -> anyhow::Result<()> {
    let mut pg_client = get_super_pg_client()
        .context("drop error: failed to get connection string")?;

    for (path_buf, script) in database_project.drop_scripts {
        pg_client.batch_execute(&script)
            .with_context(|| format!("drop error: failed to execute script: {:?}", path_buf))?;
    }
    return Ok(());
}

