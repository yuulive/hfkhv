

use std::env;
use anyhow;
use anyhow::Context;
use postgres;
use crate::project::DatabaseProject;


fn read_env_var(key: &str) -> anyhow::Result<String> {
    let result = env::var(key).with_context(|| format!("failed to read env variable: {:?}", key))?;
    return Ok(result);
}

fn get_super_pg_client() -> anyhow::Result<postgres::Client> {
    let super_connection_string = read_env_var("PGFINE_SUPER_CONNECTION_STRING")?;
    
    // FIXME match tlsMode
    let pg_client = postgres::Client::connect(&super_connection_string, postgres::NoTls)?;
    return Ok(pg_client);
}

pub fn create(database_project: DatabaseProject) -> anyhow::Result<()> {
    let mut pg_client = get_super_pg_client()?;
    for script in database_project.create_scripts {
        pg_client.batch_execute(&script)?;
    }
    return Ok(());
}


pub fn drop(database_project: DatabaseProject) -> anyhow::Result<()> {
    let mut pg_client = get_super_pg_client()?;
    for script in database_project.drop_scripts {
        pg_client.batch_execute(&script)?;
    }
    return Ok(());
}

