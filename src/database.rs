
use anyhow;
use postgres;
use crate::project::DatabaseProject;




pub fn create(database_project: DatabaseProject) -> anyhow::Result<()> {

    let super_connection_string = "";

    // FIXME match tlsMode
    let pg_client = postgres::Client::connect(super_connection_string, postgres::NoTls)?;

    



    return Ok(());
}
