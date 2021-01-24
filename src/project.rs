

use std::fs;
use std::io::prelude::*;
use std::path;
use anyhow;


fn get_create_db_script() -> String {
    let database_name = "database_name"; // FIXME: take from env variable
    let role_name = "role_name"; // FIXME: take from env variable


    return format!("\n\
        CREATE ROLE {role_name};\n\
        CREATE DATABASE {database_name}\n\
        WITH\n\
        OWNER = {role_name}\n\
        TEMPLATE = template0\n\
        ENCODING = 'UTF8'\n\
        LC_COLLATE = 'en_US.UTF-8'\n\
        LC_CTYPE = 'en_US.UTF-8'\n\
        TABLESPACE = pg_default\n\
        CONNECTION LIMIT = 10;\n\
    ", role_name=role_name, database_name=database_name);
}


pub fn init(path_str: &str) -> anyhow::Result<()> {
    println!("project::init({:?})", path_str);

    let path_obj = path::Path::new(path_str);
    if path_obj.exists() {
        bail!("already exists: {:?}", path_str);
    }

    fs::create_dir_all(path_obj)?;
    fs::create_dir(path_obj.join("tables"))?;
    fs::create_dir(path_obj.join("views"))?;
    fs::create_dir(path_obj.join("functions"))?;
    fs::create_dir(path_obj.join("roles"))?;
    fs::create_dir(path_obj.join("migrations"))?;

    {
        // create.sql
        let path_buf = path_obj.join("create.sql");
        let content = get_create_db_script();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path_buf)?;

        file.write_all(content.as_bytes())?;
    }
    
    return Ok(());
}
