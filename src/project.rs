

use std::fs;
use std::path;
use path::PathBuf;
use anyhow;
use postgres;
use crate::utils;



fn get_default_role_name() -> String {
    let default_role_name = String::from("pgfine_role");
    let connection_string_result = utils::read_env_var("PGFINE_CONNECTION_STRING");
    match connection_string_result {
        Ok(connection_string) => {
            let pg_config_result = connection_string.parse::<postgres::Config>();
            match pg_config_result {
                Ok(pg_config) => {
                    let user_result = pg_config.get_user();
                    match user_result {
                        Some(user_str) => {
                            return user_str.into();
                        },
                        None => {
                            return default_role_name;
                        }
                    }
                },
                Err(_) => {
                    return default_role_name;
                }
            };
        },
        Err(_) => {
            return default_role_name;
        }
    };
}

fn get_default_database_name() -> String {
    let default_database_name = String::from("pgfine_database");
    let connection_string_result = utils::read_env_var("PGFINE_CONNECTION_STRING");
    match connection_string_result {
        Ok(connection_string) => {
            let pg_config_result = connection_string.parse::<postgres::Config>();
            match pg_config_result {
                Ok(pg_config) => {
                    let dbname_result = pg_config.get_dbname();
                    match dbname_result {
                        Some(dbname) => {
                            return dbname.into();
                        },
                        None => {
                            return default_database_name;
                        }
                    }
                },
                Err(_) => {
                    return default_database_name;
                }
            };
        },
        Err(_) => {
            return default_database_name;
        }
    };
}

fn get_create_script_00() -> (String, String) {
    let role_name = get_default_role_name();
    let filename = String::from("00-create-role.sql");
    let content = format!("\n\
        CREATE ROLE \"{role_name}\";\n\
        ", role_name=role_name
    );
    return (filename, content);
}

fn get_create_script_01() -> (String, String) {
    let database_name = get_default_database_name();
    let role_name = get_default_role_name();
    let filename = String::from("01-create-database.sql");
    let content = format!("\n\
        CREATE DATABASE \"{database_name}\"\n\
        WITH\n\
        OWNER = {role_name}\n\
        TEMPLATE = template0\n\
        ENCODING = 'UTF8'\n\
        LC_COLLATE = 'en_US.UTF-8'\n\
        LC_CTYPE = 'en_US.UTF-8'\n\
        TABLESPACE = pg_default\n\
        CONNECTION LIMIT = 10;\n\
    ", role_name=role_name, database_name=database_name);
    return (filename, content);
}

fn get_drop_script_00() -> (String, String) {
    let database_name = get_default_database_name();
    let filename = String::from("00-drop-database.sql");
    let content = format!("\n\
        DROP DATABASE IF EXISTS \"{database_name}\";\n\
    ", database_name=database_name);
    return (filename, content);
}

fn get_drop_script_01() -> (String, String) {
    let role_name = get_default_role_name();
    let filename = String::from("01-drop-role.sql");
    let content = format!("\n\
        DROP ROLE IF EXISTS \"{role_name}\";\n\
    ", role_name=role_name);
    return (filename, content);
}

pub fn init(path_str: &str) -> anyhow::Result<()> {
    println!("project::init({:?})", path_str);

    let path_obj = path::Path::new(path_str);
    if path_obj.exists() {
        bail!("already exists: {:?}", path_str);
    }

    fs::create_dir_all(path_obj)?;
    fs::create_dir(path_obj.join("create"))?;
    fs::create_dir(path_obj.join("drop"))?;
    fs::create_dir(path_obj.join("tables"))?;
    fs::create_dir(path_obj.join("views"))?;
    fs::create_dir(path_obj.join("functions"))?;
    fs::create_dir(path_obj.join("roles"))?;
    fs::create_dir(path_obj.join("migrations"))?;

    {
        let (filename, content) = get_create_script_00();
        let path_buf = path_obj.join("create").join(filename);
        utils::write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_create_script_01();
        let path_buf = path_obj.join("create").join(filename);
        utils::write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_drop_script_00();
        let path_buf = path_obj.join("drop").join(filename);
        utils::write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_drop_script_01();
        let path_buf = path_obj.join("drop").join(filename);
        utils::write_file(&path_buf, &content)?;
    }

    
    return Ok(());
}



pub struct DatabaseProject {
    pub project_dirpath: PathBuf,
    pub create_scripts: Vec<String>,
    pub drop_scripts: Vec<String>,
}

impl DatabaseProject {
    fn from_path(path_str: &str) -> anyhow::Result<DatabaseProject> {

        let path_buf = path::Path::new(path_str).join("create");
        let create_script_paths = utils::list_files(&path_buf)?;
        let mut create_scripts = vec![];
        for p in create_script_paths {
            let script = utils::read_file(&p)?;
            create_scripts.push(script);
        }

        let path_buf = path::Path::new(path_str).join("drop");
        let drop_script_paths = utils::list_files(&path_buf)?;
        let mut drop_scripts = vec![];
        for p in drop_script_paths {
            let script = utils::read_file(&p)?;
            drop_scripts.push(script);
        }

        return Ok(DatabaseProject {
            project_dirpath: path_buf,
            create_scripts,
            drop_scripts,
        });

    }
}

pub fn load() -> anyhow::Result<DatabaseProject> {
    let database_project = DatabaseProject::from_path("./pgfine")?;
    return Ok(database_project);
}
