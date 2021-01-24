

use std::fs;
use std::io::prelude::*;
use std::path;
use anyhow;
use anyhow::Context;
use path::PathBuf;


fn read_file(path_buf: &PathBuf) -> anyhow::Result<String> {
    let result = fs::read_to_string(path_buf).with_context(|| format!("failed to read file: {:?}", path_buf))?;
    return Ok(result);
}

fn write_file(path_buf: &PathBuf, content: &str) -> anyhow::Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path_buf)
        .with_context(|| format!("failed to open file for writing: {:?}", path_buf))?;
        
    file.write_all(content.as_bytes()).with_context(|| format!("failed to write into file: {:?}", path_buf))?;
    return Ok(());
}

fn list_files(path_buf: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let dir = fs::read_dir(path_buf).with_context(|| format!("failed to read dir: {:?}", path_buf))?;
    let mut result = vec![];
    for dir_entry_result in dir {
        let dir_entry = dir_entry_result.with_context(|| format!("failed to read dir entry: {:?}", path_buf))?;
        result.push(dir_entry.path());
    }
    result.sort();
    return Ok(result);
}

fn get_create_script_00() -> (String, String) {
    let role_name = "role_name"; // FIXME: take from env variable
    let filename = String::from("00-create-role.sql");
    let content = format!("\n\
        CREATE ROLE {role_name};\n\
        ", role_name=role_name
    );
    return (filename, content);
}

fn get_create_script_01() -> (String, String) {
    let database_name = "database_name"; // FIXME: take from env variable
    let role_name = "role_name"; // FIXME: take from env variable
    let filename = String::from("01-create-database.sql");
    let content = format!("\n\
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
    return (filename, content);
}

fn get_drop_script_00() -> (String, String) {
    let database_name = "database_name"; // FIXME: take from env variable
    let filename = String::from("00-drop-database.sql");
    let content = format!("\n\
        DROP DATABASE IF EXISTS \"{database_name}\";\n\
    ", database_name=database_name);
    return (filename, content);
}

fn get_drop_script_01() -> (String, String) {
    let role_name = "role_name"; // FIXME: take from env variable
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
        write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_create_script_01();
        let path_buf = path_obj.join("create").join(filename);
        write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_drop_script_00();
        let path_buf = path_obj.join("drop").join(filename);
        write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_drop_script_01();
        let path_buf = path_obj.join("drop").join(filename);
        write_file(&path_buf, &content)?;
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
        let create_script_paths = list_files(&path_buf)?;
        let mut create_scripts = vec![];
        for p in create_script_paths {
            let script = read_file(&p)?;
            create_scripts.push(script);
        }

        let path_buf = path::Path::new(path_str).join("drop");
        let drop_script_paths = list_files(&path_buf)?;
        let mut drop_scripts = vec![];
        for p in drop_script_paths {
            let script = read_file(&p)?;
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
