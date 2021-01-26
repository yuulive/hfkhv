

use std::collections::HashSet;
use std::collections::HashMap;
use std::fs;
use std::path;
use path::PathBuf;
use anyhow;
use anyhow::Context;
use postgres;
use crate::utils;

#[cfg(test)]
mod tests;


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

fn object_id_from_path(path_buf: &PathBuf) -> anyhow::Result<String> {
    let filestem = path_buf.file_stem()
        .ok_or(anyhow!("object_id_from_path error: could not parse filename {:?}", path_buf))?;
    let filestem_str = filestem.to_str()
        .ok_or(anyhow!("object_id_from_path error: could not parse filename {:?}", path_buf))?;
    return Ok(filestem_str.into());
}


fn load_objects_info_by_type(
    result: &mut HashMap<String, (DatabaseObjectType, PathBuf, String)>, 
    path_buf: &PathBuf,
    object_type: DatabaseObjectType
) -> anyhow::Result<()> {
    let ls_paths = utils::list_files(&path_buf)
        .context(format!("load_objects_info error: failed to list files at {:?}", path_buf))?;
    for ls_path in ls_paths {
        let object_id = object_id_from_path(&ls_path)
            .context(format!("load_objects_info error: failed to parse object_id {:?}", ls_path))?;
        let script = utils::read_file(&ls_path)
            .context(format!("load_objects_info error: failed to read file {:?}", ls_path))?;
        result.insert(object_id, (object_type, ls_path, script));
    }
    return Ok(());
}


fn load_objects_info(path_str: &str) -> anyhow::Result<HashMap<String, (DatabaseObjectType, PathBuf, String)>> {
    let mut result = HashMap::new();

    let path_buf = path::PathBuf::from(path_str).join("tables");
    load_objects_info_by_type(&mut result, &path_buf, DatabaseObjectType::Table)?;

    let path_buf = path::PathBuf::from(path_str).join("views");
    load_objects_info_by_type(&mut result, &path_buf, DatabaseObjectType::View)?;

    let path_buf = path::PathBuf::from(path_str).join("functions");
    load_objects_info_by_type(&mut result, &path_buf, DatabaseObjectType::Function)?;

    

    // FIXME add constraints

    return Ok(result);
}

fn calc_required_by_for_object(
    object_id: &str, 
    objects_info: &HashMap<String, (DatabaseObjectType, PathBuf, String)>
) -> HashSet<String> {
    let mut result = HashSet::new();
    for (required_by_object_id, (_, _, script)) in objects_info {
        if object_id == required_by_object_id {
            continue;
        }
        if script.contains(object_id) {
            result.insert(required_by_object_id.clone());
        }
    }
    return result;
}

fn calc_required_by(
    objects_info: &HashMap<String, (DatabaseObjectType, PathBuf, String)>
) -> HashMap<String, HashSet<String>> {
    let mut result = HashMap::new();
    for (object_id, _) in objects_info {
        let required_by = calc_required_by_for_object(object_id, objects_info);
        result.insert(object_id.clone(), required_by);
    }
    return result;
}

fn calc_depends_on_for_object(object_id: &str, required_by: &HashMap<String, HashSet<String>>) -> HashSet<String> {
    let mut result = HashSet::new();
    for (depends_on_object_id, required_by) in required_by {
        if object_id == depends_on_object_id {
            continue;
        }
        if required_by.contains(object_id) {
            result.insert(depends_on_object_id.clone());
        }
    }
    return result;
}

fn calc_depends_on(required_by: &HashMap<String, HashSet<String>>) -> HashMap<String, HashSet<String>>{
    let mut result = HashMap::new();
    for (object_id, _object_required_by) in required_by {
        let depends_on = calc_depends_on_for_object(object_id, required_by);
        result.insert(object_id.clone(), depends_on);
    }
    return result;
}

fn build_database_objects(
    mut objects_info: HashMap<String, (DatabaseObjectType, PathBuf, String)>,
    mut required_by: HashMap<String, HashSet<String>>,
    mut depends_on: HashMap<String, HashSet<String>>
) -> HashMap<String, DatabaseObject> {
    let mut result = HashMap::new();
    for (object_id, (object_type, path_buf, script)) in objects_info.drain() {
        let object_depends_on = depends_on.remove(&object_id).expect("depends_on.remove(&object_id)");
        let object_required_by = required_by.remove(&object_id).expect("required_by.remove(&object_id)");
        let o = DatabaseObject {
            object_type: object_type,
            id: object_id.clone(),
            path_buf: path_buf,
            script: script,
            md5: "asd".into(),
            depends_on: object_depends_on,
            required_by: object_required_by,
        };
        result.insert(object_id, o);
    }
    return result;
}

fn resolve_dependencies(
    object_id: &String, 
    objects: &HashMap<String, DatabaseObject>,
    dependencies_vec: &mut Vec<String>,
    dependencies_set: &mut HashSet<String>,
    visited: &mut HashSet<String>,
) -> anyhow::Result<()> {
    if dependencies_set.contains(object_id) {
        return Ok(());
    }

    if visited.contains(object_id) {
        bail!("resolve_dependencies error: cicle detected {:?}", object_id);
    }
    visited.insert(object_id.clone());

    let new_dependencies;
    {
        let object = objects.get(object_id).unwrap();
        new_dependencies = object.depends_on.clone();
    }
    
    for dep in new_dependencies {
        resolve_dependencies(
            &dep,
            &objects,
            dependencies_vec,
            dependencies_set,
            visited
        )?;
    }

    dependencies_set.insert(object_id.clone());
    dependencies_vec.push(object_id.clone());
    return Ok(());
}

fn calc_execute_order(objects: &HashMap<String, DatabaseObject>) -> anyhow::Result<Vec<String>> {
    let mut dependencies_vec: Vec<String> = vec![];
    let mut dependencies_set: HashSet<String> = HashSet::new();

    for object_id in objects.keys() {
        let mut visited: HashSet<String> = HashSet::new();
        resolve_dependencies(
            object_id, 
            objects, 
            &mut dependencies_vec, 
            &mut dependencies_set,
            &mut visited
        )?;
    }

    return Ok(dependencies_vec);
}

#[derive(Debug, Copy, Clone)]
pub enum DatabaseObjectType {
    Table,
    View,
    Function,
}

#[derive(Debug, Clone)]
pub struct DatabaseObject {
    pub object_type: DatabaseObjectType,
    pub id: String,
    pub path_buf: PathBuf,
    pub script: String,
    pub md5: String,
    pub depends_on: HashSet<String>,
    pub required_by: HashSet<String>,
}

pub struct DatabaseProject {
    //pub version: String,
    pub project_dirpath: PathBuf,
    pub create_scripts: Vec<(PathBuf, String)>,
    pub drop_scripts: Vec<(PathBuf, String)>,
    pub objects: HashMap<String, DatabaseObject>,
    pub execute_order: Vec<String>,
}

impl DatabaseProject {
    fn from_path(path_str: &str) -> anyhow::Result<DatabaseProject> {

        let path_buf = path::Path::new(path_str).join("create");
        let create_script_paths = utils::list_files(&path_buf)?;
        let mut create_scripts = vec![];
        for p in create_script_paths {
            let script = utils::read_file(&p)?;
            create_scripts.push((p, script));
        }

        let path_buf = path::Path::new(path_str).join("drop");
        let drop_script_paths = utils::list_files(&path_buf)?;
        let mut drop_scripts = vec![];
        for p in drop_script_paths {
            let script = utils::read_file(&p)?;
            drop_scripts.push((p, script));
        }


        let objects_info = load_objects_info(&path_str)?;
        let required_by = calc_required_by(&objects_info);
        let depends_on = calc_depends_on(&required_by);
        let objects = build_database_objects(objects_info, required_by, depends_on);
        let execute_order= calc_execute_order(&objects)?;

        return Ok(DatabaseProject {
            project_dirpath: path_buf,
            create_scripts,
            drop_scripts,
            objects,
            execute_order,
        });

    }
}

pub fn load() -> anyhow::Result<DatabaseProject> {
    let database_project = DatabaseProject::from_path("./pgfine")?;
    return Ok(database_project);
}

