

use std::collections::HashSet;
use std::collections::HashMap;
use std::fs;
use std::path;
use std::convert::TryFrom;
use path::PathBuf;
use anyhow;
use anyhow::Context;
use md5::Md5;
use md5::Digest;
use hex;
use crate::utils;


#[cfg(test)]
mod tests;

fn get_project_path() -> anyhow::Result<PathBuf> {
    let project_path_str = utils::read_env_var("PGFINE_DIR")
        .context("get_project_path error: failed to read env variable PGFINE_DIR")?;
    return Ok(PathBuf::from(project_path_str));
}

fn get_create_script_00() -> (String, String) {
    let filename = String::from("00-create-role.sql");
    let content = "
-- available parameters for substitution {param}:
-- -- database_name
-- -- role_name
-- -- password
-- parameters are taken from PGFINE_CONNECTION_STRING env variable
-- parameters are validated to contain only alphanum cahracters and underscore
CREATE ROLE \"{role_name}\" WITH LOGIN PASSWORD '{password}'
".into();
    return (filename, content);
}

fn get_create_script_01() -> (String, String) {
    let filename = String::from("01-create-database.sql");
    let content = "
-- available parameters for substitution {param}:
-- -- database_name
-- -- role_name
-- -- password
-- parameters are taken from PGFINE_CONNECTION_STRING env variable
-- parameters are validated to contain only alphanum cahracters and underscore
CREATE DATABASE \"{database_name}\"
WITH
OWNER = \"{role_name}\"
TEMPLATE = template0
ENCODING = 'UTF8'
LC_COLLATE = 'en_US.UTF-8'
LC_CTYPE = 'en_US.UTF-8'
TABLESPACE = pg_default
CONNECTION LIMIT = 10;
".into();
    return (filename, content);
}

fn get_drop_script_00() -> (String, String) {
    let filename = String::from("00-drop-database.sql");
    let content = "
-- available parameters for substitution {param}:
-- -- database_name
-- -- role_name
-- -- password
-- parameters are taken from PGFINE_CONNECTION_STRING env variable
-- parameters are validated to contain only alphanum cahracters and underscore
DROP DATABASE IF EXISTS \"{database_name}\" WITH (FORCE);
".into();
    return (filename, content);
}

fn get_drop_script_01() -> (String, String) {
    let filename = String::from("01-drop-role.sql");
    let content = "
-- available parameters for substitution {param}:
-- -- database_name
-- -- role_name
-- -- password
-- parameters are taken from PGFINE_CONNECTION_STRING env variable
-- parameters are validated to contain only alphanum cahracters and underscore
DROP ROLE IF EXISTS \"{role_name}\";
".into();
    return (filename, content);
}

pub fn init() -> anyhow::Result<()> {
    
    let project_path = get_project_path()?;
    
    if project_path.exists() {
        println!("project directory already exists at {:?}", project_path);
        return Ok(());
    }

    fs::create_dir_all(&project_path)?;
    fs::create_dir(project_path.join("create"))?;
    fs::create_dir(project_path.join("drop"))?;
    fs::create_dir(project_path.join("tables"))?;
    fs::create_dir(project_path.join("views"))?;
    fs::create_dir(project_path.join("functions"))?;
    fs::create_dir(project_path.join("roles"))?;
    fs::create_dir(project_path.join("migrations"))?;
    fs::create_dir(project_path.join("constraints"))?;

    {
        let (filename, content) = get_create_script_00();
        let path_buf = project_path.join("create").join(filename);
        utils::write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_create_script_01();
        let path_buf = project_path.join("create").join(filename);
        utils::write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_drop_script_00();
        let path_buf = project_path.join("drop").join(filename);
        utils::write_file(&path_buf, &content)?;
    }

    {
        let (filename, content) = get_drop_script_01();
        let path_buf = project_path.join("drop").join(filename);
        utils::write_file(&path_buf, &content)?;
    }

    
    return Ok(());
}

fn object_id_from_path(path_buf: &PathBuf) -> anyhow::Result<String> {
    
    let filestem = path_buf.file_stem()
        .ok_or(anyhow!("object_id_from_path error: could not parse filename {:?}", path_buf))?;
    let filestem_str = filestem.to_str()
        .ok_or(anyhow!("object_id_from_path error: could not parse filename {:?}", path_buf))?;

    

    let parts: Vec<&str> = filestem_str.split('.').collect();
    if parts.len() != 2 {
        bail!("Database object filename must contain schema and name separated by '.'. {:?}", path_buf)
    }

    return Ok(filestem_str.to_lowercase());
}

fn migration_id_from_path(path_buf: &PathBuf) -> anyhow::Result<String> {
    let filename = path_buf.file_name()
        .ok_or(anyhow!("migration_id_from_path error: could not parse filename {:?}", path_buf))?;
    let filename_str = filename.to_str()
        .ok_or(anyhow!("migration_id_from_path error: could not parse filename {:?}", path_buf))?;

    return Ok(filename_str.into());
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


fn load_objects_info(project_path: &PathBuf) -> anyhow::Result<HashMap<String, (DatabaseObjectType, PathBuf, String)>> {
    let mut result = HashMap::new();

    let path_buf = project_path.join("tables");
    load_objects_info_by_type(&mut result, &path_buf, DatabaseObjectType::Table)?;

    let path_buf = project_path.join("views");
    load_objects_info_by_type(&mut result, &path_buf, DatabaseObjectType::View)?;

    let path_buf = project_path.join("functions");
    load_objects_info_by_type(&mut result, &path_buf, DatabaseObjectType::Function)?;

    let path_buf = project_path.join("constraints");
    load_objects_info_by_type(&mut result, &path_buf, DatabaseObjectType::Constraint)?;

    return Ok(result);
}

fn calc_required_by_for_object(
    object_id: &str, 
    objects_info: &HashMap<String, (DatabaseObjectType, PathBuf, String)>,
    search_schemas: &HashSet<String>
) -> HashSet<String> {
    let mut result = HashSet::new();
    let id_parts: Vec<&str> = object_id.split('.').collect();
    let schema: &str = id_parts[0].into();
    let name: &str = id_parts[1].into();
    let search_term;
    if search_schemas.contains(schema) {
        search_term = name;
    } else {
        search_term = object_id;
    }

    for (required_by_object_id, (_, _, script)) in objects_info {
        if object_id == required_by_object_id {
            continue;
        }
        let contains = utils::contains_whole_word_ci(&script, &search_term);
        if contains {
            result.insert(required_by_object_id.clone());
        }
    }
    return result;
}

fn calc_required_by(
    objects_info: &HashMap<String, (DatabaseObjectType, PathBuf, String)>,
    search_schemas: &HashSet<String>
) -> HashMap<String, HashSet<String>> {
    let mut result = HashMap::new();
    for (object_id, _) in objects_info {
        let required_by = calc_required_by_for_object(object_id, objects_info, &search_schemas);
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
) -> anyhow::Result<HashMap<String, DatabaseObject>> {
    let mut result = HashMap::new();
    let mut hasher = Md5::new();
    for (object_id, (object_type, path_buf, script)) in objects_info.drain() {
        let object_depends_on = depends_on.remove(&object_id).expect("depends_on.remove(&object_id)");
        let object_required_by = required_by.remove(&object_id).expect("required_by.remove(&object_id)");
        let id = object_id.clone();
        let id_parts: Vec<&str> = id.split('.').collect();
        let schema: String = id_parts[0].into();
        let name: String = id_parts[1].into();
        hasher.update(&script);
        let hash = hasher.finalize_reset();
        let hash_str = hex::encode(hash);
        let o = DatabaseObject {
            object_type: object_type,
            id,
            schema,
            name,
            path_buf: path_buf,
            script: script,
            md5: hash_str,
            depends_on: object_depends_on,
            required_by: object_required_by,
        };
        result.insert(object_id, o);
    }
    return Ok(result);
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
        bail!("resolve_dependencies error: cycle detected {:?}", object_id);
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

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum DatabaseObjectType {
    Table,
    View,
    Function,
    Constraint,
}

impl From<DatabaseObjectType> for String {
    fn from(t: DatabaseObjectType) -> Self {
        match t {
            DatabaseObjectType::Table => "table".into(),
            DatabaseObjectType::View => "view".into(),
            DatabaseObjectType::Function => "function".into(),
            DatabaseObjectType::Constraint => "constraint".into(),
        }
    }
}

impl TryFrom<&str> for DatabaseObjectType {
    type Error = anyhow::Error;
    fn try_from(t: &str) -> Result<Self, Self::Error> {
        let object_type = match t {
            "table" => DatabaseObjectType::Table,
            "view" => DatabaseObjectType::View,
            "function" => DatabaseObjectType::Function,
            "constraint" => DatabaseObjectType::Constraint,
            _ => bail!("could not convert object type from {:?}", t),
        };
        return Ok(object_type);
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseObject {
    pub object_type: DatabaseObjectType,
    pub id: String,
    pub schema: String,
    pub name: String,
    pub path_buf: PathBuf,
    pub script: String,
    pub md5: String,
    pub depends_on: HashSet<String>,
    pub required_by: HashSet<String>,
}

pub struct DatabaseProject {
    pub project_dirpath: PathBuf,
    pub create_scripts: Vec<(PathBuf, String)>,
    pub drop_scripts: Vec<(PathBuf, String)>,
    pub migration_scripts: Vec<(String, String)>,
    pub objects: HashMap<String, DatabaseObject>,
}

impl DatabaseProject {
    fn from_path(project_path: &PathBuf) -> anyhow::Result<DatabaseProject> {

        let path_buf = project_path.join("create");
        let create_script_paths = utils::list_files(&path_buf)?;
        let mut create_scripts = vec![];
        for p in create_script_paths {
            let script = utils::read_file(&p)?;
            create_scripts.push((p, script));
        }

        let path_buf = project_path.join("drop");
        let drop_script_paths = utils::list_files(&path_buf)?;
        let mut drop_scripts = vec![];
        for p in drop_script_paths {
            let script = utils::read_file(&p)?;
            drop_scripts.push((p, script));
        }

        let path_buf = project_path.join("migrations");
        let migration_script_paths = utils::list_files(&path_buf)?;
        let mut migration_scripts = vec![];
        for p in migration_script_paths {
            let script = utils::read_file(&p)?;
            let migration_id = migration_id_from_path(&p)?;
            migration_scripts.push((migration_id, script));
        }
        
        // FIXME configurable search schemas???
        let mut search_schemas: HashSet<String> = HashSet::new();
        search_schemas.insert("public".into());

        let objects_info = load_objects_info(&project_path)?;
        let required_by = calc_required_by(&objects_info, &search_schemas);
        let depends_on = calc_depends_on(&required_by);
        let objects = build_database_objects(objects_info, required_by, depends_on)?;

        return Ok(DatabaseProject {
            project_dirpath: path_buf,
            create_scripts,
            drop_scripts,
            migration_scripts,
            objects,
        });
    }

    pub fn get_next_migration(&self, migration_id: &str) -> Option<(String, String)> {
        let migration_id = String::from(migration_id);
        for (next_migration_id, next_migration_script) in self.migration_scripts.iter() {
            if *next_migration_id > migration_id {
                return Some((next_migration_id.clone(), next_migration_script.clone()));
            }
        }
        return None;
    }

    pub fn get_execute_order(&self) -> anyhow::Result<Vec<String>> {
        return calc_execute_order(&self.objects);
    }
}


pub fn load() -> anyhow::Result<DatabaseProject> {
    let project_path = get_project_path()?;
    let database_project = DatabaseProject::from_path(&project_path)?;
    return Ok(database_project);
}

