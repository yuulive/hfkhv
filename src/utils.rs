

use std::fs;
use std::io::prelude::*;
use std::path;
use std::env;
use path::PathBuf;
use anyhow;
use anyhow::Context;

#[cfg(test)]
mod tests;

pub fn read_file(path_buf: &PathBuf) -> anyhow::Result<String> {
    let result = fs::read_to_string(path_buf).with_context(|| format!("failed to read file: {:?}", path_buf))?;
    return Ok(result);
}

pub fn write_file(path_buf: &PathBuf, content: &str) -> anyhow::Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path_buf)
        .with_context(|| format!("failed to open file for writing: {:?}", path_buf))?;
        
    file.write_all(content.as_bytes()).with_context(|| format!("failed to write into file: {:?}", path_buf))?;
    return Ok(());
}

pub fn list_files(path_buf: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let dir = fs::read_dir(path_buf).with_context(|| format!("failed to read dir: {:?}", path_buf))?;
    let mut result = vec![];
    for dir_entry_result in dir {
        let dir_entry = dir_entry_result.with_context(|| format!("failed to read dir entry: {:?}", path_buf))?;
        result.push(dir_entry.path());
    }
    result.sort();
    return Ok(result);
}

pub fn read_env_var(key: &str) -> anyhow::Result<String> {
    let result = env::var(key).with_context(|| format!("failed to read env variable: {:?}", key))?;
    return Ok(result);
}

fn validate_admin_script_param(p: &str) -> anyhow::Result<()> {
    if p.len() == 0 {
        bail!("admin script parameter is empty");
    }
    for c in p.chars() {
        if c.is_alphanumeric() || c == '_' {
            continue;
        } else {
            // could it leak too much password details into console?
            bail!("admin script parameter countains invalid character ({})", c);
        }
    }
    return Ok(());
}

pub fn get_password() -> anyhow::Result<Option<String>> {
    let connection_string = read_env_var("PGFINE_CONNECTION_STRING")?;
    let pg_config = connection_string.parse::<postgres::Config>()?;
    let password_result = pg_config.get_password();
    match password_result {
        Some(bytes) => {
            let password_str = std::str::from_utf8(bytes)
                .context("get_pasword failed convert utf-8")?;
            validate_admin_script_param(&password_str)?;
            return Ok(Some(password_str.into()));
        }
        None => {
            return Ok(None);
        }
    }
}

pub fn get_database_name() -> anyhow::Result<String> {
    let connection_string = read_env_var("PGFINE_CONNECTION_STRING")?;
    let pg_config = connection_string.parse::<postgres::Config>()?;
    let database_name_result = pg_config.get_dbname();
    match database_name_result {
        Some(database_name) => {
            validate_admin_script_param(&database_name)?;
            return Ok(database_name.into());
        }
        None => {
            bail!("could not read the dbname parameter from connection string PGFINE_CONNECTION_STRING");
        }
    }
}

pub fn get_role_name() -> anyhow::Result<String> {
    let connection_string = read_env_var("PGFINE_CONNECTION_STRING")?;
    let pg_config = connection_string.parse::<postgres::Config>()?;
    let role_name_result = pg_config.get_user();
    match role_name_result {
        Some(role_name) => {
            validate_admin_script_param(&role_name)?;
            return Ok(role_name.into());
        }
        None => {
            bail!("could not get user parameter from connection string PGFINE_CONNECTION_STRING");
        }
    }
}

pub fn contains_whole_word(text: &str, search_term: &str) -> bool {

    let parts: Vec<&str> = text.split(search_term).collect();
    let parts_len = parts.len();
    if parts_len < 2 {
        return false;
    }

    for i in 1..parts_len {
        let left = parts[i - 1];
        let right = parts[i];
        let left_c = left.chars().last();
        let right_c = right.chars().next();
        let left_bound_exists = match left_c {
            Some(c) => !(c.is_alphanumeric() || c == '_'),
            None => true,
        };

        let right_bound_exists = match right_c {
            Some(c) => !(c.is_alphanumeric() || c == '_'),
            None => true,
        };

        if left_bound_exists && right_bound_exists {
            return true;
        }
    }

    return false;
}

pub fn contains_whole_word_ci(text: &str, search_term: &str) -> bool {
    let text_lower = text.to_lowercase();
    let search_term_lower = search_term.to_lowercase();
    return contains_whole_word(&text_lower, &search_term_lower);
}

pub fn validate_environment() -> anyhow::Result<()> {
    let err_msg = "all pgfine variables are mandatory to avoid mixed environments";
    read_env_var("PGFINE_CONNECTION_STRING").context(err_msg)?;
    read_env_var("PGFINE_ADMIN_CONNECTION_STRING").context(err_msg)?;
    read_env_var("PGFINE_DIR").context(err_msg)?;
    read_env_var("PGFINE_ROLE_PREFIX").context(err_msg)?;
    read_env_var("PGFINE_ROOT_CERT").context(err_msg)?;
    return Ok(());
}

