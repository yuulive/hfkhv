

use std::fs;
use std::io::prelude::*;
use std::path;
use std::env;
use path::PathBuf;
use anyhow;
use anyhow::Context;



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
