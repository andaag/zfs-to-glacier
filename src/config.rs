use std::{error::Error, fs, path::Path};

use crate::s3_utils;
use log::debug;
use regex::Regex;
use s3_utils::StorageClass;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ZfsBackupConfigEntry {
    pub snapshot_regex: String,
    pub storage_class: StorageClass,
    pub expire_in_days: i64
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ZfsBackupConfig {
    pub pool_regex: String,
    pub incremental: ZfsBackupConfigEntry,
    pub full: ZfsBackupConfigEntry,
    pub bucket: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ZfsBaseConfig {
    pub configs: Vec<ZfsBackupConfig>,
}

impl ZfsBackupConfigEntry {
    pub fn snapshot_regex_re(&self) -> Regex {
        Regex::new(&self.snapshot_regex).unwrap()
    }
}

impl ZfsBackupConfig {
    pub fn pool_regex_re(&self) -> Regex {
        Regex::new(&self.pool_regex).unwrap()
    }
}

pub fn read_config() -> Result<ZfsBaseConfig, Box<dyn Error>> {
    debug!("Loading configuration file...");
    let contents = fs::read_to_string("config.yaml").expect("Failed to read config.yaml");

    let content: ZfsBaseConfig = serde_yaml::from_str(&contents)?;
    Ok(content)
}

pub fn write_default_config() -> Result<(), Box<dyn Error>> {
    if Path::new("config.yaml").exists() {
        panic!("Cowardly not creating config.yaml, as the file already exists");
    }
    debug!("Writing default configuration file...");
    fs::write(
        "config.yaml",
        "configs:
- pool_regex: \"rpool/.*\"
  incremental:
    snapshot_regex: \"daily\"
    storage_class: \"StandardInfrequentAccess\"
    expire_in_days: 40
  full:
    snapshot_regex: \"monthly\"
    storage_class: \"DeepArchive\" #minimum storage period as of this writing is 180 days for deeparchive.
    expire_in_days: 200
  bucket: \"zfs-rpool\" #You can backup multiple pools to one bucket.",
    )?;
    println!("config.yaml written");
    Ok(())
}
