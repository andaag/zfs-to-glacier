use std::{collections::HashSet, fmt};
use std::{error::Error, iter::FromIterator, process::Child};

use crate::cmd_execute::Executor;
use crate::{
    cmd_execute::ExecutorCommand,
    config::ZfsBackupConfig,
    s3_utils::{S3Key, StorageClass},
    zfs_utils::{LocalZfsState, ZfsSnapshot},
};
use chrono::{Duration, Local};
use log::{debug, warn};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct S3Backup {
    pub snapshot: ZfsSnapshot,
    pub parent: Option<String>,
    pub storage_class: StorageClass,
    pub bucket: String,
}

impl S3Backup {
    pub fn key(&self) -> String {
        let mut key: String = match &self.parent {
            Some(_) => "incremental/".to_string(),
            None => "full/".to_string(),
        };
        key.push_str(&self.snapshot.name.replace("@", "_AT_"));
        key
    }
}
pub trait S3BackupCommand {
    fn backup_cmd(&self, dryrun: bool) -> String;
    fn backup(&self, dryrun: bool) -> Result<Child, Box<dyn Error>>;
    fn get_estimated_size(&self) -> Result<usize, Box<dyn Error>>;
}

impl S3BackupCommand for S3Backup {
    fn backup_cmd(&self, dryrun: bool) -> String {
        let dryrun_char = if dryrun { "vn" } else { "" };
        match &self.parent {
            Some(parent) => format!(
                "zfs send -Pw{} -i {} {}",
                dryrun_char, parent, self.snapshot.name
            ),
            None => format!("zfs send -Pw{} {}", dryrun_char, self.snapshot.name),
        }
    }
    fn backup(&self, dryrun: bool) -> Result<Child, Box<dyn Error>> {
        Ok(ExecutorCommand(self.backup_cmd(dryrun)).spawn()?)
    }
    fn get_estimated_size(&self) -> Result<usize, Box<dyn Error>> {
        let estimated_size = ExecutorCommand(self.backup_cmd(true))
            .execute()?
            .split("\t")
            .last()
            .expect(&format!(
                "Failed to get estimated size with {}",
                self.backup_cmd(true)
            ))
            .to_string();
        Ok(estimated_size.trim().parse::<usize>().expect(&format!(
            "Failed to parse estimated size : '{}'",
            estimated_size
        )))
    }
}

impl fmt::Display for S3Backup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "S3Backup {}, incremental {}",
            self.key(),
            self.parent.is_some()
        )
    }
}

trait S3BackupActions {
    fn new(name: &ZfsSnapshot, parent: Option<&ZfsSnapshot>, config: &ZfsBackupConfig) -> S3Backup;
}
impl S3BackupActions for S3Backup {
    fn new(
        snapshot: &ZfsSnapshot,
        parent: Option<&ZfsSnapshot>,
        config: &ZfsBackupConfig,
    ) -> S3Backup {
        let storage_class = {
            if parent.is_some() {
                config.incremental.storage_class
            } else {
                config.full.storage_class
            }
        };

        S3Backup {
            snapshot: snapshot.to_owned(),
            parent: parent.map(|x| x.name.to_owned()),
            storage_class: storage_class,
            bucket: config.bucket.to_owned()
        }
    }
}

pub trait FilterExistingFiles {
    fn filter_existing_backups(self, existing: &HashSet<S3Key>) -> Vec<S3Backup>;
}

impl FilterExistingFiles for Vec<S3Backup> {
    fn filter_existing_backups(self, existing: &HashSet<S3Key>) -> Vec<S3Backup> {
        let existing_keys: HashSet<String> =
            HashSet::from_iter(existing.into_iter().map(|x| x.key.clone()));
        self.into_iter()
            .filter(|x| !existing_keys.contains(&x.key()))
            .collect()
    }
}

pub fn get_pending_actions(local_state: &LocalZfsState, config: &ZfsBackupConfig) -> Vec<S3Backup> {
    let mut pending_backups: Vec<S3Backup> = Vec::new();
    for pool in local_state.pools.keys() {
        if !config.pool_regex_re().is_match(pool) {
            continue;
        }
        debug!("Pool '{}' is active", pool);
        let snapshots = local_state.pools.get(pool).unwrap();
        let mut last_entry: Option<&ZfsSnapshot> = None;
        for snapshot in snapshots {
            if config
                .incremental
                .snapshot_regex_re()
                .is_match(&snapshot.name)
            {
                if last_entry.is_none() {
                    warn!(
                        "\tWARN : can't incremental snapshot {}, no parent available",
                        snapshot
                    )
                } else {
                    if Local::now().signed_duration_since(snapshot.creation)
                        > Duration::days(config.incremental.expire_in_days + 1)
                    {
                        debug!("    snapshot incremental {} - skipped, too old", snapshot);
                    } else {
                        debug!("    snapshot incremental {}", snapshot);
                        pending_backups.push(S3Backup::new(snapshot, last_entry, config));
                    }
                    last_entry = Some(&snapshot);
                }
            } else if config.full.snapshot_regex_re().is_match(&snapshot.name) {
                if Local::now().signed_duration_since(snapshot.creation)
                    > Duration::days(config.full.expire_in_days + 1)
                {
                    debug!("    snapshot full {} - skipped, too old", snapshot);
                } else {
                    debug!("    snapshot full {}", snapshot);
                    pending_backups.push(S3Backup::new(snapshot, None, config));
                }
                last_entry = Some(&snapshot);
            }
        }
    }
    pending_backups
}
