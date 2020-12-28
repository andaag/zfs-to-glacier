use log::info;
use std::{collections::HashMap, error::Error};
use zfs_to_glacier::{
    cmd_execute::{Executor, ExecutorCommand},
    compute_backups::{S3Backup, S3BackupCommand},
};
use zfs_to_glacier::{
    compute_backups::{get_pending_actions, FilterExistingFiles},
    config::*,
};
use zfs_to_glacier::{
    s3_utils::*,
    zfs_utils::{LocalZfsState, ZfsSnapshot},
};
mod common;
use common::*;
use pretty_assertions::assert_eq;
use log::log;
use log::Level::Info;
use testcontainers::*;

macro_rules! test_step {
    (target: $target:expr, $($arg:tt)+) => (
        log!(target: $target, Info, " * * * * * * * * * * * * * * * * * * * * * * * * * * * ");
        log!(target: $target, Info, $($arg)+);
        log!(target: $target, Info, " * * * * * * * * * * * * * * * * * * * * * * * * * * * ");
    );
    ($($arg:tt)+) => (
        log!(Info, " * * * * * * * * * * * * * * * * * * * * * * * * * * * ");
        log!(Info, $($arg)+);
        log!(Info, " * * * * * * * * * * * * * * * * * * * * * * * * * * * ");
    )
}

struct S3TestBackup {
    pub inner: S3Backup,
}

impl S3BackupCommand for S3TestBackup {
    fn backup_cmd(&self, dryrun: bool) -> String {
        let dryrun_char = if dryrun { "n" } else { "" };
        match &self.inner.parent {
            Some(parent) => format!(
                "echo -n zfs send -vPw{} -i {} {}",
                dryrun_char, parent, self.inner.snapshot.name
            ),
            None => format!("echo -n zfs send -vPw{} {}", dryrun_char, self.inner.snapshot.name),
        }
    }

    fn backup(&self, dryrun: bool) -> Result<std::process::Child, Box<dyn Error>> {
        Ok(ExecutorCommand(self.backup_cmd(dryrun)).spawn()?)
    }

    fn get_estimated_size(&self) -> Result<usize, Box<dyn Error>> {
        self.inner.get_estimated_size()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_actions() -> Result<(), Box<dyn std::error::Error>> {
    log_init("integration_full");
    execute_in_docker!((|| async {
        let bucket = generate_unique_name();
        let client = create_client(&bucket).await?;
        let config = create_standard_config(&bucket);

        test_step!("Synchronizing initial data");
        let local_state = LocalZfsState {
            pools: {
                let mut pool_state: HashMap<String, Vec<ZfsSnapshot>> = HashMap::new();
                pool_state.insert("backup_pool".to_string(), Vec::new());
                pool_state.insert(
                    "backup_pool/backup".to_string(),
                    vec![
                        ZfsSnapshot::new("backup_pool/backup@1_yearly", chrono::Duration::days(20))?,
                        ZfsSnapshot::new("backup_pool/backup@2_monthly", chrono::Duration::days(19))?,
                        ZfsSnapshot::new("backup_pool/backup@3_ignored", chrono::Duration::days(18))?,
                        ZfsSnapshot::new("backup_pool/backup@4_daily", chrono::Duration::days(17))?,
                    ],
                );
                pool_state
            },
        };

        info!("Getting pending actions");
        let actions = get_pending_actions(&local_state, &config);
        {
            // Confirm actions are set up correctly
            assert_eq!(
                &actions,
                &vec![
                    S3Backup::new("backup_pool/backup@1_yearly", &bucket, chrono::Duration::days(20), None)?,
                    S3Backup::new("backup_pool/backup@2_monthly", &bucket, chrono::Duration::days(19), None)?,
                    S3Backup::new(
                        "backup_pool/backup@4_daily",
                        &bucket,
                        chrono::Duration::days(17),
                        Some("backup_pool/backup@2_monthly".to_string())
                    )?
                ]
            );
        }

        let local_actions: Vec<S3TestBackup> = actions
            .into_iter()
            .map(|x| S3TestBackup { inner: x })
            .collect();
        assert_eq!(local_actions.len(), 3);

        test_step!("Executing actions");
        for action in local_actions {
            info!("     upload {}", action.inner.key());
            let child = ExecutorCommand(action.backup_cmd(false)).spawn()?;
            upload_stdout(
                &client,
                Box::new(child),
                &bucket,
                &action.inner.key(),
                vec![],
                StorageClass::STANDARD,
                0,
                |_| {}
            ).await?;
        }

        info!("Confirming actions executed");
        {
            // Confirm uploads are now consistent
            assert_eq!(
                download_file(&bucket, "full/backup_pool/backup_AT_1_yearly", &client).await?,
                "zfs send -vPw backup_pool/backup@1_yearly"
            );
            assert_eq!(
                download_file(&bucket, "full/backup_pool/backup_AT_2_monthly", &client).await?,
                "zfs send -vPw backup_pool/backup@2_monthly"
            );
            assert_eq!(
                download_file(
                    &bucket,
                    "incremental/backup_pool/backup_AT_4_daily",
                    &client
                ).await?,
                "zfs send -vPw -i backup_pool/backup@2_monthly backup_pool/backup@4_daily"
            );
        }

        test_step!("Testing upload of new day");
        // Test upload of new day.
        let local_state = LocalZfsState {
            pools: {
                //@fixme : I can do a macro for this one, see https://doc.rust-lang.org/1.7.0/book/macros.html
                let mut pool_state: HashMap<String, Vec<ZfsSnapshot>> = HashMap::new();
                pool_state.insert("backup_pool".to_string(), Vec::new());
                pool_state.insert(
                    "backup_pool/backup".to_string(),
                    vec![
                        ZfsSnapshot::new("backup_pool/backup@1_yearly", chrono::Duration::days(20))?,
                        ZfsSnapshot::new("backup_pool/backup@2_monthly", chrono::Duration::days(19))?,
                        ZfsSnapshot::new("backup_pool/backup@3_ignored", chrono::Duration::days(18))?,
                        ZfsSnapshot::new("backup_pool/backup@4_daily", chrono::Duration::days(17))?,
                        ZfsSnapshot::new("backup_pool/backup@5_daily", chrono::Duration::days(16))?,
                    ],
                );
                pool_state
            },
        };

        info!("Getting remote s3 bucket state");
        let remote_state = get_all_files(&client, &config.bucket).await?;

        info!("Getting local actions");
        let total_local_actions = get_pending_actions(&local_state, &config);
        assert_eq!(total_local_actions.len(), 4);
        let local_actions: Vec<S3TestBackup> = total_local_actions
            .filter_existing_backups(&remote_state)
            .into_iter()
            .map(|x| S3TestBackup { inner: x })
            .collect();
        assert_eq!(local_actions.len(), 1);

        info!("Executing actions");
        for action in local_actions {
            let child = ExecutorCommand(action.backup_cmd(false)).spawn()?;
            upload_stdout(
                &client,
                Box::new(child),
                &bucket,
                &action.inner.key(),
                vec![],
                StorageClass::STANDARD,
                0,
                |_| {}
            ).await?;
        }
        {
            // Confirm uploads are now consistent
            assert_eq!(
                download_file(
                    &bucket,
                    "incremental/backup_pool/backup_AT_5_daily",
                    &client
                ).await?,
                "zfs send -vPw -i backup_pool/backup@4_daily backup_pool/backup@5_daily"
            );
        }

        Ok(())
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignore_expired() -> Result<(), Box<dyn Error>> {
    log_init("integration_full");
    execute_in_docker!((|| async {
        let bucket = generate_unique_name();
        let config = create_standard_config(&bucket);

        let local_state = LocalZfsState {
            pools: {
                let mut pool_state: HashMap<String, Vec<ZfsSnapshot>> = HashMap::new();
                pool_state.insert("backup_pool".to_string(), Vec::new());
                pool_state.insert(
                    "backup_pool/backup".to_string(),
                    vec![
                        ZfsSnapshot::new("backup_pool/backup@1_yearly", chrono::Duration::days(365))?,
                        ZfsSnapshot::new("backup_pool/backup@2_yearly", chrono::Duration::days(1))?,
                    ],
                );
                pool_state
            },
        };

        info!("Getting pending actions");
        let actions = get_pending_actions(&local_state, &config);
        {
            // Confirm actions are set up correctly
            assert_eq!(
                &actions,
                &vec![
                    S3Backup::new("backup_pool/backup@2_yearly", &bucket, chrono::Duration::days(1), None)?,
                ]
            );
        }

        Ok(())
    }))
}


fn create_standard_config(bucket: &str) -> ZfsBackupConfig {
    ZfsBackupConfig {
        pool_regex: "backup_pool.*".to_string(),
        incremental: ZfsBackupConfigEntry {
            snapshot_regex: "daily.*".to_string(),
            storage_class: StorageClass::DeepArchive,
            expire_in_days: 40
        },
        full: ZfsBackupConfigEntry {
            snapshot_regex: "(yearly|monthly).*".to_string(),
            storage_class: StorageClass::DeepArchive,
            expire_in_days: 200
        },
        bucket: bucket.to_string(),
    }
}
