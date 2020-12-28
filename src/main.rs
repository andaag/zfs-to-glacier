use indicatif::{ProgressBar, ProgressStyle};
use log::info;
use rusoto_core::{HttpClient, HttpConfig, Region, credential::DefaultCredentialsProvider};
use rusoto_s3::{S3Client, Tag};
use std::{cmp::max, convert::TryInto, default::Default, env, time::Duration};
use tokio::runtime;
use zfs_to_glacier::{cloudformation, compute_backups, config, s3_utils, zfs_utils};

use clap::{App, AppSettings, Arg};
use compute_backups::*;
use s3_utils::*;
use zfs_utils::*;

fn init_logging(verbose: bool) {
    if verbose {
        env::set_var("RUST_LOG", "zfs_to_glacier=debug");
    } else {
        env::set_var("RUST_LOG", "zfs_to_glacier=info");
    }
    let _ = env_logger::builder().try_init();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    runtime::Builder::new_multi_thread()
        .worker_threads(max(2, num_cpus::get()))
        .enable_all()
        .build()?
        .block_on(app())
}

async fn app() -> Result<(), Box<dyn std::error::Error>> {
    let app = App::new("ZFS S3 backup")
        .version("0.1")
        .author("Anders Aagaard <aagaande@gmail.com>")
        .about("Sync ZFS backups to S3")
        .subcommand(
            App::new("sync")
                .about("Sync state")
                .arg(
                    Arg::new("dryrun")
                        .short('n')
                        .about("Print expected actions but do nothing"),
                )
                .arg(Arg::new("verbose").short('v').about("Verbose logging")),
        )
        .subcommand(App::new("generateconfig").about("Generate default local config"))
        .subcommand(App::new("generatecloudformation").about("Generate cloudformation file"))
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    match app.subcommand() {
        Some(("sync", args)) => {
            let verbose = args.occurrences_of("verbose") > 0;
            init_logging(verbose);
            let dryrun = args.occurrences_of("dryrun") > 0;
            let config = config::read_config()?;
            let client = {
                let cred_provider =  DefaultCredentialsProvider::new().unwrap();
                let mut http_config = HttpConfig::new();
                http_config.read_buf_size(1024 * 1024 * 64);
                http_config.pool_idle_timeout(Some(Duration::from_secs(5)));
                let http_provider = HttpClient::new_with_config(http_config).unwrap();
                S3Client::new_with(http_provider, cred_provider, Region::default())
            };            

            let local_zfs_state = get_local_zfs_state()?;
            let mut actions: Vec<S3Backup> = Vec::new();
            for config in config.configs {
                let s3_backup_actions = get_pending_actions(&local_zfs_state, &config);
                let remote_files = get_all_files(&client, &config.bucket).await?;
                for backup_action in s3_backup_actions.filter_existing_backups(&remote_files) {
                    actions.push(backup_action);
                }
            }

            let mut actions_performed = 1;
            let total_actions = actions.len();

            for backup_action in actions {
                let estimated_size = backup_action.get_estimated_size()?;
                info!(
                    "Processing file {}/{} - {}",
                    actions_performed,
                    total_actions,
                    backup_action.key()
                );
                let pb = ProgressBar::new(estimated_size.try_into()?);
                let pb_template = {
                    if verbose {
                        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})\n"
                    } else {
                        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})"
                    }
                };
                pb.set_style(ProgressStyle::default_bar()
                    .template(pb_template)
                    .progress_chars("#>-"));

                if !dryrun {
                    let mut tags: Vec<Tag> = Vec::new();
                    tags.push(Tag {
                        key: "backup_cmd".to_string(),
                        value: backup_action.backup_cmd(false),
                    });
                    tags.push(Tag {
                        key: "parent".to_string(),
                        value: backup_action.parent.clone().unwrap_or("full".to_string()),
                    });
                    tags.push(Tag {
                        key: "creation_date".to_string(),
                        value: backup_action.snapshot.creation.to_rfc3339(),
                    });
                    upload_stdout(
                        &client,
                        Box::new(backup_action.backup(false)?),
                        &backup_action.bucket,
                        &backup_action.key(),
                        tags,
                        backup_action.storage_class,
                        estimated_size,
                        |bytes_sent| {
                            pb.set_position(bytes_sent);
                        },
                    )
                    .await?;
                } else {
                    info!("  Dryrun, skipping upload {}", &backup_action.key());
                }
                actions_performed += 1;
                pb.finish_with_message("File completed");
            }
        }
        Some(("generateconfig", _)) => {
            init_logging(false);
            config::write_default_config()?
        }
        Some(("generatecloudformation", _)) => {
            init_logging(false);
            let config = config::read_config()?;
            cloudformation::generate_cloudformation(&config)?
        }
        _ => {}
    }

    // @fixme future:
    // - storing the amazon etag like md5 checksum
    // - need to check that content online is in sync - on listing maybe confirm some sizes etc? Creation date?.. - maybe new snapshot?
    //    - check size, if file exists and file is wrong creation date/size we can complain.
    // - if we get an error that might be due to AWS_REGION we should put that info in the error.
    Ok(())
}
