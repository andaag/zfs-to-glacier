use chrono::Local;
use log::info;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rusoto_core::Region;
use rusoto_s3::{CreateBucketRequest, GetObjectRequest, GetObjectTaggingRequest, S3, S3Client};
use std::env;
use std::error::Error;
use std::{str};
use zfs_to_glacier::{compute_backups::S3Backup, s3_utils::StorageClass, zfs_utils::ZfsSnapshot};
use tokio::io::AsyncReadExt;

pub const ACCESS_KEY: &str = "minio";
pub const SECRET_KEY: &str = "minio1234";
pub const ENDPOINT: &str = "http://127.0.0.1:9000";

pub fn log_init(module_name: &str) {
    env::set_var("RUST_LOG", format!("{},zfs_to_glacier", module_name));
    env::set_var("RUST_LOG_STYLE", "always");
    let _ = env_logger::builder().is_test(true).try_init();
}

pub fn generate_unique_name() -> String {
    let mut rng = rand::thread_rng();
    let data = String::from_utf8(
        std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(20)
            .collect(),
    )
    .unwrap();
    format!("test-{}", data).to_lowercase()
}

#[macro_export]
macro_rules! execute_in_docker {
    ($closure:tt) =>  {
        {
            use images::generic::WaitFor;
            use testcontainers::*;
            use std::env;
            let docker = clients::Cli::default();
            let image = images::generic::GenericImage::new(format!("{}:{}", "minio/minio", "latest"))
                .with_mapped_port((9000, 9000))
                .with_env_var("MINIO_ACCESS_KEY", ACCESS_KEY)
                .with_env_var("MINIO_SECRET_KEY", SECRET_KEY)
                .with_wait_for(WaitFor::LogMessage {
                    message: "Browser Access:".to_string(),
                    stream: images::generic::Stream::StdOut,
                })
                .with_args(vec!["server".to_string(), "/data".to_string()]);
            env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
            env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
            env::set_var("S3_ENDPOINT_URL", ENDPOINT);
            let container = docker.run(image);
            let result = $closure().await;
            container.stop();
            if result.is_err() {
                let mut buffer = String::new();
                container.logs().stdout.read_to_string(&mut buffer).unwrap();
                println!(" - * - * - * - * - * - * - * - * - * - * - * - *");
                println!("Error in stdout, dumping container logs:");
                println!("Stdout: {}", buffer);
                buffer.clear();
                container.logs().stderr.read_to_string(&mut buffer).unwrap();
                println!("stderr: {}", buffer);
                println!(" - * - * - * - * - * - * - * - * - * - * - * - *");
            }
            result
        }
        
    };
}

pub async fn create_client(bucket: &str) -> Result<S3Client, Box<dyn Error>> {
    let region = Region::Custom {
        name: "us-east-1".to_owned(),
        endpoint: ENDPOINT.to_string(),
    };
    let client = S3Client::new(region);
    client
        .create_bucket(CreateBucketRequest {
            bucket: bucket.to_string(),
            ..Default::default()
        })
        .await?;
    Ok(client)
}

pub async fn download_file(bucket: &str, key: &str, client: &S3Client) -> Result<String, Box<dyn Error>> {
    info!("Downloading file s3://{}/{}", bucket, key);
    let request = client
        .get_object(GetObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        })
        .await?;
    let mut stream = request.body.unwrap().into_async_read();
    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await?;
    Ok(str::from_utf8(&buffer)?.to_string())
}

pub async fn get_tags(bucket: &str, key: &str, client: &S3Client) -> Result<Vec<rusoto_s3::Tag>, Box<dyn Error>> {
    let request = client.get_object_tagging(GetObjectTaggingRequest {
        bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
    }).await?;
    let mut tagset = request.tag_set;
    tagset.sort_by(|a,b| a.key.partial_cmp(&b.key).unwrap());    
    Ok(tagset)
}

pub trait ZfsSnapshotTesting {
    fn new(name: &str, time_since_now: chrono::Duration) -> Result<ZfsSnapshot, Box<dyn Error>>;
}

impl ZfsSnapshotTesting for ZfsSnapshot {
    fn new(name: &str, time_since_now: chrono::Duration) -> Result<ZfsSnapshot, Box<dyn Error>> {
        Ok(ZfsSnapshot {
            name: name.to_string(),
            creation: Local::now().date().and_hms(0, 0, 0) - time_since_now,
        })
    }
}

pub trait S3BackupTesting {
    fn new(
        name: &str,
        bucket: &str,
        time_since_now: chrono::Duration,
        parent: Option<String>,
    ) -> Result<S3Backup, Box<dyn Error>>;
}

impl S3BackupTesting for S3Backup {
    fn new(
        name: &str,
        bucket: &str,
        time_since_now: chrono::Duration,
        parent: Option<String>,
    ) -> Result<S3Backup, Box<dyn Error>> {
        Ok(S3Backup {
            snapshot: ZfsSnapshot {
                name: name.to_string(),
                creation: Local::now().date().and_hms(0, 0, 0) - time_since_now,
            },
            parent: parent,
            storage_class: StorageClass::DeepArchive,
            bucket: bucket.to_string(),
        })
    }
}
