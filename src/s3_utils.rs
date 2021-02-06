use crate::cmd_execute;

use async_channel::{Receiver, Sender};
use cmd_execute::CommandStreamActions;
use futures::future;
use log::{debug, error, warn};
use md5::Digest;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use rusoto_core::ByteStream;
use rusoto_s3::{CreateMultipartUploadRequest, ListObjectsV2Request, S3Client, Tag, S3};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::default::Default;
use std::error::Error;
use std::io::Read;
use std::str;
use std::time;
use std::{convert::TryInto, io::BufReader};
use std::{
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::task::JoinHandle;

const MAX_S3_PART_COUNT: usize = 10000;

#[derive(Hash, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum StorageClass {
    STANDARD,
    Glacier,
    DeepArchive,
    StandardInfrequentAccess
}

impl ToString for StorageClass {
    fn to_string(&self) -> String {
        match self {
            StorageClass::STANDARD => "STANDARD".to_string(),
            StorageClass::Glacier => "GLACIER".to_string(),
            StorageClass::DeepArchive => "DEEP_ARCHIVE".to_string(),
            StorageClass::StandardInfrequentAccess => "STANDARD_IA".to_string(),
        }
    }
}

#[derive(Hash, PartialEq, Eq, Debug)]
pub struct S3Key {
    pub key: String,
    pub etag: String,
}

macro_rules! _wrapper {
    ($f:expr) => {{ /* code from previous section */ }};
    // Variadic number of args (Allowing trailing comma)
    ($f:expr, $( $args:expr $(,)? )* ) => {{
        $f( $($args,)* )
    }};
}

#[derive(Debug)]
pub struct S3UploadFailedError(String, String);
impl fmt::Display for S3UploadFailedError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "S3 upload operation {} failed with error: {}",
            self.0, self.1
        )
    }
}
impl Error for S3UploadFailedError {}

macro_rules! retry {
    ($( $args:expr$(,)? )+) => {{
        let mut attempt:u64 = 1;
        loop {
            let res = _wrapper!($( $args, )*).await;
            if res.is_ok() {
                break res;
            }
            if attempt < 20 {
                warn!("\nTask failed, retrying... attempt {}\n{}\n\n", attempt, res.unwrap_err());
                std::thread::sleep(time::Duration::from_secs(attempt * 2));
                attempt += 1;
                continue;
            }
            warn!("Task failed, ran out of retry attempts!");
            break res;
        }
    }};
}

pub async fn get_all_files(
    client: &S3Client,
    bucket: &str,
) -> Result<HashSet<S3Key>, Box<dyn Error>> {
    let mut scan: bool = true;
    let mut continuation_token: Option<String> = None;
    let mut result: HashSet<S3Key> = HashSet::new();

    while scan {
        let request = client
            .list_objects_v2(ListObjectsV2Request {
                bucket: bucket.to_string(),
                start_after: continuation_token,
                ..Default::default()
            })
            .await?;
        continuation_token = request.continuation_token;
        scan = request.is_truncated.unwrap_or(false);

        if request.contents.is_some() {
            for entry in request.contents.unwrap() {
                let key = entry.key.unwrap().to_string();
                result.insert(S3Key {
                    key: key.to_owned(),
                    etag: entry.e_tag.unwrap().to_string(),
                });
            }
        }
    }
    Ok(result)
}

#[derive(Clone)]
struct UploadContext {
    client: S3Client,
    bucket: String,
    key: String,
    upload_id: String,
    data_sent: Arc<AtomicUsize>,
    buf_size: usize,
}

impl UploadContext {
    fn get_bytes_sent(&self) -> usize {
        self.data_sent.load(Ordering::SeqCst)
    }
}

async fn upload_stdout_send_parts<'a, T: Read, F>(
    upload_context: UploadContext,
    mut child: Box<dyn CommandStreamActions<T> + 'a>,
    callback: F,
) -> Result<Vec<rusoto_s3::CompletedPart>, Box<dyn Error>>
where
    F: Fn(u64) -> (),
{
    type BufferChannel = (i64, Vec<u8>);
    type CompletedPartChannel = Result<rusoto_s3::CompletedPart, String>;

    let (tx_buffer, rx_buffer): (Sender<BufferChannel>, Receiver<BufferChannel>) =
        async_channel::bounded(2);
    let (tx_completedpart, rx_completedpart): (
        Sender<CompletedPartChannel>,
        Receiver<CompletedPartChannel>,
    ) = async_channel::unbounded();
    let mut completed_parts: Vec<rusoto_s3::CompletedPart> = Vec::new();

    let senders: Vec<JoinHandle<Result<(), String>>> =
        (0..num_cpus::get())
            .map(|sender_thread| {
                let rx_channel = rx_buffer.clone();
                let tx_completedpart_channel = tx_completedpart.clone();
                let upload_context = upload_context.clone();
                tokio::spawn(async move {
                    while let Ok((part_count, buffer)) = rx_channel.recv().await {
                        let content_md5 = base64::encode(md5::Md5::digest(&buffer));
                        let buffer_size: usize = buffer.len();

                        let completed_part = retry!(
                            |upload_context: UploadContext,
                             buffer: Vec<u8>,
                             content_md5: String| async move {
                                debug!(
                                "  sender:Part start multipart upload s3://{}/{} - part {} - thread {}",
                                upload_context.bucket,
                                upload_context.key,
                                part_count,
                                sender_thread
                            );
                                let e_tag = upload_context
                                    .client
                                    .upload_part(rusoto_s3::UploadPartRequest {
                                        bucket: upload_context.bucket.to_string(),
                                        key: upload_context.key.to_string(),
                                        upload_id: upload_context.upload_id.to_string(),
                                        body: { Some(ByteStream::from(buffer)) },
                                        content_length: Some(buffer_size.try_into().unwrap()),
                                        content_md5: Some(content_md5),
                                        part_number: part_count,
                                        ..Default::default()
                                    })
                                    .await
                                    .map(|x| x.e_tag.unwrap());
                                debug!(
                                    "  sender:Part completed multipart upload s3://{}/{} - part {} thread {}",
                                    &upload_context.bucket, &upload_context.key, part_count, sender_thread
                                );
                                upload_context
                                    .data_sent
                                    .fetch_add(buffer_size, Ordering::SeqCst);
                                Ok(rusoto_s3::CompletedPart {
                                    e_tag: Some(e_tag.map_err(|x| x.to_string())?.clone()),
                                    part_number: Some(part_count),
                                })
                            },
                            upload_context.clone(),
                            buffer.clone(),
                            content_md5.clone()
                        );
                        tx_completedpart_channel
                            .send(completed_part)
                            .await
                            .map_err(|x| x.to_string())?;
                    }
                    Ok(())
                })
            })
            .collect();
    drop(tx_completedpart);

    {
        let mut part_count: i64 = 0;
        let mut stdout = BufReader::with_capacity(upload_context.buf_size, child.as_mut().stdout());
        let stdout_ref = stdout.by_ref();
        loop {
            part_count = part_count + 1;
            let (buffer, bytes_read) = {
                let mut b = Vec::with_capacity(upload_context.buf_size);
                let bytes_read = stdout_ref
                    .take(b.capacity().try_into().unwrap())
                    .read_to_end(&mut b)
                    .unwrap();
                (b, bytes_read)
            };
            while let Ok(result) = rx_completedpart.try_recv() {
                // extra loop to make sure we exit early if a failure occures.
                completed_parts.push(result?);
            }
            if bytes_read > 0 {
                tx_buffer.send((part_count, buffer)).await?;
                (callback)(upload_context.get_bytes_sent().try_into()?);
            } else {
                debug!("End of file reached");
                break;
            }
        }
    }
    drop(tx_buffer);

    // Join all channels and confirm results are ok.
    for sender in future::join_all(senders).await {
        let sender = sender?;
        sender?;
    }

    let exit_status = child.wait()?;
    if !exit_status.success() {
        error!("zfs command exited with failure code {}", exit_status);
        Err(Box::new(S3UploadFailedError("uploadparts".to_string(), format!("zfs command exited with error code {}", exit_status))))
    } else {
        let completed_parts = {
            // finish building completed parts
            while let Ok(result) = rx_completedpart.recv().await {
                completed_parts.push(result?);
            }    
            completed_parts.sort_by(|a, b| a.part_number.partial_cmp(&b.part_number).unwrap());
            completed_parts
        };
        Ok(completed_parts)
    }
}

pub async fn upload_stdout_internal<'a, T: Read, F>(
    client: &S3Client,
    child: Box<dyn CommandStreamActions<T> + 'a>,
    bucket: &str,
    key: &str,
    tags: Vec<Tag>,
    storage_class: StorageClass,
    callback: F,
    buf_size: usize,
) -> Result<u64, Box<dyn Error>>
where
    F: Fn(u64) -> (),
{
    let tags = {
        let mut tags = tags;
        tags.push(rusoto_s3::Tag {
            key: "buffer_size".to_string(),
            value: buf_size.to_string(),
        });
        let mut result = String::new();
        for tag in tags {
            if result.len() > 0 {
                result.push('&');
            }
            result.push_str(&utf8_percent_encode(&tag.key, NON_ALPHANUMERIC).to_string());
            result.push_str("=");
            result.push_str(&utf8_percent_encode(&tag.value, NON_ALPHANUMERIC).to_string());
        }
        result
    };
    let upload_id: Result<String, Box<dyn Error>> = {
        retry!(
            |client: S3Client, bucket: String, key: String, tags: String| async move {
                let upload_id = client
                    .create_multipart_upload(CreateMultipartUploadRequest {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        storage_class: Some(storage_class.to_string()),
                        tagging: Some(tags),
                        ..Default::default()
                    })
                    .await
                    .map(|output| output.upload_id.unwrap())?;
                Ok(upload_id)
            },
            client.clone(),
            bucket.to_string(),
            key.to_string(),
            tags.clone()
        )
    };
    let upload_context = UploadContext {
        client: client.clone(),
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id?.clone(),
        data_sent: Arc::new(AtomicUsize::new(0)),
        buf_size: buf_size,
    };

    match upload_stdout_send_parts(upload_context.clone(), child, callback).await {
        Ok(completed_parts) => {
            debug!(
                "  Completing file s3://{}/{}",
                &upload_context.bucket, &upload_context.key
            );
            let r: Result<(), Box<dyn Error>> = retry!(
                |upload_context: UploadContext, completed_parts: Vec<rusoto_s3::CompletedPart>| async move {
                    upload_context
                        .client
                        .complete_multipart_upload(rusoto_s3::CompleteMultipartUploadRequest {
                            bucket: upload_context.bucket.clone(),
                            key: upload_context.key.clone(),
                            upload_id: upload_context.upload_id.clone(),
                            multipart_upload: Some(rusoto_s3::CompletedMultipartUpload {
                                parts: Some(completed_parts.clone()),
                            }),
                            ..Default::default()
                        })
                        .await?;
                    Ok(())
                },
                upload_context.clone(),
                completed_parts.clone()
            );
            r?;
            Ok(upload_context.get_bytes_sent().try_into()?)
        }
        Err(original_err) => {
            warn!("  Aborting multipart upload file s3://{}/{}", bucket, key);
            let r: Result<(), Box<dyn Error>> = retry!(
                |upload_context: UploadContext| async move {
                    client
                        .abort_multipart_upload(rusoto_s3::AbortMultipartUploadRequest {
                            bucket: upload_context.bucket.clone(),
                            key: upload_context.key.clone(),
                            upload_id: upload_context.upload_id.clone(),
                            ..Default::default()
                        })
                        .await?;
                    Ok(())
                },
                upload_context.clone()
            );
            match r {
                Ok(_) => {
                    Err(original_err)
                }
                Err(err) => {
                    error!("Error during multipart upload, in addition abort_multipart_upload also failed: {}", err.to_string());
                    Err(original_err)
                }
            }
        }
    }
}

pub async fn upload_stdout<'a, T: Read, F>(
    client: &S3Client,
    child: Box<dyn CommandStreamActions<T> + 'a>,
    bucket: &str,
    key: &str,
    tags: Vec<Tag>,
    storage_class: StorageClass,
    estimated_size: usize,
    callback: F,
) -> Result<u64, Box<dyn Error>>
where
    F: Fn(u64) -> (),
{
    let buf_size = {
        let mut buf_size = 8 * 1024 * 1024;
        let safe_estimated_size = estimated_size * 2; // estimated_size can be compressed considerably..
        loop {
            if safe_estimated_size / buf_size < MAX_S3_PART_COUNT {
                break;
            }
            buf_size = buf_size * 2;
        }
        buf_size
    };
    Ok(upload_stdout_internal(
        client,
        child,
        bucket,
        key,
        tags,
        storage_class,
        callback,
        buf_size,
    )
    .await?)
}
