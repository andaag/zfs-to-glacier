use std::{io};
use std::io::{Read, Write};
use std::process::Command;
use std::process::Stdio;
use std::{error::Error, process::ExitStatus};
use zfs_to_glacier::s3_utils::{StorageClass, upload_stdout, upload_stdout_internal};
use zfs_to_glacier::cmd_execute::CommandStreamActions;
mod common;
use common::*;
use testcontainers::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_short_file() -> Result<(), Box<dyn Error>> {
    log_init("integration_s3_utils");
    execute_in_docker!((|| async {
        let bucket = generate_unique_name();
        let client = create_client(&bucket).await?;

        let child = Command::new("echo")
            .arg("-n")
            .arg("this is a test")
            .stdout(Stdio::piped())
            .spawn()?;
        upload_stdout(&client, Box::new(child), &bucket, "test_key", vec![], StorageClass::STANDARD, 0, |_| {}).await?;

        let content = common::download_file(&bucket, "test_key", &client).await?;
        assert_eq!(content, "this is a test");
        Ok(())
    }))
}

struct LargeFile {
    iterations: usize,
    fail:bool
}
const MIN_MULTIPART_SIZE: usize = 5 * 1024 * 1024;
const TEST_MULTIPART_SIZE: usize = 1 * 1024 * 1024;
const TEST_ITERATIONS: usize = 9;

struct FakeStdout {
    iterations: usize,
}

impl Read for FakeStdout {
    fn read(&mut self, mut buffer: &mut [u8]) -> Result<usize, std::io::Error> {
        if self.iterations == 0 {
            Ok(0)
        } else {
            buffer.write(b"S")?;
            let iter_str = format!("{:0>2}", self.iterations);
            buffer.write(iter_str.as_ref())?;
            for _ in 0..TEST_MULTIPART_SIZE {
                buffer.write(b"x")?;
            }
            buffer.write(b"E")?;
            buffer.write(iter_str.as_ref())?;
            buffer.write(b" ")?;
            self.iterations = self.iterations - 1;
            Ok(TEST_MULTIPART_SIZE + 7)
        }
    }
}

impl CommandStreamActions<FakeStdout> for LargeFile {
    fn stdout(&mut self) -> FakeStdout {
        FakeStdout {
            iterations: self.iterations,
        }
    }
    fn wait(&mut self) -> io::Result<ExitStatus> {
        if self.fail {
            Command::new("false").output().map(|x| x.status)
        } else {
            Command::new("true").output().map(|x| x.status)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_upload_large_file() -> Result<(), Box<dyn Error>> {
    log_init("integration_s3_utils");
    
    execute_in_docker!((|| async {
        let bucket = generate_unique_name();
        let client = create_client(&bucket).await?;
        let total_bytes = upload_stdout_internal(&client, Box::new(LargeFile { iterations:TEST_ITERATIONS, fail:false}), &bucket, "test_key", vec![], StorageClass::STANDARD, |_| {}, MIN_MULTIPART_SIZE).await?;

        let content = common::download_file(&bucket, "test_key", &client).await?;
        let content = content.replace(&(0..TEST_MULTIPART_SIZE).map(|_| "x").collect::<String>(), "x");
        assert_eq!(content, "S09xE09 S08xE08 S07xE07 S06xE06 S05xE05 S04xE04 S03xE03 S02xE02 S01xE01 ");
        assert_eq!(total_bytes, ((1 * 1024 * 1024) + 7) * 9);
       
        Ok(())
    }))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_very_upload_large_file() -> Result<(), Box<dyn Error>> {
    log_init("integration_s3_utils");
    
    execute_in_docker!((|| async {
        let bucket = generate_unique_name();
        let client = create_client(&bucket).await?;
        let total_bytes = upload_stdout_internal(&client, Box::new(LargeFile { iterations:30, fail:false }), &bucket, "test_key", vec![], StorageClass::STANDARD, |_| {}, MIN_MULTIPART_SIZE).await?;

        let content = common::download_file(&bucket, "test_key", &client).await?;
        let content = content.replace(&(0..TEST_MULTIPART_SIZE).map(|_| "x").collect::<String>(), "x");
        assert_eq!(content, "S30xE30 S29xE29 S28xE28 S27xE27 S26xE26 S25xE25 S24xE24 S23xE23 S22xE22 S21xE21 S20xE20 S19xE19 S18xE18 S17xE17 S16xE16 S15xE15 S14xE14 S13xE13 S12xE12 S11xE11 S10xE10 S09xE09 S08xE08 S07xE07 S06xE06 S05xE05 S04xE04 S03xE03 S02xE02 S01xE01 ");
        assert_eq!(total_bytes, ((1 * 1024 * 1024) + 7) * 30);
        Ok(())
    }))
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_command_exit_failure() -> Result<(), Box<dyn Error>> {
    log_init("integration_s3_utils");
    
    execute_in_docker!((|| async {
        let bucket = generate_unique_name();
        let client = create_client(&bucket).await?;
        let r = upload_stdout_internal(&client, Box::new(LargeFile { iterations:TEST_ITERATIONS, fail:true}), &bucket, "test_key", vec![], StorageClass::STANDARD, |_| {}, MIN_MULTIPART_SIZE).await;    
        assert_eq!(r.is_err(), true);
        Ok(())
    }))
}
