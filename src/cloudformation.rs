use std::{error::Error, fs, path::Path};

use log::debug;

use crate::config::{ZfsBackupConfig, ZfsBaseConfig};

fn create_for_bucket(config_entry: &ZfsBackupConfig) -> String {
    let template = "  $RESOURCE:
    Type: 'AWS::S3::Bucket'
    Properties:
      BucketName: '$BUCKET'
      AccessControl: Private
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true
      LifecycleConfiguration:
        Rules:
          - Id: DeleteFull
            Prefix: 'full/'
            Status: Enabled
            ExpirationInDays: $EXPIRE_IN_DAYS_FULL
          - Id: DeleteIncremental
            Prefix: 'incremental/'
            Status: Enabled
            ExpirationInDays: $EXPIRE_IN_DAYS_INC
          - Id: AbortIncompleteMultipartUpload
            Status: Enabled
            AbortIncompleteMultipartUpload:
              DaysAfterInitiation: 7
"
    .to_string();
    //@fixme : we currently don't support automatically moving to a different storage tier.
    let resource_name =
        titlecase::titlecase(&config_entry.bucket.replace("-", " ")).replace(" ", "");
    let template = template.replace("$BUCKET", &config_entry.bucket);
    let template = template.replace("$RESOURCE", &resource_name);
    let template = template.replace(
        "$EXPIRE_IN_DAYS_FULL",
        &config_entry.full.expire_in_days.to_string(),
    );
    let template = template.replace(
        "$EXPIRE_IN_DAYS_INC",
        &config_entry.incremental.expire_in_days.to_string(),
    );
    template
}

pub fn generate_cloudformation(config: &ZfsBaseConfig) -> Result<(), Box<dyn Error>> {
    if Path::new("cloudformation_zfsbackup.yaml").exists() {
        panic!("Cowardly not creating cloudformation_zfsbackup.yaml, as the file already exists");
    }
    let mut cloudformation = "AWSTemplateFormatVersion: '2010-09-09'
Description: ZFS backup config
Resources:
"
    .to_string();
    for config in &config.configs {
        cloudformation.push_str(&create_for_bucket(&config));
    }
    cloudformation.push_str(
        "  CustomUser:
    Type: 'AWS::IAM::User'
    Properties:
      UserName: 'BackupAccount'
      Policies:
        - PolicyName: 'CustomRole'
          PolicyDocument:
            Statement:
              - Effect: Allow
                Action:
                  - s3:PutObject
                  - s3:GetObjectTagging
                  - s3:PutObjectTagging
                  - s3:ListBucket
                  - s3:AbortMultipartUpload
                  - s3:ListMultipartUploadParts
                Resource:
",
    );
    for config in &config.configs {
      cloudformation.push_str(&format!(
        "                  - !Join ['', ['arn:aws:s3:::', '{}' ]]\n",
        &config.bucket
    ));
        cloudformation.push_str(&format!(
            "                  - !Join ['', ['arn:aws:s3:::', '{}/*' ]]\n",
            &config.bucket
        ));
    }
    debug!("Writing cloudformation file...");
    fs::write("cloudformation_zfsbackup.yaml", cloudformation)?;
    println!("cloudformation_zfsbackup.yaml written");
    Ok(())
}
