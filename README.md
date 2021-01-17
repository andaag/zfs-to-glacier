# What

A little rust tool to do S3 backups of my sanoid zfs snapshots. (Although any snapshot system should work)

## Why

S3 snapshots are cheap... Especially if you are (like in my case) taking a backup of a computer which is acting as a backup of something else and can take advantage of S3 deep archive. (NB : this means recovery time can increase, but price is very considerably decreased). I also quite like binary dumps in a dumb blob file storage. Backing up with something like rsync.net is more elegant, I would argue this is less complex.

[AWS S3 recovery times](https://aws.amazon.com/glacier/). Deep glacier recovery is 12-48 hours. I know from experience that if I loose the backup of my backups 12-48hours to recover my data is the least of my problems.

## How

1. zfs_to_glacier generateconfig to get a sample config.yaml
2. modify the example config.yaml
3. zfs_to_glacier generatecloudformation
4. inspect cloudformation file and upload to AWS. (Cloudformation -> Create -> new resource -> upload file). Name is freetext and no other parameters are needed.
5. Click on resources -> BackupAccount -> Security Credentials -> Create access key.
6. Set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
7. Set environment variable AWS_REGION to whatever region you uploaded the file from. (If you run the command under you'll also see the region in the endpoint url). For example export AWS_REGION="eu-west-3"
8. Run zfs_to_glacier sync. You can run zfs_to_glacier sync -v -n to see what it would snapshot in incremental mode and in full mode.

**zfs_to_glacier keep encrypted data encrypted, read warnings!**

## Warnings

1. zfs_to_glacier will keep your backups encrypted. They are sent with zfs send -w. This means if you do not have a backup of your backup key (if you use a key instead of a passphrase) you will *not* be able to recover your data from S3.
2. zfs_to_glacier uses S3's expiry, which means if you stop running this tool the automatic expiry of old data will keep going. This will eventually clear out your backups. I recommend using healthchecks.io or something like it to ensure that your backups keep going.

## How reliable is this

When sending files this will confirm both md5 checksums of each individual part of a file sent, and confirm that the zfs command exits with status 0. I don't *think* it's possible to send corrupted data this way. That said, if the zfs command exits with status code 0 and does not produce the required output of course this app would happily upload a corrupted snapshot.

I would recommend taking great care when dealing with something as critical as backups, I rely on this tool personally, but it comes with zero guarantees.

### Adding new pools

1. If you need to add a new pool, regenrate your cloudformation template and instead of uploading a new cloudformation template. Select the existing one and update it with the new file. NB : If you delete a pool, you need to clean out the bucket before, cloudformation will not delete a bucket that isn't empty.

### Limits

1. zfs_to_glacier intentionally sends each zfs snapshot as a single file, this means we are limited by the 5tb max file size in S3. If you need snapshots larger than 5tb this tool will not work.

### My backup script for reference

```sh
export CHECK_URL="https://hc-ping.com/<URL_FROM_HEALTHCHECKS.IO>"
export AWS_SECRET_ACCESS_KEY="<AWS_SECRET_KEY>"
export AWS_ACCESS_KEY_ID="<AWS_ACCESS_KEY>"
export AWS_REGION="<AWS_REGION, for example eu-west-3>"
url=$CHECK_URL
curl -fsS --retry 3 -X GET $url/start
nice zfs_to_glacier sync -v &> backup.log
if [ $? -ne 0 ]; then
    url=$url/fail
    curl -fsS --retry 3 -X POST --data-raw "$(tail -n 20 backup.log)" $url
    exit 1
fi
curl -fsS --retry 3 -X POST --data-raw "$(tail -n 20 backup.log)" $url
```
