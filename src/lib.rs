pub mod config;
pub mod s3;
pub mod zfs;

use crate::s3::S3Client;
use crate::zfs::{SUFFIX_SEPARATOR, Snapshot, VolumeSnapshotMap, ZfsError};
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use std::collections::HashSet;
use std::fmt::Display;

// Backup conventions:
// snapshot suffix: @auto-backup-2025-10-17T04:06:55Z
// snapshot suffix for incremental backup: @auto-backup-incremental-2025-10-17T04:06:55Z
const BACKUP_SUFFIX: &str = "auto-backup-";
const BACKUP_SUFFIX_INCREMENTAL: &str = "auto-backup-incremental-";
const TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%SZ";

#[derive(Debug)]
pub enum Zfs2S3Error {
    ZfsError(ZfsError),
    UploadError(String),
    SnapshotFailures(Vec<Box<dyn std::error::Error + Send + Sync>>),
    UploadFailures(Vec<Box<dyn std::error::Error + Send + Sync>>),
}

impl Display for Zfs2S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Zfs2S3Error::ZfsError(e) => write!(f, "ZFS Error: {}", e),
            Zfs2S3Error::UploadError(msg) => write!(f, "Upload Error: {}", msg),
            Zfs2S3Error::SnapshotFailures(errors) => {
                writeln!(f, "Snapshot Failures:")?;
                for err in errors {
                    writeln!(f, "- {}", err)?;
                }
                Ok(())
            }
            Zfs2S3Error::UploadFailures(errors) => {
                writeln!(f, "Upload Failures:")?;
                for err in errors {
                    writeln!(f, "- {}", err)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for Zfs2S3Error {}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum SnapshotType {
    Full,
    Incremental,
}

impl Display for SnapshotType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotType::Full => write!(f, "full"),
            SnapshotType::Incremental => write!(f, "incremental"),
        }
    }
}

pub async fn snapshot_volumes(
    volumes: &VolumeSnapshotMap,
    snapshot_type: &SnapshotType,
) -> Result<(), Zfs2S3Error> {
    let timestamp = format_iso_8601(&Utc::now());
    let mut errors: Vec<Box<dyn std::error::Error + Send + Sync>> = Vec::new();

    let suffix = match snapshot_type {
        SnapshotType::Full => BACKUP_SUFFIX,
        SnapshotType::Incremental => BACKUP_SUFFIX_INCREMENTAL,
    };

    for volume in volumes.volumes() {
        let name = format!("{volume}{SUFFIX_SEPARATOR}{suffix}{timestamp}");
        if let Err(e) = zfs::snapshot(&name).await {
            errors.push(e);
        }
    }

    if !errors.is_empty() {
        return Err(Zfs2S3Error::SnapshotFailures(errors));
    }
    Ok(())
}

/// Upload the latest snapshot of a single volume to S3
async fn upload_single_full_snapshot_to_s3(
    s3: &S3Client,
    volume: (&str, &[Snapshot]),
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Verify the latest snapshot exists
    let latest_snapshot = if let Some(snapshot) = volume.1.first() {
        snapshot
    } else {
        return Err(Zfs2S3Error::UploadError(format!(
            "No snapshots found for volume: {}",
            volume.0
        ))
        .into());
    };

    // Verify that the newest snapshot is a full snapshot
    if latest_snapshot.name.contains(BACKUP_SUFFIX_INCREMENTAL) {
        return Err(Zfs2S3Error::UploadError(format!(
            "Trying to upload a snapshot that was created as an incremental backup: {}",
            latest_snapshot.name
        ))
        .into());
    }

    // Compute key for S3 object. Use the snapshot name without the pool prefix.
    let key = latest_snapshot.to_key()?;

    // Upload the snapshot to S3
    let snapshot = zfs::stream_snapshot(&latest_snapshot.name).await?;
    log::info!("Uploading snapshot {key}");
    s3.upload_stream(snapshot, key).await?;

    Ok(())
}

/// Upload the latest incremental snapshot of a single volume to S3
async fn upload_single_incremental_snapshot_to_s3(
    s3: &S3Client,
    volume: (&str, &[Snapshot]),
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Grab the two newest snapshots
    let (to, from) = match volume.1.get(0..2) {
        Some(snapshots) if snapshots.len() == 2 => (&snapshots[0], &snapshots[1]),
        _ => {
            return Err(Zfs2S3Error::UploadError(format!(
                "Not enough snapshots to perform incremental upload for volume: {}",
                volume.0
            ))
            .into());
        }
    };

    // Verify that the newest snapshot is an incremental snapshot
    if !to.name.contains(BACKUP_SUFFIX_INCREMENTAL) {
        return Err(Zfs2S3Error::UploadError(format!(
            "Trying to upload a snapshot that was not created as an incremental backup: {}",
            to.name
        ))
        .into());
    }

    // Compute key for S3 object. Use the snapshot name without the pool prefix.
    let key = to.to_key()?;

    // Upload the snapshot to S3
    let snapshot = zfs::stream_incremental_snapshot(&from.name, &to.name).await?;
    log::info!("Uploading snapshot {key}");
    s3.upload_stream(snapshot, key).await?;

    Ok(())
}

/// Sync local snapshots to S3 by uploading missing snapshots and deleting removed snapshots
pub async fn sync_snapshots(
    s3: &S3Client,
    volumes: &VolumeSnapshotMap,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sync_missing_snapshots(s3, volumes).await?;
    sync_deleted_snapshots(s3, volumes).await?;
    Ok(())
}

/// Sync local snapshots to S3 by uploading missing snapshots
async fn sync_missing_snapshots(
    s3: &S3Client,
    volumes: &VolumeSnapshotMap,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let s3_objects = s3.list_objects().await?;

    for volume in volumes.volumes.iter() {
        // Reminder: snapshots are sorted from newest to oldest
        for (i, snapshot) in volume.1.iter().enumerate() {
            // Snapshot names in S3 are stored without the pool prefix
            let key = snapshot.to_key()?;
            if !s3_objects.contains(&key.to_string()) {
                // Create a slice from the current snapshot onward
                // This is because the upload functions only upload the latest snapshot (full or incremental)
                let snapshots = volume.1[i..].as_ref();
                let volume = (volume.0.as_str(), snapshots);

                // Upload the snapshot
                if is_incremental_snapshot(&snapshot.name) {
                    upload_single_incremental_snapshot_to_s3(s3, volume).await?;
                } else {
                    upload_single_full_snapshot_to_s3(s3, volume).await?;
                }
            }
        }
    }

    Ok(())
}

/// Sync deleted snapshots from S3 by removing snapshots that no longer exist locally
/// This function checks for each snapshot in S3 if it exists in the local volumes.
/// If a snapshot is missing locally, it deletes that snapshot from S3.
async fn sync_deleted_snapshots(
    s3: &S3Client,
    volumes: &VolumeSnapshotMap,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let s3_objects = s3.list_objects().await?;

    let local_snapshot_names: HashSet<&str> = volumes
        .volumes
        .iter()
        .flat_map(|volume| {
            volume
                .1
                .iter()
                .map(|s| s.to_key().unwrap_or(s.name.as_str()))
        })
        .collect();

    for object in s3_objects.iter() {
        // Snapshot names in S3 are stored without the pool prefix
        if !local_snapshot_names.iter().any(|s| s.eq(object)) {
            log::info!("Deleting {object} from S3.");
            s3.delete_object(object).await?;
        }
    }

    Ok(())
}

fn is_incremental_snapshot(snapshot_name: &str) -> bool {
    snapshot_name.contains(BACKUP_SUFFIX_INCREMENTAL)
}

fn format_iso_8601(t: &DateTime<Utc>) -> String {
    t.format(TIMESTAMP_FORMAT).to_string()
}

// fn parse_iso_8601(timestamp_str: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
//     NaiveDateTime::parse_from_str(timestamp_str, TIMESTAMP_FORMAT).map(|ndt| ndt.and_utc())
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_timestamp_parsing() {
//         // We work with seconds precision
//         let now = DateTime::<Utc>::from_timestamp_secs(Utc::now().timestamp()).unwrap();
//
//         let test_timestamp = format_iso_8601(&now);
//         let parsed_timestamp = parse_iso_8601(&test_timestamp);
//
//         assert_eq!(parsed_timestamp, Ok(now));
//     }
// }
