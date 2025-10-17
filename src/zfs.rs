//* A simple wrapper around ZFS commands
use crate::BACKUP_SUFFIX_INCREMENTAL;
use crate::config::Config;
use chrono::{DateTime, Utc};
use fast_glob::glob_match;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use tokio::process::Command;

pub const SUFFIX_SEPARATOR: &str = "@";

/// A mapping from volume names to their snapshots.
/// Snapshots are sorted by creation time in descending order (latest first).
#[derive(Debug)]
pub struct VolumeSnapshotMap {
    pub volumes: HashMap<String, Vec<Snapshot>>,
}

impl VolumeSnapshotMap {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let snapshots = list_snapshots().await?;
        let mut volumes: HashMap<String, Vec<Snapshot>> = list_volumes()
            .await?
            .iter()
            .map(|v| (v.clone(), Vec::new()))
            .collect();

        volumes.iter_mut().for_each(|(k, v)| {
            *v = Self::map_snapshot_to_volume(k.as_str(), &snapshots);
        });
        Ok(VolumeSnapshotMap { volumes })
    }

    pub fn volumes(&self) -> HashSet<String> {
        self.volumes.keys().cloned().collect()
    }

    pub fn keep_volume_to_backup(self, config: &Config) -> Self {
        let to_backup = self
            .volumes
            .iter()
            .filter(|v| {
                config
                    .backup
                    .volumes
                    .iter()
                    .any(|pattern| glob_match(pattern, v.0))
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        VolumeSnapshotMap { volumes: to_backup }
    }

    pub async fn refresh(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snapshots = list_snapshots().await?;
        self.volumes.iter_mut().for_each(|(k, v)| {
            *v = Self::map_snapshot_to_volume(k.as_str(), &snapshots);
        });
        Ok(())
    }

    fn map_snapshot_to_volume(volume: &str, snapshots: &[Snapshot]) -> Vec<Snapshot> {
        // Filter snapshots for this volume
        let mut snaps: Vec<Snapshot> = snapshots
            .iter()
            .filter(|s| s.name.starts_with(&format!("{volume}{SUFFIX_SEPARATOR}")))
            .cloned()
            .collect();
        // Sort by creation time descending
        snaps.sort_by(|a, b| b.creation.cmp(&a.creation));
        snaps
    }

    pub async fn apply_retention_policy(
        &mut self,
        config: &Config,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (_, snapshots) in self.volumes.iter_mut() {
            // Find the index of the first full snapshot that can be considered for deletion
            if let Some((start, _)) = snapshots
                .iter()
                .enumerate()
                .filter(|(_, s)| !s.name.contains(BACKUP_SUFFIX_INCREMENTAL))
                .nth(config.cleanup.keep_min)
            {
                // There is at least `keep_min` full snapshots, filter with retention policy
                let timestamp_cutoff = config.cleanup.keep_duration()?;

                // Find the first snapshot older than the cutoff timestamp which can be deleted
                // since we only filter snapshots after the minimum kept full snapshots.
                let time_cutoff_index = snapshots[start..]
                    .iter()
                    .position(|s| s.creation < timestamp_cutoff)
                    .map(|i| start + i)
                    .unwrap_or(snapshots.len());

                // Are there incremental snapshot older than the last kept full snapshot?
                let cutoff_index = snapshots[..time_cutoff_index]
                    .iter()
                    .rposition(|s| !s.name.contains(BACKUP_SUFFIX_INCREMENTAL))
                    .map(|i| i + 1) // Keep this full snapshot and everything before it
                    .unwrap_or(time_cutoff_index);

                snapshots.truncate(cutoff_index);
            }
        }

        sync_snapshots(self).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub name: String,
    pub creation: DateTime<Utc>,
}

#[derive(Debug)]
pub enum SnapshotError {
    InvalidFormat(String),
    TimestampError(String),
    FailedToExtractKey(String),
}

impl Display for SnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::InvalidFormat(s) => write!(f, "Invalid snapshot format: {}", s),
            SnapshotError::TimestampError(s) => write!(f, "Invalid timestamp in snapshot: {}", s),
            SnapshotError::FailedToExtractKey(s) => {
                write!(f, "Failed to extract key from snapshot name: {}", s)
            }
        }
    }
}

impl std::error::Error for SnapshotError {}

impl Snapshot {
    pub fn try_from(line: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(SnapshotError::InvalidFormat(line.to_string()).into());
        }
        let name = parts[0].to_string();
        let creation_timestamp: i64 = parts[1]
            .parse()
            .map_err(|_| SnapshotError::TimestampError(parts[1].to_string()))?;
        let creation = DateTime::<Utc>::from_timestamp_secs(creation_timestamp).ok_or(
            SnapshotError::TimestampError(creation_timestamp.to_string()),
        )?;
        Ok(Snapshot { name, creation })
    }

    pub fn to_key(&self) -> Result<&str, Box<dyn std::error::Error + Send + Sync>> {
        match self.name.split('/').collect::<Vec<&str>>().last() {
            Some(key) => Ok(key),
            None => Err(SnapshotError::FailedToExtractKey(self.name.clone()).into()),
        }
    }
}

#[derive(Debug)]
pub enum ZfsError {
    CommandError(String),
    ChildError,
}

impl Display for ZfsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ZfsError::CommandError(s) => write!(f, "ZFS command error: {}", s),
            ZfsError::ChildError => write!(f, "Failed to spawn ZFS child process"),
        }
    }
}

impl std::error::Error for ZfsError {}

async fn list_volumes() -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let output = Command::new("zfs")
        .arg("list")
        .arg("-H")
        .arg("-o")
        .arg("name")
        .arg("-t")
        .arg("volume")
        .output()
        .await?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let volumes: Vec<String> = stdout.lines().map(|line| line.to_string()).collect();
        Ok(volumes)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        Err(ZfsError::CommandError(stderr).into())
    }
}

async fn list_snapshots() -> Result<Vec<Snapshot>, Box<dyn std::error::Error + Send + Sync>> {
    let output = Command::new("zfs")
        .arg("list")
        .arg("-H")
        .arg("-o")
        .arg("name,creation")
        .arg("-t")
        .arg("snapshot")
        .arg("-p")
        .output()
        .await?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let snapshots: Vec<Snapshot> = stdout
            .lines()
            .filter_map(|line| Snapshot::try_from(line).ok())
            .collect();

        Ok(snapshots)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        Err(ZfsError::CommandError(stderr).into())
    }
}

/// Take a snapshot of a ZFS dataset
/// - `name`: The name of the snapshot in the format "pool/dataset@snapshot"
pub async fn snapshot(name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let status = Command::new("zfs")
        .arg("snapshot")
        .arg(name)
        .status()
        .await?;

    if status.success() {
        Ok(())
    } else {
        Err(ZfsError::CommandError(format!("Failed to take snapshot {}", name)).into())
    }
}

/// Send a snapshot of a ZFS dataset to a stream
/// - `name`: The name of the snapshot in the format "pool/dataset@snapshot"
pub async fn stream_snapshot(
    name: &str,
) -> Result<tokio::process::ChildStdout, Box<dyn std::error::Error + Send + Sync>> {
    let mut child = Command::new("zfs")
        .arg("send")
        .arg(name)
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    child.stdout.take().ok_or(ZfsError::ChildError.into())
}

/// Send an incremental snapshot of a ZFS dataset to a stream
/// - `from`: The name of the base snapshot in the format "pool/dataset@snapshot"
/// - `to`: The name of the target snapshot in the format "pool/dataset@snapshot"
pub async fn stream_incremental_snapshot(
    from: &str,
    to: &str,
) -> Result<tokio::process::ChildStdout, Box<dyn std::error::Error + Send + Sync>> {
    let mut child = Command::new("zfs")
        .arg("send")
        .arg("-i")
        .arg(from)
        .arg(to)
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    child.stdout.take().ok_or(ZfsError::ChildError.into())
}

/// Delete a snapshot of a ZFS dataset
/// - `name`: The name of the snapshot in the format "pool/dataset@snapshot
pub async fn delete_snapshot(name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let status = Command::new("zfs")
        .arg("destroy")
        .arg(name)
        .status()
        .await?;

    if status.success() {
        Ok(())
    } else {
        Err(ZfsError::CommandError(format!("Failed to delete snapshot {}", name)).into())
    }
}

/// Sync snapshots on ZFS.
/// Remove snapshots that are not present in the provided VolumeSnapshotMap.
async fn sync_snapshots(
    volumes: &VolumeSnapshotMap,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let snapshots = list_snapshots().await?;

    for snapshot in snapshots.iter() {
        let mut found = false;
        for volume in volumes.volumes.iter() {
            if volume.1.iter().any(|s| s.name == *snapshot.name) {
                found = true;
                break;
            }
        }
        if !found {
            delete_snapshot(&snapshot.name).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test_snapshot {
    use super::*;

    #[test]
    fn try_from_valid() {
        const CREATION_TIMESTAMP: i64 = 1625078400;
        let line = format!("pool/dataset@snap1 {CREATION_TIMESTAMP}");
        let snapshot = Snapshot::try_from(line.as_str()).unwrap();
        assert_eq!(snapshot.name, "pool/dataset@snap1");
        assert_eq!(
            snapshot.creation,
            DateTime::<Utc>::from_timestamp_secs(CREATION_TIMESTAMP).unwrap()
        );
    }

    #[test]
    fn try_from_invalid_format() {
        let line = "pool/dataset@snap1 1625078400 some value";
        let result = Snapshot::try_from(line);
        assert!(result.is_err());
    }

    #[test]
    fn try_from_invalid_timestamp() {
        let line = "pool/dataset@snap1 invalid_timestamp";
        let result = Snapshot::try_from(line);
        assert!(result.is_err());
    }
}
