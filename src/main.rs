use chrono::Utc;
use clap::Parser;
use std::env;
use std::sync::Arc;
use tokio::fs::read_to_string;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use zfs2s3::config::Config;
use zfs2s3::{SnapshotType, ensure_snapshots_for_volumes};

#[derive(Parser)]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(version = concat!("v", env!("CARGO_PKG_VERSION"), "+", env!("GIT_SHA")))]
struct Args {
    /// Run a single-shot backup (Full or Incremental)
    #[arg(long)]
    single_shot: Option<SnapshotType>,

    /// Configuration file path
    #[arg(long, short = 'c', default_value = "config.toml")]
    config: String,

    /// S3 key ID
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    s3_key_id: String,

    /// S3 secret key
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    s3_secret_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    // Application arguments
    let args = Args::parse();

    // Load configuration
    let file = read_to_string(args.config).await?;
    let config = Config::try_from(&file)?;

    // Get S3 client
    let s3_client = zfs2s3::s3::S3Client::new(
        &config.s3.url,
        &config.s3.region,
        &config.s3.bucket,
        &args.s3_key_id,
        &args.s3_secret_key,
    )?;

    // single-shot mode?
    if let Some(mode) = args.single_shot {
        // Get volumes and their snapshots to back up
        let mut volumes_to_backup = zfs2s3::zfs::VolumeSnapshotMap::new()
            .await?
            .keep_volume_to_backup(&config);

        if mode == SnapshotType::Incremental {
            ensure_snapshots_for_volumes(&volumes_to_backup).await?;
        }

        zfs2s3::snapshot_volumes(&volumes_to_backup, &mode).await?;
        volumes_to_backup.refresh().await?;

        if let Err(e) = zfs2s3::sync_snapshots(&s3_client, &volumes_to_backup).await {
            log::error!("Failed to sync snapshots to S3: {e}");
        }

        return Ok(());
    }

    // Schedules
    let config = Arc::new(config);
    let s3_client = Arc::new(s3_client);
    let cancel_token = CancellationToken::new();
    let mut handles: Vec<JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> =
        Vec::new();

    // Spawn signal handler
    let signal_cancel_token = cancel_token.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal_cancel_token.cancel();
    });

    // Operation lock to prevent concurrent backups and cleanups
    let op_lock = Arc::new(tokio::sync::Mutex::new(()));

    // Backup task
    let handle_full_backups = tokio::task::spawn(run_scheduled_backups(
        Arc::clone(&config),
        Arc::clone(&s3_client),
        cancel_token.clone(),
        Arc::clone(&op_lock),
    ));
    handles.push(handle_full_backups);

    // Perform scheduled cleanup
    let handle_cleanup = tokio::task::spawn({
        run_cleanup(
            Arc::clone(&config),
            Arc::clone(&s3_client),
            cancel_token.clone(),
            Arc::clone(&op_lock),
        )
    });
    handles.push(handle_cleanup);

    // Wait for all handles to complete
    for handle in handles {
        handle.await??;
    }

    Ok(())
}

async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");

    select! {
        _ = sigterm.recv() => {
            log::info!("Received SIGTERM")
        },
        _ = sigint.recv() => {
            log::info!("Received SIGINT")
        },
    }
}

async fn run_scheduled_backups(
    config: Arc<Config>,
    s3_client: Arc<zfs2s3::s3::S3Client>,
    cancel_token: CancellationToken,
    op_lock: Arc<tokio::sync::Mutex<()>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Run both Full and Incremental schedules in the same task to avoid having
    // both schedules trigger backups at the same time.
    let schedule = config.backup.schedule()?;
    let incremental = config.backup.incremental()?;

    while !cancel_token.is_cancelled() {
        let now = Utc::now();
        let schedule_next = schedule
            .after(&now)
            .next()
            .ok_or("No upcoming backup from schedule")?;

        let incremental_next = incremental
            .after(&now)
            .next()
            .ok_or("No upcoming backup from schedule")?;

        let schedule_duration = (schedule_next - now).to_std()?;
        let incremental_duration = (incremental_next - now).to_std()?;

        let (duration, snapshot_type) = if incremental_duration < schedule_duration {
            (incremental_duration, SnapshotType::Incremental)
        } else {
            (schedule_duration, SnapshotType::Full)
        };

        select! {
            _ = sleep(duration) => {}
            _ = cancel_token.cancelled() => {
                break;
            }
        }

        // Acquire operation lock
        let _lock = op_lock.lock().await;

        // Get volumes to back up
        let mut volumes = zfs2s3::zfs::VolumeSnapshotMap::new()
            .await?
            .keep_volume_to_backup(&config);

        if snapshot_type == SnapshotType::Incremental {
            // Ensure there is at least one snapshot for each volume to back up
            // before performing incremental backup
            if let Err(e) = ensure_snapshots_for_volumes(&volumes).await {
                log::error!("Failed to ensure snapshots for incremental backup: {e}");
                continue;
            }
        }

        // Perform backup
        if let Err(e) = zfs2s3::snapshot_volumes(&volumes, &snapshot_type).await {
            log::error!("Failed to snapshot volumes: {e}");
            continue;
        }
        if let Err(e) = volumes.refresh().await {
            log::error!("Failed to refresh volume snapshots: {e}");
            continue;
        }

        // Sync local snapshots to S3. This step is to remediate issues from
        // missed uploads.
        if let Err(e) = zfs2s3::sync_snapshots(&s3_client, &volumes).await {
            log::error!("Failed to sync snapshots to S3: {e}");
        }
    }

    Ok(())
}

async fn run_cleanup(
    config: Arc<Config>,
    s3_client: Arc<zfs2s3::s3::S3Client>,
    cancel_token: CancellationToken,
    op_lock: Arc<tokio::sync::Mutex<()>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let schedule = config.cleanup.schedule()?;
    while !cancel_token.is_cancelled() {
        let now = Utc::now();
        let next = schedule
            .after(&now)
            .next()
            .ok_or("No upcoming cleanup from schedule")?;
        let duration = (next - now).to_std()?;

        select! {
            _ = sleep(duration) => {}
            _ = cancel_token.cancelled() => {
                break;
            }
        }

        // Acquire operation lock
        let _lock = op_lock.lock().await;

        // Get volumes to back up
        let mut volumes = zfs2s3::zfs::VolumeSnapshotMap::new()
            .await?
            .keep_volume_to_backup(&config);

        if let Err(e) = volumes.refresh().await {
            log::error!("Failed to refresh volume snapshots: {e}");
            continue;
        }

        if let Err(e) = volumes.apply_retention_policy(&config).await {
            log::error!("Failed to apply retention policy: {e}");
            continue;
        }

        if let Err(e) = zfs2s3::sync_snapshots(&s3_client, &volumes).await {
            log::error!("Failed to delete snapshots from S3: {e}");
            continue;
        }
    }

    Ok(())
}
