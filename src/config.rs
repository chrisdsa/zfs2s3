use chrono::{DateTime, Utc};
use cron::Schedule;
use humantime;
use serde::Deserialize;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Clone)]
pub enum ConfigError {
    InvalidCronExpression(String),
    InvalidDuration(String),
    InvalidToml(String),
}

impl std::error::Error for ConfigError {}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidCronExpression(expr) => {
                const EXPRESSION_INFO: &str = r#"Cron expression format:
      sec  min   hour   day of month   month   day of week   year
E.g., "0   30   9,12,15     1,15       May-Aug  Mon,Wed,Fri  2018/2"
Supported specification: https://docs.oracle.com/cd/E12058_01/doc/doc.1014/e12030/cron_expressions.htm
"#;
                write!(f, "Invalid cron expression: {expr}\n\n{EXPRESSION_INFO}")
            }
            ConfigError::InvalidDuration(e) => {
                write!(f, "Invalid duration: {}", e)
            }
            ConfigError::InvalidToml(e) => {
                write!(f, "Invalid TOML configuration: {}", e)
            }
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub backup: BackupPolicy,
    #[serde(default)]
    pub cleanup: CleanupPolicy,
    pub s3: S3,
}

impl Config {
    pub fn try_from(toml: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config: Config = toml::from_str(toml)?;
        config.validate()?;
        Ok(config)
    }
    /// Validate the configuration. Returns Ok(()) if valid, or ConfigError if invalid.
    fn validate(&self) -> Result<(), ConfigError> {
        // Validate expressions
        self.backup.schedule()?;
        self.backup.incremental()?;
        self.cleanup.schedule()?;
        self.cleanup.keep_duration()?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct BackupPolicy {
    /// When to take snapshots (cron expression).
    /// Create an incremental backup unless the threshold for full backup is met.
    ///       sec  min   hour   day of month   month   day of week   year
    /// E.g., "*    *     0      15             *           *          *"
    ///       15th of every month at midnight UTC.
    schedule: String,
    /// When to take incremental snapshots (cron expression).
    ///       sec  min   hour   day of month   month   day of week   year
    /// E.g., "*    *      * 15 * * *" for monthly on the 15th at midnight UTC.
    #[serde(default)]
    incremental: String,
    /// List of glob pattern to specify volumes
    #[serde(default)]
    pub volumes: Vec<String>,
}

impl BackupPolicy {
    pub fn schedule(&self) -> Result<Schedule, ConfigError> {
        let expression = self.schedule.as_str();
        to_cron(expression)
    }

    pub fn incremental(&self) -> Result<Schedule, ConfigError> {
        let expression = self.incremental.as_str();
        to_cron(expression)
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct CleanupPolicy {
    /// When to run cleanup (cron expression)
    schedule: String,
    /// Keep at least this many full snapshots
    pub keep_min: usize,
    /// Keep full snapshots for this duration
    /// When a full snapshot is deleted, all incremental snapshots
    /// older than the full snapshot are also deleted.
    /// E.g. "90d" for 90 days, "12w" for 12 weeks, "18m" for 18 months
    keep_duration: String,
    /// Snapshots to exclude from cleanup based on glob patterns
    #[serde(default)]
    pub exclude: Vec<String>,
}

impl CleanupPolicy {
    pub fn schedule(&self) -> Result<Schedule, ConfigError> {
        let expression = self.schedule.as_str();
        to_cron(expression)
    }

    pub fn keep_duration(&self) -> Result<DateTime<Utc>, ConfigError> {
        let duration = humantime::parse_duration(&self.keep_duration)
            .map_err(|e| ConfigError::InvalidDuration(e.to_string()))?;

        let date_time = Utc::now()
            - chrono::Duration::from_std(duration)
                .map_err(|e| ConfigError::InvalidDuration(e.to_string()))?;
        Ok(date_time)
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct S3 {
    /// S3 bucket name
    pub bucket: String,
    /// S3 url
    pub url: String,
    /// S3 region
    pub region: String,
    // Access key ID and secret access key are provided via environment
    // variables and or command line args.
}

fn to_cron(expression: &str) -> Result<Schedule, ConfigError> {
    Schedule::try_from(expression)
        .map_err(|_| ConfigError::InvalidCronExpression(expression.to_string()))
}

#[cfg(test)]
mod test_config {
    use super::*;

    #[test]
    fn invalid_config_wrong_cron() {
        const CONFIG: &str = r#"
[backup]
schedule = "* * 0 "
incremental = "0 4 * 14 * * *"
volumes = ["zfs2s3/vm-*", "zfs2s3/ct-*"]

[cleanup]
schedule = "* * 5 * *"
keep_min = 3
keep_duration = "90d"

[s3]
bucket = "my-bucket"
url = "http://localhost:3900"
"#;
        let config = Config::try_from(CONFIG);

        assert!(config.is_err());
    }

    #[test]
    fn invalid_config_wrong_duration() {
        const CONFIG: &str = r#"
[backup]
schedule = "0 0 0 15 * * *"
incremental = "0 4 * 14 * * *"
volumes = ["zfs2s3/vm-*", "zfs2s3/ct-*"]

[cleanup]
schedule = "0 0 5 * * * *"
keep_min = 3
keep_duration = "90x"

[s3]
bucket = "my-bucket"
url = "http://localhost:3900"
"#;
        let config = Config::try_from(CONFIG);
        assert!(config.is_err());
    }

    #[test]
    fn invalid_config_field_missing() {
        const CONFIG: &str = r#"
[backup]
schedule = "0 0 0 15 * * *"
incremental = "0 4 * 14 * * *"
volumes = ["zfs2s3/vm-*", "zfs2s3/ct-*"]

[cleanup]
schedule = "0 0 5 * * * *"
keep_duration = "90d"

[s3]
bucket = "my-bucket"
url = "http://localhost:3900"
"#;
        let config = Config::try_from(CONFIG);
        assert!(config.is_err());
    }

    #[test]
    fn valid_config() {
        const CONFIG: &str = r#"
[backup]
schedule = " 0 0 5 * * Sun *"
incremental = "0 30 4 * * Mon-Sat *"
volumes = ["zfs2s3/vm-*", "zfs2s3/ct-*"]

[cleanup]
schedule = "0 0 5 * * * *"
keep_min = 3
keep_duration = "3 months"

[s3]
bucket = "my-bucket"
url = "http://localhost:3900"
region = "garage"
"#;

        let config = Config::try_from(CONFIG);
        assert!(config.is_ok());
    }
}
