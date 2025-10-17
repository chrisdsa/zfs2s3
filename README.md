# ZFS to S3

This repository contains scripts and tools to back up ZFS datasets to S3
compatible storage. It leverages ZFS snapshots and incremental sends to
efficiently transfer data to the cloud.

## Requirements

- ZFS installed on the source system

## How to use

## Create the configuration file

```toml
[backup]
schedule = " */10 * * * * * *"
incremental = "*/2 * * * * * *"
volumes = ["zfs2s3pool/vm-*", "zfs2s3pool/ct-*"]

[cleanup]
schedule = "*/10 * * * * * *"
keep_min = 3
keep_duration = "30 sec"

[s3]
bucket = "backup"
url = "http://localhost:3900"
region = "garage"
```

Cron expression format:

```text
      sec  min   hour   day of month   month   day of week   year  
E.g., "0   30   9,12,15     1,15       May-Aug  Mon,Wed,Fri  2018/2"  
```

Supported specification: https://docs.oracle.com/cd/E12058_01/doc/doc.1014/e12030/cron_expressions.htm

## Run the application

See the help message for available commands and options:

```bash
zfs2s3 --help
```

Run a single time:

```bash
zfs2s3 --config /path/to/config.toml --single-shot full
```

Run continuously:

```bash
zfs2s3 --config /path/to/config.toml
```
