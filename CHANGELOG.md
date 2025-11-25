# Changelog

## [0.2.2] - 2025-11-25
### Fixed
- Increased s3 part chunk size to be able to upload large files. Was limited to around 80GB before.

## [0.2.1] - 2025-11-25

### Fixed
- New volumes were not being backed up unless the application was restarted

## [0.2.0] - 2025-11-14

### Added
- Exclusion patterns to avoid deleting some snapshots during cleanup

### Changed
- Minor improvements to sync_snapshots, now using `flat_map`.

## [0.1.0] - 2025-11-07
### Added
- Initial release of zfs2s3 backup application
- ZFS snapshot handling
- Backup ZFS volumes to S3-compatible object storage
- Basic configuration via config file
- S3 integration for upload and deletion of backups
- Command-line interface
- Workflow to create release
