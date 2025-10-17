
# Create the ZFS pool
ZFS_POOL_NAME=${ZFS_POOL_NAME:-"zfspool"}
zpool create "${ZFS_POOL_NAME}" "${VIRTUAL_DISK}"

# Create a block device volume (zvol) for testing
ZFS_VOLUME_NAME=${ZFS_VOLUME_NAME:-"zfsvol"}
zfs create -V 512M "${ZFS_POOL_NAME}/${ZFS_VOLUME_NAME}"

# Create a new snapshot
zfs snapshot "${ZFS_POOL_NAME}/myvolume@snapshot1"


# Destroy the ZFS pool and clean up
zpool destroy "${ZFS_POOL_NAME}"
rm -f "${VIRTUAL_DISK}"

# Create backup file
zfs send  zfs2s3pool/vm-1000-disk@1 > /tmp/zfs2s3/backup1
zfs send -i zfs2s3pool/vm-1000-disk@1 zfs2s3pool/vm-1000-disk@3 > /tmp/zfs2s3/backup1-3

# Restore backup
zfs receive zfs2s3pool/restore-disk < /tmp/zfs2s3/backup1
zfs receive zfs2s3pool/restore-disk < /tmp/zfs2s3/backup1-3

## Note:
When restoring a snapshot, the snapshot is restored using the volume name. For example, we have a snapshot
`zfs2s3pool/vm-disk-1001@auto-backup-2025-10-31T15:48:25Z` on S3 and the local volume doesn't exist anymore.
We restore it using:
`zfs receive zfs2s3pool/vm-disk-1001 < /tmp/zfs2s3/backup-file`

The snapshot will appear in zfs list -t snapshot as:
```shell
 zfs list
NAME                      USED  AVAIL  REFER  MOUNTPOINT
zfs2s3pool               10.2M   822M    24K  /zfs2s3pool
zfs2s3pool/vm-disk-1001  10.1M   822M  10.1M  -

zfs list -t snapshot
NAME                                                       USED  AVAIL  REFER  MOUNTPOINT
zfs2s3pool/vm-disk-1001@auto-backup-2025-10-31T15:48:25Z     0B      -  10.1M  -
```

# Checksum
zfs send pool/dataset@snapshot-name | sha256sum

# List all snapshots in a pool
zfs list -t snapshot