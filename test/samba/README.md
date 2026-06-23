# Samba on FUSE integration test

Exports a SeaweedFS FUSE mount over SMB with Samba's `smbd` and drives it with
`smbclient`, verifying that SMB file operations work correctly on top of the
mount and that data stays consistent across both protocols.

## What it checks

The functional battery in `smb_tests.sh` covers:

- connecting to the share and listing the root
- 1 MiB upload/download round-trip with content verification
- subdirectory creation and writes into it
- file rename
- 64 MiB upload/download (exercises SeaweedFS chunk splitting)
- recursive upload of a directory tree
- cross-protocol consistency: files written over SMB appear on the FUSE mount
  with identical content, and files written directly on the FUSE mount are
  readable over SMB
- deleting files and directory trees

The locking / concurrency battery in `lock_tests.sh` covers the harder cases a
network-filesystem backend has to get right:

- **POSIX `fcntl` byte-range locking** on the FUSE mount: a held exclusive lock
  denies a conflicting lock, allows a non-overlapping range, and is reacquirable
  after release (exercises the mount's `SetLk`/`GetLk`)
- **Distributed locking** (`-dlm`): a file held open for writing on one mount
  blocks a writer on a second mount until it is released
- **Distributed-lock integrity**: concurrent writers to the same file from two
  mounts leave exactly one intact payload, never a torn mix
- **Concurrency**: parallel writers to distinct files all succeed

Both FUSE mounts are started with `-dlm` (distributed lock manager). The second
mount (`/mnt/seaweedfs2`) exists only to contend with the smbd-backed mount in
the distributed-locking tests; both see the same filer path, so `.../share` is
the same data on each.

> Note on DLM semantics: `-dlm` coordinates *write access* (one mount writes a
> file at a time) and guarantees writes are not torn. It does not guarantee
> which concurrent writer wins or instant cross-mount read convergence — the
> holder's buffered data is flushed on close, asynchronously to lock release.
> When a holder closes the file, a writer on another mount acquires the freed
> lock within ~1s and completes.

## Layout

| File | Purpose |
| --- | --- |
| `smb_tests.sh` | SMB functional battery. Shared by both runners. |
| `lock_tests.sh` | SMB locking / concurrency battery. Shared by both runners. |
| `smb.conf.template` | Samba config; placeholders are filled in at run time. |
| `run.sh` | Local runner: `weed mini` + two `-dlm` mounts + `smbd` + both batteries, all as the current user on unprivileged ports. |
| `entrypoint.sh` | Container entrypoint: starts two `-dlm` FUSE mounts and runs `smbd`. |
| `run_inside_container.sh` | Runs both batteries inside the container against the local `smbd`. |
| `Dockerfile` | Adds Samba to the `chrislusf/seaweedfs:e2e` image. |
| `docker-compose.yml` | master + volume + filer + samba services. |

## Running locally

Requirements: `weed` on `$PATH`, `fusermount3`, and Samba's `smbd` /
`smbclient` / `smbpasswd` (Debian/Ubuntu: `apt-get install samba smbclient`).

```sh
test/samba/run.sh
```

No `sudo` is needed: `smbd` runs as the current user on port 4450 and all state
lives under a temp work dir that is cleaned up on exit.

## Running with Docker

Mirrors the CI job. Requires `/dev/fuse` and `SYS_ADMIN` (provided in the
compose file).

```sh
# build the base e2e image first (from the repo's docker/ dir)
docker compose -f test/samba/docker-compose.yml up --wait
docker compose -f test/samba/docker-compose.yml exec -T samba /run_inside_container.sh
docker compose -f test/samba/docker-compose.yml down -v
```

## CI

`.github/workflows/samba-integration.yml` runs on changes to `weed/mount/**`,
`weed/filer/**`, or `test/samba/**`. It builds the e2e image, builds the Samba
harness image on top, brings up the cluster, runs the battery, and uploads
server logs as artifacts.

## Notes

- The share disables Samba's DOS-attribute / xattr mapping and oplocks. The
  SeaweedFS FUSE mount does not implement that surface, and leaving it on
  produces `NT_STATUS_NOT_SUPPORTED` errors unrelated to data integrity.
- The share path is a subdirectory of the mount (`.../share`) so the runner can
  verify SMB-side operations directly on the FUSE side.
