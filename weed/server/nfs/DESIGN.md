# SeaweedFS NFS Support Design

Issue: `seaweedfs/seaweedfs#3972`
Status: proposed

## Summary

SeaweedFS should support NFS as a filer-backed gateway, not by re-exporting a
local `weed mount` FUSE mount.

The core decision is to build the filer-side primitives that NFS needs:

1. persistent inode assignment for every filer entry
2. inode-to-path lookup for stable filehandle resolution
3. deterministic filehandles that survive restart and failover
4. cluster-visible lock state

Once those primitives exist, SeaweedFS can add a `weed nfs` frontend. The first
frontend can use the already-vendored `rclone`/`go-nfs` NFSv3 stack for speed,
but the design must not depend on rclone's path-based handle cache or billy/VFS
abstractions.

## Goals

- Export any filer path over NFS to Linux, macOS, and virtualization clients.
- Support restart and multi-head failover without avoidable stale handles.
- Reuse SeaweedFS filer metadata, chunk reads, and write paths.
- Fit the same gateway model as existing WebDAV and SFTP support.
- Keep the implementation Go-first inside the SeaweedFS repository.

## Non-goals

- Full NFSv4.1, pNFS, ACLs, or Kerberos in the first phase.
- Wrapping `weed mount` with an external NFS export as the primary solution.
- Matching every kernel `nfsd` feature before an experimental release exists.

## Existing Building Blocks

SeaweedFS already has most of the pieces needed for a filer-native gateway:

- `weed/pb/filer.proto`
  filer metadata RPCs already cover lookup, list, create, update, delete,
  rename, statistics, configuration, and metadata subscription.
- `weed/server/webdav_server.go`
  proves SeaweedFS can expose a filer-backed filesystem protocol without FUSE.
- `weed/sftpd/*`
  shows the same pattern again with a different protocol surface.
- `weed/mount/*`
  already contains the strongest POSIX semantics in the repo:
  symlink, hardlink, xattr, metadata subscription, chunk cache, read/write
  buffering, ordered mutation streaming, and coarse distributed locking.
- `go.mod`
  already includes `github.com/rclone/rclone`, and rclone already wraps
  `github.com/willscott/go-nfs` for an experimental NFSv3 server.

## Why Not Re-export `weed mount`

The quickest experiment is to run `weed mount` and export the mounted path with
an external NFS server or with NFS-Ganesha VFS. That is not a good primary
design for SeaweedFS.

Problems:

- It stacks NFS on top of FUSE on top of filer metadata and volume reads.
- It duplicates caching and writeback decisions in too many layers.
- Filehandles become tied to local mount state instead of filer state.
- Multi-head failover is weak because each head only knows its local mount.
- Debugging correctness becomes much harder when NFS, FUSE, and filer all have
  their own buffering and invalidation behavior.

This path is acceptable only for a manual proof of concept.

## Option Analysis

### Option A: `weed mount` + external NFS export

Pros:

- almost no SeaweedFS code
- fastest way to test basic client compatibility

Cons:

- weak fit for stable filehandles
- poor base for active-active failover
- locking and cache behavior stay split across layers

Recommendation:

- useful prototype only

### Option B: native `weed nfs` backed directly by filer

Pros:

- matches the WebDAV and SFTP gateway pattern
- one metadata authority: the filer
- can reuse mount's chunk cache and mutation helpers without depending on FUSE
- easiest path to a supported SeaweedFS feature

Cons:

- requires new filer primitives for inode lookup, handles, and locks

Recommendation:

- main implementation path

### Option C: NFS-Ganesha with a SeaweedFS FSAL

Pros:

- strongest long-term path for mature NFS protocol handling
- attractive if NFSv4.x, richer lock/state recovery, or ACL support become
  mandatory

Cons:

- introduces a C plugin and separate packaging/runtime model
- not a good first implementation for a Go-first codebase

Recommendation:

- keep as an optional later frontend if production requirements exceed the
  native Go frontend

## The Real Constraint: Stable Object Identity

NFS is built around stable object identity. SeaweedFS filer metadata is
currently path-addressed.

That mismatch is the real design problem.

Today:

- `weed mount` can allocate and track inodes locally for its own session.
- filer entries already have an `inode` field in `FuseAttributes`.
- but not every filer create path guarantees that `inode` is set
  server-side
- and the filer does not currently expose a cluster-wide inode-to-path lookup
  service for other gateways

For NFS failover to work, any NFS head must be able to decode a filehandle and
resolve it to the current object without relying on local path caches.

## Required Filer Changes

### 1. Persistent inode assignment on every create path

Every filer entry must have a non-zero persistent inode.

That means inode assignment must happen server-side for:

- direct filer HTTP uploads
- gRPC `CreateEntry`
- auto-created parent directories
- WebDAV creates
- SFTP creates
- lazy remote entries materialized into the filer

Proposed rule:

- if `entry.Attributes.Inode == 0` at create time, the filer assigns one before
  persistence

Allocator options:

- reuse a master-backed sequencer
- add a filer-local snowflake-style allocator similar to the master sequencer

The design does not depend on which allocator is chosen. It only requires:

- cluster-unique inode values
- fast local generation
- no dependence on a mount process

### 2. Inode secondary index

NFS filehandles must resolve without knowing the current path. A handle cannot
just be a hashed path.

Add a filer-maintained secondary index:

- `inode -> {primary path, hardlink paths, kind, generation}`

This index must be updated transactionally with:

- create
- rename
- hardlink create
- unlink
- recursive delete

Required internal capability:

- resolve inode to a current path and entry

Useful outward API:

- `LookupEntryByInode(export_root, inode)` or an equivalent internal call

### 3. Deterministic filehandle format

Handles must be deterministic across NFS heads.

Recommended format:

- `export_id | object_type | inode | generation | checksum`

Properties:

- rename does not change the handle
- restart does not change the handle
- failover to another head does not change the handle
- delete and recreate can return `STALE` by changing `generation`

Do not use:

- local random handle allocation
- path hashes as the canonical handle format

Those approaches are acceptable for a local prototype but not for SeaweedFS HA.

### 4. Cluster-visible lock state

The current mount implementation has:

- per-process POSIX byte-range locks in `weed/mount/posix_file_lock.go`
- coarse whole-file distributed write coordination through the DLM

That is not enough for NFS.

NFS needs lock state keyed by inode and visible across all NFS heads, with:

- lease expiry
- reclaim/grace behavior after restart
- correct unlock semantics on disconnect

Proposed direction:

- build a filer-backed lock service first
- then adapt it to the chosen NFS frontend

### 5. Metadata subscription for cache invalidation

Each NFS head should follow filer metadata events using the existing
`SubscribeMetadata` stream.

Use this to:

- invalidate cached path resolutions
- update inode secondary index state if a local cache exists
- drop deleted handles quickly
- keep rename-heavy workloads correct without aggressive polling

## Proposed Architecture

### Command surface

Standalone command:

- `weed nfs -filer=<host:port> -filer.path=/export -ip.bind=0.0.0.0 -port=2049`

Later embedded mode:

- `weed filer -nfs`

This mirrors existing `weed webdav`, `weed sftp`, and `weed filer -webdav`
style deployment.

### Package layout

Suggested packages:

- `weed/server/nfs/`
  server entrypoint and command integration
- `weed/filerfs/`
  reusable filer-backed filesystem core extracted from mount/WebDAV/SFTP logic
- `weed/nfs/`
  NFS frontend adapter and protocol glue

### Internal layers

#### `FilerFsCore`

Responsibilities:

- lookup, list, create, update, delete, rename via filer RPC
- ordered mutations via `streamMutateMux`
- chunk reads via `filer.LookupFn`, reader cache, and tiered chunk cache
- buffered writes and final metadata commit

#### `InodeIndex`

Responsibilities:

- allocate or backfill inodes
- resolve inode to current object
- track hardlink path sets

#### `HandleManager`

Responsibilities:

- deterministic encode/decode
- fast local hot-cache for recent handles
- no authority over canonical identity

#### `LockManager`

Responsibilities:

- whole-file and byte-range locks backed by filer/DLM state
- lock reclaim during a restart grace period

#### `NfsFrontend`

Responsibilities:

- translate NFS RPCs to `FilerFsCore`
- map wire errors to filer and POSIX errors
- keep protocol engine swappable

## Library Choice

Use the already-vendored rclone/go-nfs stack only as the first RPC engine, not
as the architectural center.

Why:

- it already exists in `go.mod`
- it already provides a working NFSv3 server
- it is enough to validate client compatibility quickly

Why not depend on it too deeply:

- rclone's default persistent handle cache hashes paths, which is not
  rename-stable
- rclone's abstraction is centered on billy/VFS, not SeaweedFS filer identity
- future NFSv4.x or stronger lock semantics may require a different frontend

Implementation rule:

- hide the chosen NFS library behind a small internal server interface
- keep object identity, handles, inode lookup, and locks in SeaweedFS-owned
  code

## Protocol Scope by Phase

### Phase 1: experimental NFSv3

Support:

- regular files and directories
- create/read/write/delete/rename
- symlink/readlink
- chmod/chown/utimes
- statfs

Explicitly defer:

- Kerberos
- ACLs
- NFSv4 delegations
- pNFS

### Phase 2: HA-safe NFSv3

Add:

- deterministic handles across restart and across heads
- filer-side inode index
- hardlink correctness
- lock reclaim grace period
- metadata follower on each head

### Phase 3: enterprise semantics

If workloads require more protocol maturity than the Go frontend can deliver,
add a second frontend:

- SeaweedFS-specific FSAL for NFS-Ganesha

The filer-side inode, handle, and lock primitives remain the same.

## Read Path

1. Decode handle to `{export_id, inode, generation}`.
2. Resolve inode to current path through the inode index.
3. Fetch entry metadata from filer.
4. Read file chunks using the same chunk lookup and caching strategies used by
   mount/WebDAV.
5. Return NFS attributes derived from filer `FuseAttributes`.

This keeps SeaweedFS's existing chunk and cache logic in one place.

## Write Path

1. Open creates a buffered local writer.
2. Small writes can stay in memory; larger writes spill to a temp or swap file.
3. `commit`/`fsync` flushes data chunks first, then updates filer metadata.
4. The operation is not acknowledged as durable until the filer metadata update
   succeeds.

This is similar in spirit to existing WebDAV and SFTP buffering, but NFS must
surface a stricter durability boundary for `commit`/`fsync`.

## Consistency Model

Default target:

- close-to-open consistency

Rules:

- mutation replies only after filer metadata commit
- metadata subscription invalidates peer caches quickly
- handle identity does not depend on path cache lifetime

Optional export tuning can later trade stronger sync behavior for throughput.

## Security

Phase 1 should stay simple:

- AUTH_SYS or host-based access only
- per-export IP allowlist
- root squash option

This is consistent with an experimental NFSv3 gateway.

If the product needs NFSv4 + Kerberos, that is a separate protocol maturity
step, not something to bolt onto the first implementation.

## Testing

### Local CI

- unit tests for handle encode/decode
- unit tests for inode index updates on create/rename/link/unlink/delete
- stale-handle tests after delete/recreate
- lock tests for whole-file and byte-range behavior

### Integration tests

Run:

- filer
- NFS gateway
- Linux client container

Verify:

- mount/unmount
- file and directory CRUD
- rename with open handles
- restart and remount behavior
- multi-head failover behavior

### Protocol suites

Recommended suites:

- Connectathon/cthon04 for basic, general, special, and lock tests
- `xfstests` NFS subset for Linux client behavior
- `pynfs` if or when an NFSv4 frontend exists

SeaweedFS should also keep its existing `pjdfstest` mount coverage, because the
NFS gateway will still reuse the same filer semantics.

### Failure tests

- restart one NFS head while the filer stays up
- fail over a client from head A to head B
- rename or unlink during active reads and writes
- restart during lock ownership and verify reclaim/grace behavior
- filer leader change during active NFS traffic

## Recommended Implementation Order

1. Add server-side inode assignment for all filer create flows.
2. Add inode secondary index and lookup support.
3. Extract reusable filer-backed filesystem helpers from mount/WebDAV/SFTP into
   a shared core.
4. Add `weed nfs` experimental frontend using a thin NFSv3 RPC engine.
5. Add filer-backed lock state and reclaim logic.
6. Run protocol and failover suites before calling the feature supported.

## Open Questions

- Should inode allocation live in filer or reuse the master's sequencer?
- Is hardlink support required in phase 1, or can it return `NOTSUPP`
  initially?
- Is byte-range locking mandatory for the first target workloads
  (Atlassian/shared-home, VM image stores), or can phase 1 stay read-mostly?
- Should `weed filer -nfs` be embedded immediately, or only after the
  standalone `weed nfs` command stabilizes?

## Decision

Do not make NFS support a wrapper around `weed mount`.

Build NFS as a filer-native gateway. Start with the filer-side inode, handle,
and lock primitives that both a native Go frontend and a future Ganesha FSAL
would need. Then layer an experimental NFSv3 frontend on top of that shared
foundation.
