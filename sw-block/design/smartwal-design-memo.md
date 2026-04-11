# SmartWAL Design Memo

Date: 2026-04-10
Status: design research
Scope: BlockVol write path, crash recovery, and replicated recovery

## 1. What SmartWAL Is

SmartWAL changes what the WAL carries. Instead of storing the full data
payload for every write, the WAL stores only a small sequencing/recovery
descriptor. The actual data goes directly to the extent store.

```
Current:    WriteLBA(lba, data) → WAL(LSN, lba, data[4KB]) → flusher → extent
SmartWAL:   WriteLBA(lba, data) → extent(lba, data[4KB]) → WAL(LSN, lba, checksum[32B])
```

The WAL becomes a metadata journal: ordered, durable, replayable, but small.

## 2. What SmartWAL Is Not

SmartWAL is not "skip the WAL." The WAL still exists. It still provides:

- Crash recovery ordering
- Durable commit proof
- Replication sequencing (LSN)
- Recoverability classification

SmartWAL changes the write encoding, not the recovery authority.

## 3. Why SmartWAL Matters for BlockVol

The current WAL carries full 4KB payloads. A 64MB WAL holds ~16K entries.
Under sustained qd=16 writes, the WAL fills in seconds. This creates:

1. WAL exhaustion under burst writes (the 30s dd_write timeout)
2. WAL retention tension (pin for replication vs recycle for space)
3. Catch-up vs rebuild forced by WAL recycling, not by actual data divergence
4. Flusher becoming the critical path (must copy data from WAL to extent
   before WAL can recycle)

With SmartWAL, a 64MB WAL holds ~1M entries (64B each). WAL pressure
effectively disappears. The flusher is no longer needed as a data copier
(data already in extent). The retention problem vanishes because the WAL
is small enough to retain indefinitely.

## 4. The Algorithm

### 4.1 Write Path (Single Node)

```
WriteLBA(lba, data):
  1. ALLOCATE extent space for lba (may be existing location or new CoW location)
  2. WRITE data to extent at allocated location
  3. BARRIER: fdatasync on extent fd — data is now durable on storage
  4. WRITE WAL record: {LSN, epoch, lba, physical_offset, length, crc32(data)}
  5. WAL record is in the WAL ring buffer (durable after next WAL fsync)
  6. Return success to SCSI/iSCSI layer

SyncCache (SCSI SYNCHRONIZE CACHE):
  1. fsync the WAL — all pending WAL records become durable
  2. The SyncCache response proves: all writes up to this LSN are
     both data-durable (step 3) and metadata-durable (this fsync)
```

### 4.2 Crash Recovery (Single Node)

```
Recovery:
  1. Open extent file (data store). It may contain writes that completed
     step 2-3 but not step 4 (data durable, no WAL record). These are
     invisible — the WAL doesn't reference them, so they are harmlessly
     overwritten later.

  2. Open WAL. Find the last valid record (checksum-verified, contiguous
     from WAL start or last checkpoint).

  3. For each committed WAL record from oldest to newest:
     a. Read data from extent at the physical_offset in the record.
     b. Verify crc32 matches the WAL record's stored checksum.
     c. If match: this write is recovered. Update the in-memory dirty
        map / extent index.
     d. If mismatch: DATA-BEFORE-METADATA INVARIANT VIOLATED. This means
        the barrier at step 3 was ineffective. Log critical error. Mark
        this LBA as damaged.

  4. Rebuild allocator free-space from the extent index (not from a
     persistent free-space map). Any blocks not referenced by the index
     are free — this reclaims orphaned allocations from step 1-2 where
     step 4 never completed.

  5. Set nextLSN = last recovered LSN + 1.
  6. Resume normal operation.
```

### 4.3 Write Path (Replicated)

```
WriteLBA on primary:
  1-5. Same as single-node write path (data to extent, metadata to WAL)
  6. SHIP to replica: {LSN, epoch, lba, data[4KB], crc32}
     Note: the wire message carries the FULL data payload. SmartWAL
     optimizes local WAL, not the replication wire. The replica needs
     the data bytes.
  7. Return success to SCSI layer (write-back: after step 5)

Ship to replica:
  The shipper reads the data from the extent (not from the WAL) and
  sends it to the replica over the data channel. The wire format is
  unchanged from today.

On the replica:
  1. Receive {LSN, epoch, lba, data, crc32} from the data channel
  2. Write data to local extent at lba
  3. Barrier: fdatasync on local extent
  4. Write local WAL record: {LSN, epoch, lba, physical_offset, crc32}
  5. The replica's WAL is also SmartWAL — small records only

Barrier (SyncCache / sync_all):
  Primary sends BarrierReq(target_lsn) to replica via ctrl channel.
  Replica verifies receivedLSN >= target_lsn, calls fdatasync on its
  WAL, returns flushedLSN.
  This proves: all entries up to flushedLSN are both data-durable and
  metadata-durable on the replica.
```

### 4.4 Replicated Crash Recovery

```
Primary crashes and restarts:
  1. Local recovery (section 4.2) — extent + WAL replay
  2. Primary discovers its replicas via master assignment
  3. Probe each replica:
     a. Replica reports its flushedLSN = R
     b. Primary's WAL start = S, head = H
     c. If R >= S: WAL catch-up from R to H (small WAL records,
        but data must be re-read from extent and re-shipped)
     d. If R < S: extent-based delta recovery (LBA dirty map)
  4. Catch-up or delta recovery completes → replica back to keepup

Replica crashes and restarts:
  1. Local recovery (section 4.2) on replica
  2. Replica reconnects to primary
  3. Primary probes: replica's R vs primary's S and H
  4. Same decision: WAL catch-up or extent delta

Key difference from current system:
  - WAL holds ~1M entries instead of ~16K
  - WAL recycling is rare (WAL is small, entries are tiny)
  - Catch-up from WAL covers much longer outages
  - Full rebuild becomes rare — only when WAL is truly exhausted
    (days of accumulated writes, not seconds)
```

## 5. The Invariants

### I1: Data Before Metadata

The extent write (step 2) must be durable (fdatasync, step 3) BEFORE the
WAL record (step 4) is written. If this is violated, crash recovery finds
a WAL record pointing to unwritten or partially written extent data.

This is the #1 source of bugs in production SmartWAL implementations:

- Ceph BlueStore: early releases had missing barriers between extent write
  and RocksDB metadata commit. Resulted in data corruption on power loss.

- ZFS ZIL: bug where indirect data blocks were referenced by ZIL records
  before the data block's fdatasync completed. Fixed by adding explicit
  flush between indirect write and ZIL record write.

- LVM thin: metadata journal entries referenced data blocks that were
  not yet durable. Fixed by adding bio flush flags.

For BlockVol: the barrier is a single fdatasync call on the extent fd
between step 2 and step 4. This must never be elided, even under
performance optimization pressure.

### I2: WAL Record Atomicity

Each WAL record must be self-contained and checksummed. On recovery, any
record that fails its checksum is treated as not-written. All records
after a failed checksum are also discarded (preserving sequence order).

The WAL ring buffer already provides this via per-entry CRC. No change
needed for SmartWAL — the record format changes (smaller payload) but
the atomicity mechanism is the same.

### I3: Monotonic Sequence

WAL records have strictly increasing LSNs. On recovery, if a gap is
detected, all records after the gap are discarded. This prevents
a scenario where a later write is visible but an earlier write is lost.

### I4: Orphan Safety

Allocated-but-unreferenced extent blocks (data written but WAL record
not committed) must be harmlessly reclaimable. The allocator rebuilds
its free-space from the extent index at mount time.

For BlockVol with fixed-size 4KB blocks and a 1GB extent: the extent is
pre-allocated. There is no dynamic allocation. Orphaned writes are simply
blocks that will be overwritten by the next write to that LBA. No special
reclamation needed.

### I5: No In-Place Overwrite During Recovery

During recovery, if the WAL indicates LBA X was written, the recovery
process must not assume the current extent data at LBA X is correct.
It must verify via the CRC in the WAL record.

In normal operation, writes CAN overwrite in-place (same LBA, same
extent offset) because the WAL provides the recovery mechanism. But
during crash recovery, the extent may contain a partial write from a
crash during step 2. The CRC check detects this.

### I6: Replication Wire Carries Full Data

SmartWAL optimizes the LOCAL WAL, not the replication wire. The replica
needs the full data payload to apply the write. The shipper reads data
from the extent (not from the WAL) and ships it.

This means:
- The shipper must read from extent, not WAL (new behavior)
- The extent data at a given LBA must be stable while being shipped
  (no concurrent overwrite to the same physical location during ship)
- The dirty map tracks which LBAs need shipping

### I7: Replica Applies Data Before WAL

On the replica, the received data must be written to the local extent
and fsynced BEFORE the local WAL record is written. The replica's crash
recovery must be able to distinguish "data received and durable" from
"data received but not durable." The local WAL record is the proof of
durability.

## 6. Gotchas and Mitigations

### G1: Barrier Cost

The fdatasync between extent write and WAL write adds latency on every
write. For a single 4KB write on NVMe SSD, fdatasync costs ~50-100us.

Mitigation: group commit. Batch multiple extent writes, issue one
fdatasync, then write all corresponding WAL records. The SyncCache
path already does group commit for the WAL fsync. Extend it to the
extent fdatasync.

```
Group commit with SmartWAL:
  Accumulate N writes to extent (no barrier yet)
  fdatasync extent (one barrier for N writes)
  Write N WAL records
  On SyncCache: fsync WAL
```

This amortizes the barrier cost across N writes. With qd=16, the
effective barrier cost per write is ~6us instead of ~100us.

### G2: Concurrent Read During Write

If a read arrives for an LBA that has been written to the extent (step 2)
but whose WAL record has not been committed (step 4), what should the
read return?

Answer: the new data. The extent is the source of truth for reads. The
WAL is only for crash recovery. A read after step 2 sees the new data
regardless of whether the WAL record exists yet.

This is consistent with write-back semantics: the write is "admitted"
after step 2, and reads see admitted data.

### G3: Crash Between Extent Write and WAL Write

If the system crashes after step 2-3 but before step 4:
- The extent has the new data at the LBA
- The WAL has no record of this write
- On recovery, the WAL doesn't know about this write
- The next write to this LBA will overwrite it
- If a replica has received this write (shipped between step 2 and crash),
  the replica has data the primary lost

Mitigation: This is the same as the current system. In write-back mode,
a crash after WAL append but before fsync can lose the write. SmartWAL
does not change the durability contract — only SyncCache provides the
durable proof.

### G4: Shipper Reads Stale Extent Data

The shipper reads data from the extent for replication. If a second
write to the same LBA arrives between the first write and the shipper
reading the first write's data, the shipper may read the second write's
data instead.

Mitigation: The shipper should read the data at the time of the write
(inline with WriteLBA), not lazily from the extent later. This means
the ship path is:

```
WriteLBA:
  1. Write data to extent
  2. Barrier
  3. Write WAL record
  4. Ship(LSN, lba, data)  ← data is the same buffer from step 1
```

The data buffer is passed directly to Ship, not re-read from extent.
This eliminates the stale-read race.

For the LBA dirty map based recovery path (catch-up from extent after
an outage), stale reads are handled differently: the primary freezes
a consistent snapshot (checkpoint) before reading extent data for the
delta transfer. The checkpoint LSN defines which data is authoritative.

### G5: WAL Wrap-Around With Small Records

With ~1M entries in a 64MB WAL, the WAL ring buffer wraps much more
slowly. But it still wraps eventually. When it does, old records must
be safe to overwrite.

Mitigation: the same recycling logic applies. A record can be recycled
when:
1. It has been fsynced (part of a durable SyncCache)
2. All replicas have confirmed receipt (if using WAL-based catch-up)
3. Or the replica uses LBA dirty map for catch-up (no WAL retention
   needed for replication)

With LBA dirty map based recovery, condition 2 is eliminated. The WAL
recycles based only on condition 1 (local durability). This is the clean
model.

### G6: SmartWAL and the LBA Dirty Map Interaction

SmartWAL and LBA dirty map are complementary:

- SmartWAL eliminates WAL pressure (small records, huge capacity)
- LBA dirty map eliminates WAL retention for replication (catch-up from
  extent, not from WAL)

Together:
- WAL does one job: local crash recovery sequencing
- Extent does one job: authoritative data store
- LBA dirty map does one job: per-replica catch-up tracking
- No tension between any of them

### G7: Replica Crash Recovery With SmartWAL

The replica also uses SmartWAL locally. On replica crash:

1. Replay local WAL records → verify extent data via CRC
2. Report flushedLSN to primary on reconnect
3. Primary sends delta (from flushedLSN to current) via extent reads
4. Replica applies delta, writes to local extent, writes local WAL records
5. Replica is caught up

The replica's local SmartWAL recovery is identical to the primary's.
The replication catch-up is identical to today except the source of
catch-up data is the extent (via dirty map) instead of the WAL.

### G8: Overwrite Ordering Under Concurrent Writers

If multiple SCSI commands target the same LBA range simultaneously:

```
Thread A: WriteLBA(100, dataA) → extent write → WAL(LSN=5, lba=100)
Thread B: WriteLBA(100, dataB) → extent write → WAL(LSN=6, lba=100)
```

The WAL ordering (LSN 5 before LSN 6) must match the extent ordering
(dataA before dataB). If Thread B's extent write completes first, the
extent has dataB, but Thread A's WAL record (LSN=5) references the same
physical location with a CRC of dataA. On recovery, the CRC mismatch
for LSN=5 indicates corruption.

Mitigation: WriteLBA must hold the per-LBA write lock through both
extent write and WAL record write. This ensures that for the same LBA,
extent writes and WAL records are in the same order. The existing ioMu
(or per-LBA lock in the dirty map) provides this.

### G9: Performance Implication of Extent-First Write

Current: WriteLBA → WAL append (sequential, fast) → return
SmartWAL: WriteLBA → extent pwrite (random, slower) → barrier → WAL append

The extent write is a random pwrite to a pre-allocated file. On NVMe SSD,
this is ~10-20us for 4KB. The WAL append is sequential. Adding the extent
pwrite before the WAL append increases per-write latency by ~10-20us.

Mitigation: This is offset by:
1. No flusher needed (data already in extent) — saves the copy
2. WAL is tiny — SyncCache fsync is faster
3. Group commit amortizes the barrier across N writes
4. The flusher was already doing pwrite to extent — we're just moving
   it earlier in the pipeline

Net effect: similar or slightly better throughput, slightly higher
single-write latency, significantly better burst write behavior (no
WAL exhaustion).

## 7. Size Threshold

For BlockVol with 4KB block size, the threshold decision is simple:

- All writes are exactly 4KB (one block) or multiples of 4KB
- A 4KB data payload + 32B WAL record = 4096B + 32B overhead
- A metadata-only WAL record = 32-64B

At 4KB, the SmartWAL overhead is 99.2% savings per WAL entry.
There is no reason to inline 4KB payloads in the WAL.

Recommendation: ALL writes use SmartWAL (extent-first, metadata-WAL).
No inline path needed for block storage with fixed block size.

For comparison:
- ZFS ZIL threshold: 32KB (filesystem with variable record sizes)
- Ceph BlueStore: 4-16KB (object store with variable sizes)
- BlockVol: 4KB fixed — always SmartWAL

## 8. Implementation Phases

### Phase 1: Extent-first write path
- WriteLBA writes to extent before WAL
- WAL record format changes to metadata-only (LSN, lba, crc32, flags)
- Flusher becomes a checkpoint mechanism (periodic metadata snapshot)
  instead of a data copier
- Crash recovery replays WAL records, verifies extent data via CRC
- All existing tests must pass with the new write path

### Phase 2: LBA dirty map for replication catch-up
- Per-replica bitmap: which LBAs changed since last confirmed position
- On catch-up: read dirty LBAs from extent, ship to replica
- WAL no longer retained for replication — recycles freely
- Catch-up and rebuild become the same operation (extent delta), just
  different sizes

### Phase 3: Group commit for extent barrier
- Batch N extent writes, one fdatasync, N WAL records
- Amortize barrier cost across concurrent writes
- This is the performance optimization phase

## 9. Relationship to V3 Engine

SmartWAL is a data algorithm change, not an engine change. The V3 engine
should not know whether writes are WAL-inlined or extent-first. The engine
sees:

- RecoverabilityClass: inline | extent_referenced | snapshot_based
- DurabilityProof: WAL fsync LSN + barrier confirmation
- RecoveryDecision: no_recovery | catch_up | delta_rebuild | full_rebuild

SmartWAL changes how RecoverabilityClass is produced (by the storage
layer), but not how the engine uses it (for decisions).

Rule: SmartWAL changes write encoding and recoverability classes, but
must not change the core truth domains of V3.

## 10. Production References

| System | Pattern | Years in Production | Key Lesson |
|--------|---------|--------------------|-|
| Ceph BlueStore | Extent + RocksDB metadata journal | 6+ (since Luminous 2017) | Barrier bugs were the #1 corruption source |
| ZFS ZIL | Inline < 32KB, indirect >= 32KB | 15+ (since 2005) | Indirect block lifecycle must be explicit |
| LVM thin | Data device + metadata journal | 10+ (since kernel 3.2, 2012) | Metadata journal rebuild on mount is essential |
| DRBD | Activity log bitmap (no data WAL) | 20+ (since 2004) | Bitmap-only tracking is sufficient for block devices |
| PostgreSQL | Full-page images in WAL | 25+ (since 1996) | Opposite direction — but proves WAL atomicity matters |

The common lesson: the algorithm is well-understood and proven. The bugs
are in barrier enforcement and lifecycle management, not in the algorithm
design itself.
