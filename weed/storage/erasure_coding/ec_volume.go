package erasure_coding

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	NotFoundError             = errors.New("needle not found")
	destroyDelaySeconds int64 = 0
)

// ecjLoadChunkBytes bounds each positional read when seeding deletedNeedles
// from .ecj. A multiple of NeedleIdSize; 1 MiB is 131072 entries per syscall,
// which keeps a bloated journal from spending mount in per-entry reads.
const ecjLoadChunkBytes = 1 << 20

// A .ecj smaller than this is never rewritten, however redundant. Below a
// megabyte the duplication costs nothing and the rewrite is pure churn.
const ecjCompactMinBytes = 1 << 20

// Rewrite only when the journal is at least this many times larger than the
// set it encodes. A journal holds one entry per delete, so a healthy one is
// close to 1x; four times the set means most of the file is repeats.
const ecjCompactRatio = 4

// EcjCompactTmpExt names the staging file for a compacted journal, next to the
// .ecj it replaces. Listed with the other EC index files wherever those are
// removed, so a tmp left by a crash between write and rename does not outlive
// its volume.
const EcjCompactTmpExt = ".ecj.compact.tmp"

type EcVolume struct {
	VolumeId                  needle.VolumeId
	Collection                string
	dir                       string
	dirIdx                    string
	ecxActualDir              string // directory where .ecx/.ecj were actually found (may differ from dirIdx after fallback)
	ecxFile                   *os.File
	ecxFileSize               int64
	ecxCreatedAt              time.Time
	Shards                    []*EcVolumeShard
	ShardLocations            map[ShardId][]pb.ServerAddress
	ShardLocationsRefreshTime time.Time
	// ShardLocationsStale marks the map for a prompt re-check: a read that failed
	// against a cached location has disproved what the map claims, and the normal
	// freshness window is far too long to serve from a map known to be wrong.
	ShardLocationsStale bool
	ShardLocationsLock  sync.RWMutex
	Version             needle.Version
	ecjFile             *os.File
	ecjFileAccessLock   sync.Mutex
	// ecjHold registers this volume as a holder of its .ecj path for as long
	// as ecjFile may be open; see ecj_registry.go.
	ecjHold     *ecjHold
	diskType    types.DiskType
	datFileSize int64
	ExpireAtSec uint64     //ec volume destroy time, calculated from the ec volume was created
	ECContext   *ECContext // EC encoding parameters

	// EncodeTsNs is the encode time (unix nanos) loaded from .vif; reads carry it
	// so a shard from a different encode run is rejected. 0 for pre-upgrade volumes.
	EncodeTsNs int64

	// ecjFileSize mirrors the on-disk size of the .ecj deletion journal and
	// is maintained under ecjFileAccessLock: the write offset for appends, and
	// the size mount-time compaction compares against the id set and re-checks
	// on disk before replacing the file. The runtime delete count comes from
	// deletedNeedles, not from this.
	ecjFileSize int64

	// deletedNeedles is the in-memory set of needle ids that have been
	// deleted since the volume was encoded. .ecx is immutable at runtime —
	// it only stores the sorted (id, offset, size) index written at encode
	// time — and runtime deletes are journaled to .ecj + tracked here.
	// Reads consult this set to mask out deleted needles on top of the
	// sealed .ecx lookup. Heartbeat delete_count is derived from len(set).
	// Seeded from .ecj in NewEcVolume and updated under deletedNeedlesLock.
	deletedNeedlesLock sync.RWMutex
	deletedNeedles     map[types.NeedleId]struct{}

	// Bitrot checksum sidecar for the active generation (optional). bitrot is
	// nil unless bitrotStatus == BitrotOn, and is loaded at mount. Guarded by
	// bitrotLock.
	bitrotLock   sync.RWMutex
	bitrot       *volume_server_pb.EcBitrotProtection
	bitrotStatus BitrotStatus

	lastIoError        error
	lastIoErrorCount   int32
	ioErrorQuarantined bool
	lastIoErrorLock    sync.RWMutex
}

func (ev *EcVolume) CheckReadWriteError(err error) {
	if err == nil {
		ev.clearIoError()
		return
	}
	if errors.Is(err, syscall.EIO) {
		ev.noteIoError(err)
		return
	}
	ev.clearIoError()
}

func (ev *EcVolume) noteIoError(err error) {
	ev.lastIoErrorLock.Lock()
	defer ev.lastIoErrorLock.Unlock()
	ev.lastIoError = err
	ev.lastIoErrorCount++
	stats.VolumeServerStorageIoErrorCounter.Inc()
}

func (ev *EcVolume) clearIoError() {
	ev.lastIoErrorLock.Lock()
	defer ev.lastIoErrorLock.Unlock()
	ev.lastIoError = nil
	ev.lastIoErrorCount = 0
}

func (ev *EcVolume) ResetIoErrorState() {
	ev.lastIoErrorLock.Lock()
	defer ev.lastIoErrorLock.Unlock()
	ev.lastIoError = nil
	ev.lastIoErrorCount = 0
	ev.ioErrorQuarantined = false
}

func (ev *EcVolume) MarkIoQuarantined() {
	ev.lastIoErrorLock.Lock()
	defer ev.lastIoErrorLock.Unlock()
	ev.ioErrorQuarantined = true
}

func (ev *EcVolume) GetIoErrorState() (error, int32, bool) {
	ev.lastIoErrorLock.RLock()
	defer ev.lastIoErrorLock.RUnlock()
	return ev.lastIoError, ev.lastIoErrorCount, ev.ioErrorQuarantined
}

// statEcxSize returns the size of an .ecx file, os.ErrNotExist when it is absent
// (or a directory), so the resolver can prefer a non-empty copy.
func statEcxSize(path string) (int64, error) {
	info, statErr := os.Stat(path)
	if statErr != nil {
		return 0, statErr
	}
	if info.IsDir() {
		return 0, os.ErrNotExist
	}
	return info.Size(), nil
}

func NewEcVolume(diskType types.DiskType, dir string, dirIdx string, collection string, vid needle.VolumeId) (ev *EcVolume, err error) {
	return newEcVolumeWith(diskType, dir, dirIdx, collection, vid, realEcjFsOps)
}

// newEcVolumeWith is NewEcVolume with the journal-compaction filesystem steps
// supplied, so tests can drive a failing publish through the real mount.
func newEcVolumeWith(diskType types.DiskType, dir string, dirIdx string, collection string, vid needle.VolumeId, ecjOps ecjFsOps) (ev *EcVolume, err error) {
	ev = &EcVolume{dir: dir, dirIdx: dirIdx, Collection: collection, VolumeId: vid, diskType: diskType}

	dataBaseFileName := EcShardFileName(collection, dir, int(vid))
	indexBaseFileName := EcShardFileName(collection, dirIdx, int(vid))

	// open ecx file. Wrap errors with %w so callers walking up the stack
	// (notably Store.MountEcShards) can use errors.Is(err, os.ErrNotExist)
	// to decide whether to try the next local disk vs. bail. A 0-byte .ecx
	// is a legitimate index for a volume that had no live needles at encode
	// time (e.g. all needles deleted before WriteSortedFileFromIdx) and
	// must mount successfully here. A 0-byte stub left by a failed copy
	// stream is indistinguishable from that empty case by file size alone;
	// preventing such stubs is the receiver-side cleanup in writeToFile's
	// job, not this open path.
	// Resolve the .ecx, preferring the copy co-located with the shard data on
	// this disk — where a move or reconstruct leaves it — then the caller's
	// index directory. That directory is either the shared -dir.idx dir or a
	// sibling disk that owns the .ecx when this disk holds only a 0-byte stub
	// left by an interrupted copy (#9212). A 0-byte .ecx is a legitimate empty
	// index, so the local copy yields only to a *non-empty* copy elsewhere,
	// never to a mere absence: prefer a non-empty .ecx local-first, then fall
	// back to whichever exists at all.
	localBaseFileName := dataBaseFileName
	sharedBaseFileName := indexBaseFileName
	localSize, localErr := statEcxSize(localBaseFileName + ".ecx")
	sharedSize, sharedErr := int64(0), os.ErrNotExist
	if dirIdx != dir {
		sharedSize, sharedErr = statEcxSize(sharedBaseFileName + ".ecx")
	}
	switch {
	case localErr == nil && localSize > 0:
		indexBaseFileName, ev.ecxActualDir = localBaseFileName, dir
	case sharedErr == nil && sharedSize > 0:
		indexBaseFileName, ev.ecxActualDir = sharedBaseFileName, dirIdx
		glog.V(1).Infof("ecx not local at %s.ecx, using %s.ecx", localBaseFileName, sharedBaseFileName)
	case localErr == nil: // local exists but is a 0-byte empty index
		indexBaseFileName, ev.ecxActualDir = localBaseFileName, dir
	case sharedErr == nil: // only a 0-byte copy in the index dir
		indexBaseFileName, ev.ecxActualDir = sharedBaseFileName, dirIdx
	default:
		return nil, fmt.Errorf("cannot open ec volume index %s.ecx (or %s.ecx): %w", localBaseFileName, sharedBaseFileName, os.ErrNotExist)
	}
	if ev.ecxFile, err = backend.OpenVolumeFile(indexBaseFileName+".ecx", os.O_RDWR); err != nil {
		return nil, fmt.Errorf("cannot open ec volume index %s.ecx: %w", indexBaseFileName, err)
	}
	ecxFi, statErr := ev.ecxFile.Stat()
	if statErr != nil {
		_ = ev.ecxFile.Close()
		return nil, fmt.Errorf("can not stat ec volume index %s.ecx: %w", indexBaseFileName, statErr)
	}
	ev.ecxFileSize = ecxFi.Size()
	ev.ecxCreatedAt = ecxFi.ModTime()

	// open ecj file and seed the in-memory deleted set from it.
	//
	// Register as a holder first: this waits out a compaction another disk's
	// volume may be running on the same path, so the handle below is on the
	// final inode, and it stops any compaction from replacing the file under
	// this handle.
	ev.ecjHold = acquireEcjHold(indexBaseFileName + ".ecj")
	if ev.ecjFile, err = openEcjFile(indexBaseFileName + ".ecj"); err != nil {
		ev.Close()
		return nil, fmt.Errorf("cannot open ec volume journal %s.ecj: %v", indexBaseFileName, err)
	}
	if ecjFi, statErr := ev.ecjFile.Stat(); statErr == nil {
		ev.ecjFileSize = ecjFi.Size()
	} else {
		glog.Warningf("stat ec volume journal %s.ecj: %v", indexBaseFileName, statErr)
	}
	// Truncate a torn tail before the loader runs: appends land at the
	// physical end, so a trailing partial record would misalign every later
	// delete and lose it on the next mount.
	if ragged := ev.ecjFileSize % int64(types.NeedleIdSize); ragged != 0 {
		whole := ev.ecjFileSize - ragged
		glog.Warningf("ec volume %d: truncating torn .ecj tail %d -> %d bytes", vid, ev.ecjFileSize, whole)
		if truncErr := ev.ecjFile.Truncate(whole); truncErr != nil {
			ev.Close()
			return nil, fmt.Errorf("ec volume %d: repair torn .ecj tail: %w", vid, truncErr)
		}
		if syncErr := ev.ecjFile.Sync(); syncErr != nil {
			ev.Close()
			return nil, fmt.Errorf("ec volume %d: sync .ecj after tail repair: %w", vid, syncErr)
		}
		ev.ecjFileSize = whole
	}
	ev.deletedNeedles = make(map[types.NeedleId]struct{})
	loadErr := ev.loadDeletedNeedlesFromEcj()
	if loadErr != nil {
		glog.Warningf("ec volume %d: load deleted needles from .ecj: %v", vid, loadErr)
	}

	// read volume info. Prefer .vif at the data dir (where shards live), but
	// fall back to the index dir when the data dir does not have one — the
	// orphan-shard reconciliation in Store loads shards on a disk whose only
	// EC artefacts are .ec?? files, with .ecx / .ecj / .vif on a sibling disk
	// (issue #9212). Without this fallback we'd write a stub .vif on the
	// shard disk and lose the real EC config + datFileSize.
	vifFileName := dataBaseFileName + ".vif"
	if dirIdx != dir {
		if _, statErr := os.Stat(vifFileName); statErr != nil && os.IsNotExist(statErr) {
			altVif := EcShardFileName(collection, dirIdx, int(vid)) + ".vif"
			if _, altStatErr := os.Stat(altVif); altStatErr == nil {
				vifFileName = altVif
			}
		}
	}
	ev.Version = needle.Version3
	// A present-but-unreadable or malformed .vif FAILS the mount: every new
	// encode records a positive uniform block size there, and defaulting to
	// the legacy layout would serve those shards with the wrong offset math.
	// Absent stays legal — legacy volumes predate the sidecar.
	volumeInfo, _, found, vifErr := volume_info.MaybeLoadVolumeInfo(vifFileName)
	if vifErr != nil {
		ev.Close()
		return nil, fmt.Errorf("ec volume %d: load %s: %w", vid, vifFileName, vifErr)
	}
	if found {
		ev.Version = needle.Version(volumeInfo.Version)
		ev.datFileSize = volumeInfo.DatFileSize
		ev.ExpireAtSec = volumeInfo.ExpireAtSec

		// Initialize EC context from .vif if present; fallback to defaults
		if volumeInfo.EcShardConfig != nil {
			ds := int(volumeInfo.EcShardConfig.DataShards)
			ps := int(volumeInfo.EcShardConfig.ParityShards)
			ev.EncodeTsNs = volumeInfo.EcShardConfig.GetEncodeTsNs()

			// A config that is PRESENT but records an impossible ratio is not
			// a volume to fall back on: substituting the default 10+4 with the
			// legacy layout would read uniform shards with the wrong offset
			// math and answer with the wrong bytes. Only an ENTIRELY absent
			// config means "this predates the record", which the else-branch
			// below serves with the legacy defaults.
			if !ValidEcShardCounts(volumeInfo.EcShardConfig.DataShards, volumeInfo.EcShardConfig.ParityShards) {
				ev.Close()
				return nil, fmt.Errorf("ec volume %d: %s records invalid shard counts %d+%d",
					vid, vifFileName, volumeInfo.EcShardConfig.DataShards, volumeInfo.EcShardConfig.ParityShards)
			} else if blockErr := ValidateBlockSize(volumeInfo.EcShardConfig.GetBlockSize()); blockErr != nil {
				// A recorded block size that no encoder could have produced maps
				// every read to the wrong shard offset. Refuse the mount rather
				// than serve those bytes or silently pick a layout.
				ev.Close()
				return nil, fmt.Errorf("ec volume %d: %s: %w", vid, vifFileName, blockErr)
			} else {
				ev.ECContext = &ECContext{
					Collection:   collection,
					VolumeId:     vid,
					DataShards:   ds,
					ParityShards: ps,
					BlockSize:    volumeInfo.EcShardConfig.GetBlockSize(),
				}
				glog.V(1).Infof("Loaded EC config from VolumeInfo for volume %d: %s", vid, ev.ECContext.String())
			}
		} else {
			// A vif that carries no ecShardConfig answers nothing about the
			// layout — it is no more informative than an absent one, so it
			// must not skip the sidecar. Going straight to the defaults here
			// read a uniform volume with the legacy offset math.
			cfg, sidecarFound, sidecarErr := layoutFromSidecar(dataBaseFileName, indexBaseFileName)
			if sidecarErr != nil {
				ev.Close()
				return nil, fmt.Errorf("ec volume %d: %s records no EC config and the bitrot sidecar cannot establish the layout: %w", vid, vifFileName, sidecarErr)
			}
			if sidecarFound {
				ev.ECContext = &ECContext{
					Collection:   collection,
					VolumeId:     vid,
					DataShards:   int(cfg.GetDataShards()),
					ParityShards: int(cfg.GetParityShards()),
					BlockSize:    cfg.GetBlockSize(),
				}
				ev.EncodeTsNs = cfg.GetEncodeTsNs()
				glog.V(0).Infof("ec volume %d: .vif records no EC config; took it from the bitrot sidecar: %s",
					vid, ev.ECContext.String())
			} else {
				ev.ECContext = NewDefaultECContext(collection, vid)
			}
		}
	} else {
		// Don't fabricate a stub .vif here: a version-only stub implies the
		// default 10+4 ratio with DatFileSize=0 and no encode identity, which
		// the custom-ratio resolver and the startup credibility checks must not
		// mistake for an authoritative config. Mount with in-memory defaults and
		// leave the real .vif to the encoder or a recovery tool (the Rust volume
		// server already behaves this way).
		//
		// The bitrot sidecar records the same EC config at encode time, so when
		// it is present it answers the layout question the missing .vif cannot:
		// defaulting a uniform-layout volume to the legacy block sizes maps
		// every read to the wrong shard offset. `weed fix -ecx` reads the
		// sidecar for the same reason.
		ev.ECContext = NewDefaultECContext(collection, vid)
		cfg, sidecarFound, sidecarErr := layoutFromSidecar(dataBaseFileName, indexBaseFileName)
		if sidecarErr != nil {
			// With no .vif the sidecar is the ONLY record of this volume's
			// layout. Present but unusable is not "assume legacy" — that
			// answers reads with the wrong shard offsets, which is worse than
			// not answering at all.
			ev.Close()
			return nil, fmt.Errorf("ec volume %d: no .vif and the bitrot sidecar cannot establish the layout: %w", vid, sidecarErr)
		}
		if sidecarFound {
			ev.ECContext = &ECContext{
				Collection:   collection,
				VolumeId:     vid,
				DataShards:   int(cfg.GetDataShards()),
				ParityShards: int(cfg.GetParityShards()),
				BlockSize:    cfg.GetBlockSize(),
			}
			ev.EncodeTsNs = cfg.GetEncodeTsNs()
			glog.V(0).Infof("ec volume %d: .vif missing; took EC config from the bitrot sidecar: %s",
				vid, ev.ECContext.String())
		} else {
			// Only now are the defaults what the volume actually mounted on;
			// logging this after the sidecar answered would send an operator
			// triaging wrong bytes after the legacy layout instead.
			glog.Warningf("vif file not found, using defaults, volumeId:%d, filename:%s", vid, vifFileName)
		}
	}

	ev.ShardLocations = make(map[ShardId][]pb.ServerAddress)

	// Load the active-generation bitrot checksum sidecar (optional).
	if err := ev.loadActiveBitrotSidecar(); err != nil {
		ev.Close()
		return nil, err
	}

	// Fold a bloated journal back down to the set it encodes. Last, once every
	// check that can refuse the mount has passed, so a volume the server
	// declines to serve keeps its files as they were.
	if compactErr := ev.compactEcjAfterLoad(loadErr, ecjOps); compactErr != nil {
		ev.Close()
		return nil, fmt.Errorf("ec volume %d: .ecj compaction left no usable journal handle: %w", vid, compactErr)
	}

	return
}

func (ev *EcVolume) AddEcVolumeShard(ecVolumeShard *EcVolumeShard) (bool, error) {
	for _, s := range ev.Shards {
		if s.ShardId == ecVolumeShard.ShardId {
			return false, nil
		}
	}
	// A 0-byte shard file beside an index with entries is residue of a
	// failed copy or a truncation, not a mountable shard: registering it
	// would advertise a size-0 claim that serves nothing and, since
	// placement pins re-copies to the owning disk, would keep attracting
	// repairs to a file that was never valid. A 0-byte shard beside a
	// 0-byte index is different — that is the legitimate layout of a
	// volume encoded with no live needles, and it must keep mounting.
	// The startup scan already skips 0-byte shard files; this covers the
	// mount RPC path, which opens the file directly.
	if ecVolumeShard.Size() == 0 && ev.ecxFileSize > 0 {
		return false, fmt.Errorf("ec volume %d shard %d: shard file is empty (0 bytes) but the index has %d entries: residue of a failed copy, not a mountable shard",
			ev.VolumeId, ecVolumeShard.ShardId, ev.ecxFileSize/types.NeedleMapEntrySize)
	}
	ev.Shards = append(ev.Shards, ecVolumeShard)
	slices.SortFunc(ev.Shards, func(a, b *EcVolumeShard) int {
		if a.VolumeId != b.VolumeId {
			return int(a.VolumeId - b.VolumeId)
		}
		return int(a.ShardId - b.ShardId)
	})
	return true, nil
}

func (ev *EcVolume) DeleteEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, deleted bool) {
	foundPosition := -1
	for i, s := range ev.Shards {
		if s.ShardId == shardId {
			foundPosition = i
		}
	}
	if foundPosition < 0 {
		return nil, false
	}

	ecVolumeShard = ev.Shards[foundPosition]
	ecVolumeShard.Unmount()
	ev.Shards = append(ev.Shards[:foundPosition], ev.Shards[foundPosition+1:]...)
	return ecVolumeShard, true
}

func (ev *EcVolume) FindEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, found bool) {
	for _, s := range ev.Shards {
		if s.ShardId == shardId {
			return s, true
		}
	}
	return nil, false
}

func (ev *EcVolume) Close() {
	for _, s := range ev.Shards {
		s.Close()
	}
	ev.ecjFileAccessLock.Lock()
	if ev.ecjFile != nil {
		_ = ev.ecjFile.Close()
		ev.ecjFile = nil
	}
	if ev.ecjHold != nil {
		ev.ecjHold.release()
		ev.ecjHold = nil
	}
	ev.ecjFileAccessLock.Unlock()
	if ev.ecxFile != nil {
		_ = ev.ecxFile.Sync()
		// Do NOT nil ecxFile: LocateEcShardNeedle reads it without the
		// ecVolumesLock after the resolving lookup released it, so a concurrent
		// eviction that nils the field would race that read. A closed-but-set fd
		// yields a clean read error (recovered from parity) and no data race.
		_ = ev.ecxFile.Close()
	}
}

// Sync flushes the .ecx and .ecj files to disk without closing them.
// This ensures that deletions made via DeleteNeedleFromEcx are visible
// to other processes/file handles that may read these files.
func (ev *EcVolume) Sync() {
	ev.ecjFileAccessLock.Lock()
	if ev.ecjFile != nil {
		if err := ev.ecjFile.Sync(); err != nil {
			glog.Warningf("failed to sync ecj file for volume %d: %v", ev.VolumeId, err)
		}
	}
	ev.ecjFileAccessLock.Unlock()
	if ev.ecxFile != nil {
		if err := ev.ecxFile.Sync(); err != nil {
			glog.Warningf("failed to sync ecx file for volume %d: %v", ev.VolumeId, err)
		}
	}
}

func (ev *EcVolume) Destroy() {
	ev.Close()

	for _, s := range ev.Shards {
		s.Destroy()
	}
	// Sweep the EC-only index files from BOTH the data directory and the shared
	// index directory. A move or reconstruct can leave a copy in whichever
	// directory is not ecxActualDir; removing only the active one leaves a stale
	// index that a later reload could pick up and re-mount as a phantom EC
	// volume. .ecx/.ecj are EC-specific, so removing both copies is safe.
	for _, base := range ev.ecIndexBaseNames() {
		os.Remove(base + ".ecx")
		os.Remove(base + ".ecj")
		os.Remove(base + EcjCompactTmpExt)
	}
	// The .vif is shared with a coexisting normal volume (e.g. mid-decode), so
	// only remove the active copy, not both.
	os.Remove(ev.FileName(".vif"))
	// Remove the bitrot checksum sidecar(s) so a later volume reuse cannot load
	// stale protection. Search both the data and index bases.
	RemoveBitrotSidecars(ev.DataBaseFileName())
	if ev.IndexBaseFileName() != ev.DataBaseFileName() {
		RemoveBitrotSidecars(ev.IndexBaseFileName())
	}
}

// ecIndexBaseNames returns the base paths for the volume's EC index files in
// both the data and index directories, deduplicated when they coincide.
func (ev *EcVolume) ecIndexBaseNames() []string {
	bases := []string{ev.DataBaseFileName()}
	if ev.IndexBaseFileName() != ev.DataBaseFileName() {
		bases = append(bases, ev.IndexBaseFileName())
	}
	return bases
}

// DiskType returns the disk type the EC volume currently reports under.
// Defaults to the physical location's disk type; orchestrators can override
// it via SetDiskType so the volume keeps reporting under the source
// volume's disk type after encoding (#9423).
func (ev *EcVolume) DiskType() types.DiskType {
	return ev.diskType
}

// SetDiskType overrides the EC volume's reported disk type and propagates
// to its mounted shards. Intended for the orchestrator-driven mount path
// (VolumeEcShardsMount); not persisted across restarts.
func (ev *EcVolume) SetDiskType(d types.DiskType) {
	ev.diskType = d
	for _, s := range ev.Shards {
		s.DiskType = d
	}
}

func (ev *EcVolume) FileName(ext string) string {
	switch ext {
	case ".ecx", ".ecj":
		return EcShardFileName(ev.Collection, ev.ecxActualDir, int(ev.VolumeId)) + ext
	}
	// .vif
	return ev.DataBaseFileName() + ext
}

func (ev *EcVolume) DataBaseFileName() string {
	return EcShardFileName(ev.Collection, ev.dir, int(ev.VolumeId))
}

func (ev *EcVolume) IndexBaseFileName() string {
	return EcShardFileName(ev.Collection, ev.dirIdx, int(ev.VolumeId))
}

func (ev *EcVolume) ShardSize() uint64 {
	if len(ev.Shards) > 0 {
		return uint64(ev.Shards[0].Size())
	}
	return 0
}

// DatFileSize returns the source .dat file size as recorded in .vif at
// EC encoding time. Zero for old EC volumes whose .vif predates the
// field, or for .vif files we failed to parse. Used by the Store-level
// prune in store_ec_reconcile.go to validate that a sibling-disk .dat
// is plausibly the encoding source before deleting the partial EC.
func (ev *EcVolume) DatFileSize() int64 {
	return ev.datFileSize
}

func (ev *EcVolume) Size() (size uint64) {
	for _, shard := range ev.Shards {
		if shardSize := shard.Size(); shardSize > 0 {
			size += uint64(shardSize)
		}
	}
	return
}

func (ev *EcVolume) CreatedAt() time.Time {
	return ev.ecxCreatedAt
}

func (ev *EcVolume) ShardIdList() (shardIds []ShardId) {
	for _, s := range ev.Shards {
		shardIds = append(shardIds, s.ShardId)
	}
	return
}

func (ev *EcVolume) ToVolumeEcShardInformationMessage(diskId uint32) (messages []*master_pb.VolumeEcShardInformationMessage) {
	ecInfoPerVolume := map[needle.VolumeId]*master_pb.VolumeEcShardInformationMessage{}

	fileCount, deleteCount := ev.FileAndDeleteCount()

	for _, s := range ev.Shards {
		m, ok := ecInfoPerVolume[s.VolumeId]
		if !ok {
			m = &master_pb.VolumeEcShardInformationMessage{
				Id:          uint32(s.VolumeId),
				Collection:  s.Collection,
				DiskType:    string(ev.diskType),
				ExpireAtSec: ev.ExpireAtSec,
				DiskId:      diskId,
				FileCount:   fileCount,
				DeleteCount: deleteCount,
				EncodeTsNs:  ev.EncodeTsNs,
			}
			ecInfoPerVolume[s.VolumeId] = m
		}

		// Update EC shard bits and sizes.
		si := ShardsInfoFromVolumeEcShardInformationMessage(m)
		si.Set(NewShardInfo(s.ShardId, ShardSize(s.Size())))
		m.EcIndexBits = uint32(si.Bitmap())
		m.ShardSizes = si.SizesInt64()
	}

	for _, m := range ecInfoPerVolume {
		messages = append(messages, m)
	}
	return
}

// FileAndDeleteCount returns the current (fileCount, deleteCount) for this
// EC volume.
//
//   - fileCount = .ecx size / NeedleMapEntrySize — the total number of
//     needles recorded in the sealed sorted index. Because .ecx is written
//     at encode time and only overwritten during decode/rebuild (which
//     preserves record count), this matches the "cumulative put count"
//     semantics of regular volume FileCount.
//
//   - deleteCount = len(deletedNeedles) — the number of unique runtime
//     deletes tracked in memory. The set is seeded from .ecj on load and
//     appended to on every successful DeleteNeedleFromEcx. Because a
//     needle delete is applied on exactly one shard holder, the admin
//     aggregation sums deleteCount across nodes to get the volume's true
//     delete total.
//
// Both values are O(1) — no index walking.
func (ev *EcVolume) FileAndDeleteCount() (fileCount, deleteCount uint64) {
	fileCount = uint64(ev.ecxFileSize) / uint64(types.NeedleMapEntrySize)
	ev.deletedNeedlesLock.RLock()
	deleteCount = uint64(len(ev.deletedNeedles))
	ev.deletedNeedlesLock.RUnlock()
	return
}

// IsNeedleDeleted reports whether the given needle id is in the in-memory
// deleted set. Callers that have already looked the needle up in .ecx
// should consult this to apply runtime deletion state on top of the
// sealed index.
func (ev *EcVolume) IsNeedleDeleted(needleId types.NeedleId) bool {
	ev.deletedNeedlesLock.RLock()
	_, ok := ev.deletedNeedles[needleId]
	ev.deletedNeedlesLock.RUnlock()
	return ok
}

// markNeedleDeletedInMemory inserts a needle id into the deleted set.
func (ev *EcVolume) markNeedleDeletedInMemory(needleId types.NeedleId) {
	ev.deletedNeedlesLock.Lock()
	ev.deletedNeedles[needleId] = struct{}{}
	ev.deletedNeedlesLock.Unlock()
}

// loadDeletedNeedlesFromEcj walks the .ecj journal and populates the
// in-memory deleted set. Called once from NewEcVolume under the exclusive
// ownership of the just-constructed (and not yet shared) EcVolume.
func (ev *EcVolume) loadDeletedNeedlesFromEcj() error {
	if ev.ecjFile == nil || ev.ecjFileSize < int64(types.NeedleIdSize) {
		return nil
	}
	buf := make([]byte, ecjLoadChunkBytes)
	for off := int64(0); off+int64(types.NeedleIdSize) <= ev.ecjFileSize; {
		want := min(int64(ecjLoadChunkBytes), ev.ecjFileSize-off)
		want -= want % int64(types.NeedleIdSize)
		if want == 0 {
			break
		}
		if _, err := ev.ecjFile.ReadAt(buf[:want], off); err != nil {
			return fmt.Errorf("read ecj at %d: %w", off, err)
		}
		for i := int64(0); i+int64(types.NeedleIdSize) <= want; i += int64(types.NeedleIdSize) {
			ev.deletedNeedles[types.BytesToNeedleId(buf[i:i+types.NeedleIdSize])] = struct{}{}
		}
		off += want
	}
	return nil
}

// openEcjFile opens path as the deletion journal handle, creating it if
// absent. Both the mount and the reopen after compaction go through here, so
// the handle a compacted volume appends through behaves like the original.
func openEcjFile(path string) (*os.File, error) {
	return backend.OpenVolumeFile(path, os.O_RDWR|os.O_CREATE)
}

// ecjFsOps are the filesystem steps that publish a compacted journal.
// Production uses realEcjFsOps; tests substitute failing steps to cover the
// failure paths through the real mount.
type ecjFsOps struct {
	rename   func(oldpath, newpath string) error
	fsyncDir func(path string) error
	reopen   func(path string) (*os.File, error)
}

var realEcjFsOps = ecjFsOps{
	rename:   os.Rename,
	fsyncDir: func(p string) error { return util.FsyncDir(filepath.Dir(p)) },
	reopen:   openEcjFile,
}

// ecjHandleLostError marks a compaction failure that left the volume without a
// usable journal handle, so the mount must fail: deletes would error, or land
// in an inode no longer at the journal's path. Decided where the failure
// happens rather than inferred afterwards from ecjFile.
type ecjHandleLostError struct{ err error }

func (e *ecjHandleLostError) Error() string { return e.err.Error() }
func (e *ecjHandleLostError) Unwrap() error { return e.err }

// compactEcjAfterLoad compacts the journal unless its load failed. After a
// failed load the set holds only the part of the journal read before the
// error, and rewriting the file from it would delete the rest for good.
func (ev *EcVolume) compactEcjAfterLoad(loadErr error, ops ecjFsOps) error {
	if loadErr != nil {
		glog.Warningf("ec volume %d: not compacting .ecj: its load failed, so the in-memory set may be partial", ev.VolumeId)
		return nil
	}
	return ev.maybeCompactEcj(ops)
}

// maybeCompactEcj rewrites a bloated .ecj from the set just loaded out of it.
//
// The journal is semantically a SET of deleted needle ids, written as an
// append-only log that nothing dedupes. VolumeEcShardsCopy, EC index recovery
// and ec_decode's merge append a peer's whole journal onto this one, so a
// volume whose shards are balanced back and forth grows the file
// geometrically (1.51 TB for ~100 distinct ids in production).
//
// Compaction is safe because the set IS the journal's meaning, provided
// nothing else can write the file between the load and the rename. The
// ecj_registry reservation and the on-disk re-check establish that: this
// volume is the only holder of the path, no copy is writing to it, and the file
// is still the inode and size that was loaded.
//
// Returns an error only when the volume is left without a usable journal
// handle, which must fail the mount. Every other failure leaves the original
// journal in place and is logged here.
func (ev *EcVolume) maybeCompactEcj(ops ecjFsOps) error {
	ecjPath := ev.FileName(".ecj")
	tmpPath := EcShardFileName(ev.Collection, ev.ecxActualDir, int(ev.VolumeId)) + EcjCompactTmpExt
	wanted := ev.ecjNeedsCompaction()
	_, tmpStatErr := os.Stat(tmpPath)
	staleTmp := tmpStatErr == nil
	if (!wanted && !staleTmp) || ev.ecjHold == nil {
		return nil
	}
	end, ok := ev.ecjHold.tryBeginCompaction()
	if !ok {
		glog.V(1).Infof("ec volume %d: skipping .ecj compaction: another holder or a copy can reach %s", ev.VolumeId, ecjPath)
		return nil
	}
	defer end()
	// A tmp left by a crash between its write and the rename. Removed under the
	// reservation, so it cannot be another holder's compaction in flight.
	if staleTmp {
		_ = os.Remove(tmpPath)
	}
	if !wanted {
		return nil
	}
	return ev.compactEcjReserved(ecjPath, tmpPath, ops)
}

// compactEcjReserved is the part of maybeCompactEcj that runs under the path
// reservation: re-check the file, write the compacted tmp, publish it. Same
// error contract: an error only when no usable journal handle is left.
func (ev *EcVolume) compactEcjReserved(ecjPath, tmpPath string, ops ecjFsOps) error {
	unchanged, err := ev.ecjUnchangedSinceLoad(ecjPath)
	if err != nil {
		glog.Warningf("ec volume %d: compact .ecj: stat journal: %v", ev.VolumeId, err)
		return nil
	}
	if !unchanged {
		glog.Warningf("ec volume %d: skipping .ecj compaction: %s changed on disk after it was loaded", ev.VolumeId, ecjPath)
		return nil
	}
	ids := ev.sortedDeletedIds()
	glog.Warningf("ec volume %d: compacting bloated .ecj deletion journal on-disk=%d unique=%d compacted=%d",
		ev.VolumeId, ev.ecjFileSize, len(ids), len(ids)*types.NeedleIdSize)
	if err := writeCompactedEcjTmp(tmpPath, ids); err != nil {
		// A partial tmp would pin its bytes, and the likeliest cause here is
		// ENOSPC, where those bytes are exactly what is scarce.
		_ = os.Remove(tmpPath)
		glog.Warningf("ec volume %d: compact .ecj: write compacted journal: %v", ev.VolumeId, err)
		return nil
	}
	if err := ev.publishCompactedEcj(ecjPath, tmpPath, ops); err != nil {
		var lost *ecjHandleLostError
		if errors.As(err, &lost) {
			return err
		}
		glog.Warningf("ec volume %d: compact .ecj: journal left as it was: %v", ev.VolumeId, err)
	}
	return nil
}

// ecjNeedsCompaction reports whether the loaded journal is bloated enough to
// rewrite: file_bytes >= max(ecjCompactMinBytes, ecjCompactRatio * set_bytes).
// The 1 MiB floor keeps a small healthy journal from ever being rewritten. It
// reads only the set's length, so the common no-op mount copies nothing.
func (ev *EcVolume) ecjNeedsCompaction() bool {
	ev.deletedNeedlesLock.RLock()
	distinct := int64(len(ev.deletedNeedles))
	ev.deletedNeedlesLock.RUnlock()
	compactedLen := distinct * int64(types.NeedleIdSize)
	return ev.ecjFileSize >= ecjCompactMinBytes && ev.ecjFileSize >= compactedLen*ecjCompactRatio
}

// sortedDeletedIds returns the deleted set sorted, so the rewritten file is
// deterministic and two holders compacting one set write identical bytes.
func (ev *EcVolume) sortedDeletedIds() []types.NeedleId {
	ev.deletedNeedlesLock.RLock()
	ids := make([]types.NeedleId, 0, len(ev.deletedNeedles))
	for id := range ev.deletedNeedles {
		ids = append(ids, id)
	}
	ev.deletedNeedlesLock.RUnlock()
	slices.Sort(ids)
	return ids
}

// ecjUnchangedSinceLoad reports whether the file at ecjPath is still the one
// this volume loaded: the same file as the open handle and the size the set
// was read from. A copy that appended, or replaced the file, after the load
// fails this, and compacting then would drop what it wrote.
func (ev *EcVolume) ecjUnchangedSinceLoad(ecjPath string) (bool, error) {
	if ev.ecjFile == nil {
		return false, nil
	}
	held, err := ev.ecjFile.Stat()
	if err != nil {
		return false, err
	}
	onPath, err := os.Stat(ecjPath)
	if err != nil {
		return false, err
	}
	return os.SameFile(held, onPath) && held.Size() == onPath.Size() && onPath.Size() == ev.ecjFileSize, nil
}

// writeCompactedEcjTmp writes sorted ids to tmpPath through a buffer and
// fsyncs it. Opened like every other volume file, so the journal it becomes
// has the same mode and open flags as one that was never compacted.
func writeCompactedEcjTmp(tmpPath string, ids []types.NeedleId) error {
	f, err := backend.OpenVolumeFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, ecjLoadChunkBytes)
	var rec [types.NeedleIdSize]byte
	for _, id := range ids {
		types.NeedleIdToBytes(rec[:], id)
		if _, err := w.Write(rec[:]); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// publishCompactedEcj replaces the live journal with the compacted tmp file:
// drop the handle (Windows cannot rename over an open file), rename, fsync the
// directory, reopen the handle.
//
// A failed rename publishes nothing: the tmp is removed and the handle to the
// original journal restored, and the rename error is returned as is. If that
// restore fails too, or anything fails after the rename, the volume has no
// usable handle and the error is an *ecjHandleLostError carrying every error
// involved.
func (ev *EcVolume) publishCompactedEcj(ecjPath, tmpPath string, ops ecjFsOps) error {
	if ev.ecjFile != nil {
		_ = ev.ecjFile.Close()
		ev.ecjFile = nil
	}
	if renameErr := ops.rename(tmpPath, ecjPath); renameErr != nil {
		_ = os.Remove(tmpPath)
		reopened, reopenErr := ops.reopen(ecjPath)
		if reopenErr != nil {
			return &ecjHandleLostError{fmt.Errorf("rename %s over %s: %w; reopening the original journal then failed: %w",
				tmpPath, ecjPath, renameErr, reopenErr)}
		}
		ev.ecjFile = reopened
		return renameErr
	}
	lost := func(what string, err error) error {
		return &ecjHandleLostError{fmt.Errorf("replaced %s but could not %s: %w", ecjPath, what, err)}
	}
	// The tmp sync persisted contents, not the directory entry. Without this a
	// power loss can restore the old journal and discard deletes acknowledged
	// against the replacement.
	if err := ops.fsyncDir(ecjPath); err != nil {
		return lost("fsync its directory", err)
	}
	reopened, err := ops.reopen(ecjPath)
	if err != nil {
		return lost("reopen it", err)
	}
	fi, err := reopened.Stat()
	if err != nil {
		_ = reopened.Close()
		return lost("stat it", err)
	}
	ev.ecjFile = reopened
	ev.ecjFileSize = fi.Size()
	return nil
}

func (ev *EcVolume) LocateEcShardNeedle(needleId types.NeedleId, version needle.Version) (offset types.Offset, size types.Size, intervals []Interval, err error) {

	// find the needle from ecx file
	offset, size, err = ev.FindNeedleFromEcx(needleId)
	if err != nil {
		return types.Offset{}, 0, nil, fmt.Errorf("FindNeedleFromEcx: %w", err)
	}

	intervals = ev.LocateEcShardNeedleInterval(version, offset.ToActualOffset(), types.Size(needle.GetActualSize(size, version)))
	return
}

func (ev *EcVolume) LocateEcShardNeedleInterval(version needle.Version, offset int64, size types.Size) (intervals []Interval) {
	shard := ev.Shards[0]
	var shardSize int64
	if ev.datFileSize > 0 {
		// Use datFileSize to calculate the shardSize to match the EC encoding logic.
		// This is the authoritative value stored in .vif during EC encoding.
		shardSize = ev.datFileSize / int64(ev.ECContext.DataShards)
	} else {
		// Fallback for old EC volumes without datFileSize in .vif.
		// Subtract 1 to handle the ambiguous case where ecdFileSize is an exact
		// multiple of ErasureCodingLargeBlockSize but the data is actually in small
		// blocks (e.g., datFileSize was just under DataShards*ErasureCodingLargeBlockSize).
		shardSize = shard.ecdFileSize - 1
	}
	// calculate the locations in the ec shards
	intervals = LocateData(ev.ECContext.LargeBlockSize(), ev.ECContext.SmallBlockSize(), shardSize, offset, types.Size(needle.GetActualSize(size, version)))

	return
}

// IntervalToShardIdAndOffset resolves an interval against this volume's shard
// block layout.
func (ev *EcVolume) IntervalToShardIdAndOffset(interval Interval) (ShardId, int64) {
	return interval.ToShardIdAndOffset(ev.ECContext.LargeBlockSize(), ev.ECContext.SmallBlockSize())
}

func (ev *EcVolume) FindNeedleFromEcx(needleId types.NeedleId) (offset types.Offset, size types.Size, err error) {
	offset, size, err = SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, nil)
	if err != nil {
		ev.CheckReadWriteError(err)
		return
	}
	ev.CheckReadWriteError(nil)
	if ev.IsNeedleDeleted(needleId) {
		size = types.TombstoneFileSize
	}
	return
}

func SearchNeedleFromSortedIndex(ecxFile *os.File, ecxFileSize int64, needleId types.NeedleId, processNeedleFn func(file *os.File, offset int64) error) (offset types.Offset, size types.Size, err error) {
	var key types.NeedleId
	buf := make([]byte, types.NeedleMapEntrySize)
	l, h := int64(0), ecxFileSize/types.NeedleMapEntrySize
	for l < h {
		m := (l + h) / 2
		if n, err := ecxFile.ReadAt(buf, m*types.NeedleMapEntrySize); err != nil {
			if n != types.NeedleMapEntrySize {
				return types.Offset{}, types.TombstoneFileSize, fmt.Errorf("ecx file %d read at %d: %w", ecxFileSize, m*types.NeedleMapEntrySize, err)
			}
		}
		key, offset, size = idx.IdxFileEntry(buf)
		if key == needleId {
			if processNeedleFn != nil {
				err = processNeedleFn(ecxFile, m*types.NeedleMapEntrySize)
			}
			return
		}
		if key < needleId {
			l = m + 1
		} else {
			h = m
		}
	}

	err = NotFoundError
	return
}

func (ev *EcVolume) IsTimeToDestroy() bool {
	return ev.ExpireAtSec > 0 && time.Now().Unix() > (int64(ev.ExpireAtSec)+destroyDelaySeconds)
}

func (ev *EcVolume) WalkIndex(processNeedleFn func(key types.NeedleId, offset types.Offset, size types.Size) error) error {
	if ev.ecxFile == nil {
		return fmt.Errorf("no ECX file associated with EC volume %v", ev.VolumeId)
	}
	return idx.WalkIndexFile(ev.ecxFile, 0, processNeedleFn)
}
