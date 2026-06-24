package filersink

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/security"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ChunkTransferSnapshot is a lock-free, copyable view of a chunk transfer's
// progress, returned by ActiveTransfers.
type ChunkTransferSnapshot struct {
	ChunkFileId   string
	Path          string
	BytesReceived int64
	Status        string // "downloading", "uploading", or "waiting 10s" etc.
	LastErr       string
}

// ChunkTransferStatus tracks the progress of a single chunk being replicated.
// Fields are guarded by mu: ChunkFileId and Path are immutable after creation,
// while BytesReceived, Status, and LastErr are updated by fetchAndWrite and
// read by ActiveTransfers.
type ChunkTransferStatus struct {
	mu sync.RWMutex
	ChunkTransferSnapshot
}

type FilerSink struct {
	filerSource       *source.FilerSource
	grpcAddress       string
	dir               string
	replication       string
	collection        string
	ttlSec            int32
	diskType          string
	dataCenter        string
	grpcDialOption    grpc.DialOption
	address           string
	writeChunkByFiler bool
	isIncremental     bool
	executor          *util.LimitedConcurrentExecutor
	signature         int32
	activeTransfers   sync.Map // chunkFileId -> *ChunkTransferStatus
	uploader          *operation.Uploader
}

func init() {
	sink.Sinks = append(sink.Sinks, &FilerSink{})
}

func (fs *FilerSink) GetName() string {
	return "filer"
}

func (fs *FilerSink) GetSinkToDirectory() string {
	return fs.dir
}

func (fs *FilerSink) IsIncremental() bool {
	return fs.isIncremental
}

func (fs *FilerSink) Initialize(configuration util.Configuration, prefix string) error {
	fs.isIncremental = configuration.GetBool(prefix + "is_incremental")
	fs.dataCenter = configuration.GetString(prefix + "dataCenter")
	fs.signature = util.RandomInt32()
	return fs.DoInitialize(
		"",
		configuration.GetString(prefix+"grpcAddress"),
		configuration.GetString(prefix+"directory"),
		configuration.GetString(prefix+"replication"),
		configuration.GetString(prefix+"collection"),
		configuration.GetInt(prefix+"ttlSec"),
		configuration.GetString(prefix+"disk"),
		security.LoadClientTLS(util.GetViper(), "grpc.client"),
		false)
}

func (fs *FilerSink) SetSourceFiler(s *source.FilerSource) {
	fs.filerSource = s
}

// SetUploader sets a custom uploader for this sink, used when the target
// cluster requires different TLS certificates than the global config.
// Must be called during initialization, before any replication goroutines
// start, since it writes fs.uploader without synchronization.
func (fs *FilerSink) SetUploader(uploader *operation.Uploader) {
	fs.uploader = uploader
}

func (fs *FilerSink) getUploader() (*operation.Uploader, error) {
	if fs.uploader != nil {
		return fs.uploader, nil
	}
	return operation.NewUploader()
}

func (fs *FilerSink) DoInitialize(address, grpcAddress string, dir string,
	replication string, collection string, ttlSec int, diskType string, grpcDialOption grpc.DialOption, writeChunkByFiler bool) (err error) {
	fs.address = address
	if fs.address == "" {
		fs.address = pb.GrpcAddressToServerAddress(grpcAddress)
	}
	fs.grpcAddress = grpcAddress
	fs.dir = dir
	fs.replication = replication
	fs.collection = collection
	fs.ttlSec = int32(ttlSec)
	fs.diskType = diskType
	fs.grpcDialOption = grpcDialOption
	fs.writeChunkByFiler = writeChunkByFiler
	fs.executor = util.NewLimitedConcurrentExecutor(32)
	return nil
}

// SetChunkConcurrency replaces the chunk replication executor with one using the
// given concurrency limit. Must be called during initialization, before any
// replication goroutines start, since it replaces fs.executor without
// synchronization.
func (fs *FilerSink) SetChunkConcurrency(concurrency int) {
	if concurrency > 0 {
		fs.executor = util.NewLimitedConcurrentExecutor(concurrency)
	}
}

// ActiveTransfers returns an immutable snapshot of all in-progress chunk transfers.
func (fs *FilerSink) ActiveTransfers() []ChunkTransferSnapshot {
	var transfers []ChunkTransferSnapshot
	fs.activeTransfers.Range(func(key, value any) bool {
		t := value.(*ChunkTransferStatus)
		t.mu.RLock()
		transfers = append(transfers, t.ChunkTransferSnapshot)
		t.mu.RUnlock()
		return true
	})
	return transfers
}

func (fs *FilerSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {

	dir, name := util.FullPath(key).DirAndName()

	glog.V(4).Infof("delete entry: %v", key)
	err := filer_pb.Remove(context.Background(), fs, dir, name, deleteIncludeChunks, true, true, true, signatures)
	if err != nil {
		glog.V(0).Infof("delete entry %s: %v", key, err)
		return fmt.Errorf("delete entry %s: %v", key, err)
	}
	return nil
}

var _ sink.EntryMover = (*FilerSink)(nil)

// MoveEntry relocates oldKey to newKey on the target filer via AtomicRenameEntry:
// a metadata-only move that relocates a whole subtree in one transaction, so a
// directory rename never leaves descendants missing and chunks are neither
// re-copied nor leaked.
//
// When the move fails because the old path is genuinely gone on the sink — a
// descendant the parent rename already relocated, or one never replicated —
// there is nothing to move, so it creates the new path instead (CreateEntry
// short-circuits when the entry is already there, and never deletes). Existence
// is re-checked with a direct lookup rather than inferred from the rename error,
// so a rolled-back move that left the old entry intact propagates for retry
// instead of being mistaken for "gone".
func (fs *FilerSink) MoveEntry(oldKey, newKey string, newEntry *filer_pb.Entry, signatures []int32) error {
	oldDir, oldName := util.FullPath(oldKey).DirAndName()
	newDir, newName := util.FullPath(newKey).DirAndName()

	err := fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: oldDir,
			OldName:      oldName,
			NewDirectory: newDir,
			NewName:      newName,
			Signatures:   signatures,
		})
		return err
	})
	if err == nil {
		return nil
	}
	if missing, lookupErr := fs.entryMissing(oldKey); lookupErr == nil && missing {
		glog.V(2).Infof("move %s => %s: old path gone, creating %s", oldKey, newKey, newKey)
		return fs.CreateEntry(newKey, newEntry, signatures)
	}
	return fmt.Errorf("move %s => %s: %w", oldKey, newKey, err)
}

// entryMissing reports whether key has no entry on the target filer. A lookup
// not-found (sentinel or the gRPC string form) means missing; any other lookup
// error is returned so the caller does not treat an unknown state as missing.
func (fs *FilerSink) entryMissing(key string) (bool, error) {
	dir, name := util.FullPath(key).DirAndName()
	missing := false
	err := fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, lookupErr := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lookupErr == nil {
			return nil
		}
		// The string check is a deliberate compatibility fallback: a cross-cluster
		// gRPC error often arrives as a plain status string that no longer wraps the
		// filer_pb.ErrNotFound sentinel, so errors.Is alone would miss it.
		if errors.Is(lookupErr, filer_pb.ErrNotFound) || strings.Contains(lookupErr.Error(), filer_pb.ErrNotFound.Error()) {
			missing = true
			return nil
		}
		return lookupErr
	})
	return missing, err
}

func (fs *FilerSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {

	return fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		dir, name := util.FullPath(key).DirAndName()

		// look up existing entry
		lookupRequest := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}
		// glog.V(1).Infof("lookup: %v", lookupRequest)
		if resp, err := filer_pb.LookupEntry(context.Background(), client, lookupRequest); err == nil {
			if destinationSatisfiesSource(resp.Entry, entry) {
				glog.V(3).Infof("skip overwriting %s", key)
				return nil
			}
			glog.Warningf("re-replicating %s: destination not current (dst %d chunks, src %d chunks)",
				key, len(resp.Entry.GetChunks()), len(entry.GetChunks()))
		}

		replicatedChunks, err := fs.replicateChunks(context.Background(), entry.GetChunks(), key, getEntryMtimeNs(entry))

		if err != nil {
			// don't swallow: committing mismatched bytes would propagate corruption
			if errors.Is(err, errChunkSizeMismatch) {
				return fs.onCorruptChunk(key, entry, err)
			}
			glog.Warningf("replicate entry chunks %s: %v", key, err)
			return nil
		}

		// glog.V(4).Infof("replicated %s %+v ===> %+v", key, entry.GetChunks(), replicatedChunks)

		request := &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: entry.IsDirectory,
				Attributes:  entry.Attributes,
				Extended:    entry.Extended,
				Chunks:      replicatedChunks,
				Content:     entry.Content,
				RemoteEntry: entry.RemoteEntry,
			},
			IsFromOtherCluster: true,
			Signatures:         signatures,
		}

		glog.V(3).Infof("create: %v", request)
		if err := filer_pb.CreateEntry(context.Background(), client, request); err != nil {
			glog.V(0).Infof("create entry %s: %v", key, err)
			return fmt.Errorf("create entry %s: %v", key, err)
		}

		return nil
	})
}

func (fs *FilerSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {

	dir, name := util.FullPath(key).DirAndName()

	// read existing entry
	var existingEntry *filer_pb.Entry
	err = fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		glog.V(4).Infof("lookup entry: %v", request)
		resp, err := filer_pb.LookupEntry(context.Background(), client, request)
		if err != nil {
			glog.V(0).Infof("lookup %s: %v", key, err)
			return err
		}

		existingEntry = resp.Entry

		return nil
	})

	if err != nil {
		return false, fmt.Errorf("lookup %s: %v", key, err)
	}

	glog.V(4).Infof("oldEntry %+v, newEntry %+v, existingEntry: %+v", oldEntry, newEntry, existingEntry)

	// the supersession checks below must look up where the incoming entry lives
	// now, which for a rename is newParentPath/newEntry.Name, not the old key.
	targetKey := updatedEntryKey(key, newParentPath, newEntry)

	switch chooseUpdateAction(existingEntry, oldEntry, newEntry) {
	case updateSkip:
		// a newer, complete version already landed (out-of-order); leave it
		glog.V(2).Infof("late updates %s", key)
		return true, nil
	case updateRepair:
		glog.Warningf("repair %s: destination diverged from source (dst %d chunks, src %d chunks); re-replicating full source content",
			key, len(existingEntry.GetChunks()), len(newEntry.GetChunks()))
		replicatedChunks, err := fs.replicateChunks(context.Background(), newEntry.GetChunks(), targetKey, getEntryMtimeNs(newEntry))
		if err != nil {
			if errors.Is(err, errChunkSizeMismatch) {
				return true, fs.onCorruptChunk(targetKey, newEntry, err)
			}
			glog.Warningf("replicate entry chunks %s: %v", key, err)
			return true, nil
		}
		existingEntry.Chunks = replicatedChunks
		existingEntry.Attributes = newEntry.Attributes
		existingEntry.Extended = newEntry.Extended
		existingEntry.HardLinkId = newEntry.HardLinkId
		existingEntry.HardLinkCounter = newEntry.HardLinkCounter
		existingEntry.Content = newEntry.Content
		existingEntry.RemoteEntry = newEntry.RemoteEntry
	default:
		// source-side chunks resolve via source filer; sink volume IDs may collide.
		deletedChunks, newChunks, err := compareChunks(context.Background(), filer.LookupFn(fs.filerSource), oldEntry, newEntry)
		if err != nil {
			return true, fmt.Errorf("replicate %s compare chunks error: %v", key, err)
		}

		// delete the chunks that are deleted from the source
		if deleteIncludeChunks {
			// remove the deleted chunks. Actual data deletion happens in filer UpdateEntry FindUnusedFileChunks
			existingEntry.Chunks = filer.DoMinusChunksBySourceFileId(existingEntry.GetChunks(), deletedChunks)
		}

		// replicate the chunks that are new in the source
		replicatedChunks, err := fs.replicateChunks(context.Background(), newChunks, targetKey, getEntryMtimeNs(newEntry))
		if err != nil {
			if errors.Is(err, errChunkSizeMismatch) {
				return true, fs.onCorruptChunk(targetKey, newEntry, err)
			}
			glog.Warningf("replicate entry chunks %s: %v", key, err)
			return true, nil
		}
		existingEntry.Chunks = append(existingEntry.GetChunks(), replicatedChunks...)
		existingEntry.Attributes = newEntry.Attributes
		existingEntry.Extended = newEntry.Extended
		existingEntry.HardLinkId = newEntry.HardLinkId
		existingEntry.HardLinkCounter = newEntry.HardLinkCounter
		existingEntry.Content = newEntry.Content
		existingEntry.RemoteEntry = newEntry.RemoteEntry
	}

	// save updated meta data
	return true, fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory:          newParentPath,
			Entry:              existingEntry,
			IsFromOtherCluster: true,
			Signatures:         signatures,
		}

		if _, err := client.UpdateEntry(context.Background(), request); err != nil {
			return fmt.Errorf("update existingEntry %s: %v", key, err)
		}

		return nil
	})

}
func compareChunks(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, oldEntry, newEntry *filer_pb.Entry) (deletedChunks, newChunks []*filer_pb.FileChunk, err error) {
	aData, aMeta, aErr := filer.ResolveChunkManifest(ctx, lookupFileIdFn, oldEntry.GetChunks(), 0, math.MaxInt64)
	if aErr != nil {
		return nil, nil, aErr
	}
	bData, bMeta, bErr := filer.ResolveChunkManifest(ctx, lookupFileIdFn, newEntry.GetChunks(), 0, math.MaxInt64)
	if bErr != nil {
		return nil, nil, bErr
	}

	deletedChunks = append(deletedChunks, filer.DoMinusChunks(aData, bData)...)
	deletedChunks = append(deletedChunks, filer.DoMinusChunks(aMeta, bMeta)...)

	newChunks = append(newChunks, filer.DoMinusChunks(bData, aData)...)
	newChunks = append(newChunks, filer.DoMinusChunks(bMeta, aMeta)...)

	return
}

// getEntryMtimeNs returns the mtime at full nanosecond precision so versions
// rewritten within the same second can be ordered. int64 ns is safe until ~2262.
func getEntryMtimeNs(entry *filer_pb.Entry) int64 {
	if entry == nil || entry.Attributes == nil {
		return 0
	}
	return entry.Attributes.Mtime*int64(1e9) + int64(entry.Attributes.MtimeNs)
}

// onCorruptChunk handles a permanent chunk size-mismatch (fetched source bytes
// disagree with source metadata). If the source already holds a newer version the
// event is a stale replay whose chunk was overwritten and GC'd — skip it (lossless:
// meta events are full snapshots, so a later event re-carries the current chunks).
// Otherwise the live version is corrupt, so return the error and halt loudly.
func (fs *FilerSink) onCorruptChunk(key string, entry *filer_pb.Entry, err error) error {
	if fs.hasSourceNewerVersion(key, getEntryMtimeNs(entry)) {
		glog.Warningf("skip stale entry %s with superseded corrupt chunk: %v", key, err)
		return nil
	}
	glog.Errorf("refuse to replicate entry with corrupt chunk %s: %v", key, err)
	return err
}

// updateAction decides how UpdateEntry reconciles an incoming source version with
// the destination's current entry.
type updateAction int

const (
	// updateNormal applies the source diff: destination at or behind the source.
	updateNormal updateAction = iota
	// updateSkip leaves the destination untouched: a newer complete version already landed.
	updateSkip
	// updateRepair re-replicates full source over a destination strictly shorter than
	// the source despite a newer mtime — a truncated copy from a failed replication.
	updateRepair
)

// updatedEntryKey returns the sink path the incoming entry lands at: for a rename
// newParentPath/newEntry.Name, else key's parent + name. Supersession must use this,
// not the pre-rename key, which would look deleted on the source and skip a live event.
func updatedEntryKey(key, newParentPath string, newEntry *filer_pb.Entry) string {
	targetDir := newParentPath
	if targetDir == "" {
		targetDir, _ = util.FullPath(key).DirAndName()
	}
	return string(util.NewFullPath(targetDir, newEntry.Name))
}

func chooseUpdateAction(existing, oldEntry, incoming *filer_pb.Entry) updateAction {
	exNs, inNs := getEntryMtimeNs(existing), getEntryMtimeNs(incoming)
	if exNs > inNs {
		// Destination is strictly newer: incoming is a stale/out-of-order replay of
		// an older source version, so never roll the destination back to it — even
		// when the newer destination is smaller (a legitimate truncate). A newer copy
		// that is itself incomplete is reconciled by -initialSnapshot, not by an
		// older event.
		return updateSkip
	}
	// Destination at or behind incoming. updateNormal applies the incremental
	// oldEntry->incoming diff, valid only when the destination already holds
	// oldEntry's chunks. A cheap, allocation-free positional identity match proves
	// that for the common (in-sync) path; only when it fails — divergence, chunk
	// reordering, or an out-of-band destination — do we run the precise O(n log n)
	// checks.
	if existing != nil && oldEntry != nil && !destinationMatchesReference(existing, oldEntry) {
		// Truncated/gapped: the destination does not cover every byte range oldEntry
		// holds. Range containment (not a scalar byte count, attr FileSize, or ETag)
		// catches it for any destination, and is safe at equal mtime since a complete
		// destination always covers oldEntry.
		if !coversReference(existing, oldEntry) {
			return updateRepair
		}
		// Stale superseded chunk at full coverage: a chunk this backup wrote whose
		// source oldEntry no longer references. Restricted to backup-written chunks
		// (out-of-band rsync/direct chunks are ignored) AND to a strictly-older
		// destination — at equal mtime same-second versions cannot be ordered, so
		// defer rather than risk a rollback (reliable ordering is a separate fix).
		if exNs < inNs && hasStaleBackupChunk(existing, oldEntry) {
			return updateRepair
		}
	}
	return updateNormal
}

// destinationSatisfiesSource reports whether an existing destination entry already
// satisfies the incoming source entry, so CreateEntry can skip the write. Coverage
// is checked first: a chunk-truncated destination (covering fewer bytes than the
// source) must be re-replicated even when its attr.Md5-backed ETag still matches
// the source, so ETag equality must never bypass the coverage check. A destination
// that is at least as complete is skippable when it has identical content (ETag) or
// is a newer-or-equal version.
func destinationSatisfiesSource(existing, incoming *filer_pb.Entry) bool {
	if existing == nil {
		return false
	}
	exNs, inNs := getEntryMtimeNs(existing), getEntryMtimeNs(incoming)
	if exNs > inNs {
		return true // destination strictly newer: incoming is older → keep destination
	}
	if !destinationMatchesReference(existing, incoming) {
		if !coversReference(existing, incoming) {
			return false // missing a byte range the source covers (truncated/gapped)
		}
		if exNs < inNs && hasStaleBackupChunk(existing, incoming) {
			return false // a provably-older destination holds a superseded backup chunk
		}
	}
	if filer.ETag(existing) == filer.ETag(incoming) {
		return true // identical content
	}
	return exNs >= inNs // equal mtime → keep (deferred); strictly older → re-replicate
}

// destinationMatchesReference is a cheap, allocation-free sufficient check that the
// destination already holds exactly the reference's chunks: equal count and, in
// order, each destination chunk was replicated from the corresponding reference
// chunk (SourceFileId == reference FileId) at the same byte range (Offset/Size).
// True ⇒ full coverage and no stale chunk, so updateNormal/skip is safe and the
// O(n log n) range and identity checks can be skipped — the common in-sync path.
// False is never a false positive: a reordered, diverged, or out-of-band
// (rsync/direct) destination just falls back to the precise checks.
func destinationMatchesReference(existing, reference *filer_pb.Entry) bool {
	ex, ref := existing.GetChunks(), reference.GetChunks()
	if len(ex) != len(ref) {
		return false
	}
	for i := range ref {
		if ex[i].SourceFileId == "" || ex[i].SourceFileId != ref[i].GetFileIdString() {
			return false
		}
		// Require the same byte range too. replicateOneChunk copies the source
		// chunk's Offset/Size verbatim, so SourceFileId identity already implies an
		// identical range today; verifying it here keeps this fast path sound even
		// if replication ever re-chunks (split/coalesce), instead of silently
		// depending on that invariant from another file.
		if ex[i].Offset != ref[i].Offset || ex[i].Size != ref[i].Size {
			return false
		}
	}
	return true
}

// hasStaleBackupChunk reports whether existing holds a chunk that this backup
// wrote (SourceFileId set) whose source chunk `reference` no longer references.
// That means an overwrite/deletion event was missed: the destination still carries
// the superseded source chunk's bytes even though total coverage looks full, so the
// incremental diff cannot fix it. Chunks without SourceFileId (seeded out-of-band,
// e.g. rsync or a direct write) are ignored, so such destinations keep their normal
// path and are never re-copied on this signal.
func hasStaleBackupChunk(existing, reference *filer_pb.Entry) bool {
	if existing == nil || reference == nil {
		return false
	}
	refFids := make(map[string]bool, len(reference.GetChunks()))
	for _, c := range reference.GetChunks() {
		refFids[c.GetFileIdString()] = true
	}
	for _, c := range existing.GetChunks() {
		if c.SourceFileId == "" {
			continue // not backup-written → do not judge staleness
		}
		if !refFids[c.SourceFileId] {
			return true
		}
	}
	return false
}

// mergedIntervals returns the entry's chunk byte ranges, sorted and merged
// (overlapping or touching ranges combined). Chunk manifests are counted by their
// own offset/size span, which already covers the range they wrap, so no manifest
// resolution is needed.
func mergedIntervals(entry *filer_pb.Entry) [][2]int64 {
	if entry == nil {
		return nil
	}
	chunks := entry.GetChunks()
	if len(chunks) == 0 {
		return nil
	}
	ivs := make([][2]int64, 0, len(chunks))
	for _, c := range chunks {
		ivs = append(ivs, [2]int64{int64(c.Offset), int64(c.Offset) + int64(c.Size)})
	}
	sort.Slice(ivs, func(i, j int) bool { return ivs[i][0] < ivs[j][0] })
	merged := make([][2]int64, 0, len(ivs))
	for _, iv := range ivs {
		if n := len(merged); n > 0 && iv[0] <= merged[n-1][1] {
			if iv[1] > merged[n-1][1] {
				merged[n-1][1] = iv[1]
			}
			continue
		}
		merged = append(merged, iv)
	}
	return merged
}

// coversReference reports whether existing's chunks cover every byte range that
// reference's chunks cover. Equal total coverage is not sufficient proof — extra
// chunks at other offsets can hide a missing range — so containment is verified
// range by range against the merged interval sets.
func coversReference(existing, reference *filer_pb.Entry) bool {
	ex := mergedIntervals(existing)
	i := 0
	for _, r := range mergedIntervals(reference) {
		lo, hi := r[0], r[1]
		for lo < hi {
			for i < len(ex) && ex[i][1] <= lo {
				i++
			}
			if i >= len(ex) || ex[i][0] > lo {
				return false // a byte in reference is not covered by existing
			}
			lo = ex[i][1]
		}
	}
	return true
}
