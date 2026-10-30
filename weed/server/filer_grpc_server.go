package weed_server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// fencedFindEntry reads an entry with an exact log-position fence: the stamp
// and the read share the path lock mutations hold across write and notify, so
// every event at or below the fence is in the entry and none above it are.
// Any read whose result a client versions must go through here.
func (fs *FilerServer) fencedFindEntry(ctx context.Context, path util.FullPath) (entry *filer.Entry, logTsNs int64, err error) {
	pathLock := fs.entryLockTable.AcquireLock("fencedFindEntry", path, util.SharedLock)
	logTsNs = time.Now().UnixNano()
	entry, err = fs.filer.FindEntry(ctx, path)
	fs.entryLockTable.ReleaseLock(path, pathLock)
	return
}

func (fs *FilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {

	glog.V(4).InfofCtx(ctx, "LookupDirectoryEntry %s", filepath.Join(req.Directory, req.Name))

	entry, logTsNs, err := fs.fencedFindEntry(ctx, util.JoinPath(req.Directory, req.Name))

	if err == filer_pb.ErrNotFound {
		return &filer_pb.LookupDirectoryEntryResponse{LogTsNs: logTsNs, LogSignature: fs.filer.Signature}, err
	}
	if err != nil {
		glog.V(3).InfofCtx(ctx, "LookupDirectoryEntry %s: %+v, ", filepath.Join(req.Directory, req.Name), err)
		return nil, err
	}

	return &filer_pb.LookupDirectoryEntryResponse{
		Entry:        entry.ToProtoEntry(),
		LogTsNs:      logTsNs,
		LogSignature: fs.filer.Signature,
	}, nil
}

func (fs *FilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) (err error) {

	glog.V(4).Infof("ListEntries %v", req)

	limit := int(req.Limit)
	if limit == 0 {
		limit = fs.option.DirListingLimit
	}

	paginationLimit := filer.PaginationSize
	if limit < paginationLimit {
		paginationLimit = limit
	}

	lastFileName := req.StartFromFileName
	includeLastFile := req.InclusiveStartFrom
	snapshotTsNs := req.SnapshotTsNs
	if snapshotTsNs == 0 {
		snapshotTsNs = time.Now().UnixNano()
	}
	sentSnapshot := false
	var listErr error
	for limit > 0 {
		var hasEntries bool
		lastFileName, listErr = fs.filer.StreamListDirectoryEntries(stream.Context(), util.FullPath(req.Directory), lastFileName, includeLastFile, int64(paginationLimit), req.Prefix, "", "", func(entry *filer.Entry) (bool, error) {
			hasEntries = true
			pbEntry := entry.ToProtoEntry()
			if req.OmitChunks {
				// Stamp the size before dropping the only other thing carrying
				// it. Most stores fold the chunk extents into FileSize when they
				// decode, but one that keeps entries as JSON rather than as an
				// encoded Entry never re-derives it, and the caller would be
				// left with a zero. The entries are still read whole, because
				// expiring one here deletes its data and that needs the chunks.
				if pbEntry.Attributes != nil {
					pbEntry.Attributes.FileSize = filer.FileSize(pbEntry)
				}
				pbEntry.Chunks = nil
			}
			resp := &filer_pb.ListEntriesResponse{
				Entry: pbEntry,
			}
			if !sentSnapshot {
				resp.SnapshotTsNs = snapshotTsNs
				sentSnapshot = true
			}
			if err = stream.Send(resp); err != nil {
				return false, err
			}

			limit--
			if limit == 0 {
				return false, nil
			}
			return true, nil
		})

		if listErr != nil {
			return listErr
		}
		if err != nil {
			return err
		}
		if !hasEntries {
			break
		}

		includeLastFile = false

	}

	// No snapshot-only response for empty directories: many consumers (Java
	// FilerClient, S3 listing) treat any response as an entry. The trailer
	// carries it instead; older clients ignore trailers.
	stream.SetTrailer(metadata.Pairs(filer_pb.ListSnapshotTsNsTrailerKey, strconv.FormatInt(snapshotTsNs, 10)))

	return nil
}

func (fs *FilerServer) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {

	resp := &filer_pb.LookupVolumeResponse{
		LocationsMap: make(map[string]*filer_pb.Locations),
	}

	// Use master client's lookup with fallback - it handles cache and master query
	vidLocations, err := fs.filer.MasterClient.LookupVolumeIdsWithFallback(ctx, req.VolumeIds)

	// Convert wdclient.Location to filer_pb.Location
	// Return partial results even if there was an error
	for vidString, locations := range vidLocations {
		resp.LocationsMap[vidString] = &filer_pb.Locations{
			Locations: wdclientLocationsToPb(locations),
		}
	}

	return resp, err
}

// wdclientLocationsToPb converts the wdclient's internal Location entries
// (carrying DataInRemote and grpc-port metadata) to the protobuf form served
// by the filer gRPC API, preserving the fields the lookup client uses to
// prefer a local replica over a remote-tiered one.
func wdclientLocationsToPb(locations []wdclient.Location) []*filer_pb.Location {
	locs := make([]*filer_pb.Location, 0, len(locations))
	for _, loc := range locations {
		locs = append(locs, &filer_pb.Location{
			Url:          loc.Url,
			PublicUrl:    loc.PublicUrl,
			GrpcPort:     uint32(loc.GrpcPort),
			DataCenter:   loc.DataCenter,
			DataInRemote: loc.DataInRemote,
		})
	}
	return locs
}

func (fs *FilerServer) lookupFileId(ctx context.Context, fileId string) (targetUrls []string, err error) {
	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return nil, err
	}
	locations, found := fs.filer.MasterClient.GetLocations(uint32(fid.VolumeId))
	if !found || len(locations) == 0 {
		return nil, fmt.Errorf("not found volume %d in %s", fid.VolumeId, fileId)
	}
	for _, loc := range locations {
		targetUrls = append(targetUrls, fmt.Sprintf("http://%s/%s", loc.Url, fileId))
	}
	return
}

func (fs *FilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (resp *filer_pb.CreateEntryResponse, err error) {

	glog.V(4).InfofCtx(ctx, "CreateEntry %v/%v", req.Directory, req.Entry.Name)
	if len(req.Entry.HardLinkId) > 0 {
		glog.V(4).InfofCtx(ctx, "CreateEntry %s/%s with HardLinkId %x counter=%d", req.Directory, req.Entry.Name, req.Entry.HardLinkId, req.Entry.HardLinkCounter)
	}

	resp = &filer_pb.CreateEntryResponse{}

	// An exclusive or conditional create is a read-then-write that the per-path
	// lock below only makes atomic on this filer, while the store's insert is an
	// upsert. Route it to the entry's ring owner so one filer's lock arbitrates
	// every creator cluster-wide; is_moved bounds this to one hop. Plain creates
	// are upserts either way and stay local.
	routed := req.OExcl || conditionIsSet(req.Condition)
	if !req.IsMoved && routed {
		fullpath := util.NewFullPath(req.Directory, req.Entry.Name)
		// Held apart from the named resp, which the local path below writes into:
		// a failed forward must not leave it nil.
		var ownerResp *filer_pb.CreateEntryResponse
		handled, forwardErr := fs.forwardToWriteOwner(ctx, entryRouteKey(fullpath), func(owner pb.ServerAddress) error {
			glog.V(2).InfofCtx(ctx, "CreateEntry %s: forwarding to owner %s", fullpath, owner)
			req.IsMoved = true
			return pb.WithFilerClient(false, 0, owner, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				forwarded, e := client.CreateEntry(ctx, req)
				if e != nil {
					return e
				}
				ownerResp = forwarded
				return nil
			})
		})
		if handled {
			if forwardErr != nil {
				return &filer_pb.CreateEntryResponse{}, forwardErr
			}
			return ownerResp, nil
		}
	} else if req.IsMoved && routed {
		if err := fs.checkMovedMarker(ctx, req.IsMoved, fs.writeOwner(entryRouteKey(util.NewFullPath(req.Directory, req.Entry.Name)))); err != nil {
			return &filer_pb.CreateEntryResponse{}, err
		}
	}

	chunks, garbage, err2 := fs.cleanupChunks(ctx, util.Join(req.Directory, req.Entry.Name), nil, req.Entry)
	if err2 != nil {
		return &filer_pb.CreateEntryResponse{}, fmt.Errorf("CreateEntry cleanupChunks %s %s: %v", req.Directory, req.Entry.Name, err2)
	}

	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks
	so, err := fs.applyStorageDefaultsToEntry(ctx, newEntry)
	if err != nil {
		return nil, err
	}

	// Serialize concurrent mutations to the same path on this filer so the
	// read (existence/condition) and the write are atomic. Callers route a
	// key's writes to this owner filer, making this local lock sufficient.
	fullpath := newEntry.FullPath
	pathLock := fs.entryLockTable.AcquireLock("CreateEntry", fullpath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(fullpath, pathLock)

	// Fence stamped under the lock: a no-op create returns no event, but the
	// acknowledged state still reflects everything at or below this.
	resp.LogTsNs = time.Now().UnixNano()
	resp.LogSignature = fs.filer.Signature

	// Evaluate the optional precondition against the current entry while the
	// path lock is held, so the check and the write are atomic on this filer.
	// The fetched entry is then handed to CreateEntry below so it does not look
	// the same path up again under the lock.
	var existing *filer.Entry
	if conditionIsSet(req.Condition) {
		current, findErr := fs.filer.FindEntry(ctx, fullpath)
		if findErr != nil && findErr != filer_pb.ErrNotFound {
			return &filer_pb.CreateEntryResponse{}, fmt.Errorf("CreateEntry condition check %s: %w", fullpath, findErr)
		}
		if findErr == filer_pb.ErrNotFound {
			current = nil
		}
		if !writeConditionSatisfied(req.Condition, current) {
			glog.V(3).InfofCtx(ctx, "CreateEntry %s: precondition failed: %v", fullpath, req.Condition)
			return &filer_pb.CreateEntryResponse{
				Error:     "precondition failed",
				ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED,
			}, nil
		}
		existing = current
	}

	ctx, eventSink := filer.WithMetadataEventSink(ctx)
	createErr := fs.filer.CreateEntry(ctx, newEntry, existing, req.OExcl, req.IsFromOtherCluster, req.Signatures, req.SkipCheckParentDirectory, so.MaxFileNameLength)

	if createErr == nil {
		fs.filer.DeleteChunksNotRecursive(garbage)
		resp.MetadataEvent = eventSink.Last()
	} else {
		glog.V(3).InfofCtx(ctx, "CreateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), createErr)
		resp.Error = createErr.Error()
		resp.ErrorCode = filerErrorCode(createErr)
	}

	return
}

// filerErrorCode maps a filer error to its proto code so a client can match it
// with errors.Is against the sentinels instead of parsing the message.
func filerErrorCode(err error) filer_pb.FilerError {
	switch {
	case errors.Is(err, filer_pb.ErrEntryNameTooLong):
		return filer_pb.FilerError_ENTRY_NAME_TOO_LONG
	case errors.Is(err, filer_pb.ErrParentIsFile):
		return filer_pb.FilerError_PARENT_IS_FILE
	case errors.Is(err, filer_pb.ErrExistingIsDirectory):
		return filer_pb.FilerError_EXISTING_IS_DIRECTORY
	case errors.Is(err, filer_pb.ErrExistingIsFile):
		return filer_pb.FilerError_EXISTING_IS_FILE
	case errors.Is(err, filer_pb.ErrEntryAlreadyExists):
		return filer_pb.FilerError_ENTRY_ALREADY_EXISTS
	}
	return filer_pb.FilerError_OK
}

// ObjectTransaction applies an ordered list of entry mutations atomically with
// respect to other writers of the same object, by holding the per-path lock on
// lock_key for the whole call. The optional condition is checked first, against
// the entry at lock_key. This lets a caller describe a multi-entry object
// operation (e.g. delete the null version + write a delete marker + flip the
// latest pointer) as one request, replacing a distributed lock held across
// several RPCs. Callers must route the object's writes to its owner filer for
// the lock to be authoritative.
func (fs *FilerServer) ObjectTransaction(ctx context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	if req.LockKey == "" {
		return &filer_pb.ObjectTransactionResponse{Error: "lock_key is required"}, nil
	}

	// Route-by-key: if this filer is not the ring owner of route_key, forward the
	// whole transaction to the owner so its per-path lock is the single
	// serialization point — even when the caller's ring view was stale. is_moved
	// bounds this to one hop: a forwarded transaction is applied locally, so two
	// filers that disagree on the owner during a ring change cannot loop.
	if req.RouteKey != "" {
		if err := fs.checkMovedMarker(ctx, req.IsMoved, fs.writeOwner(req.RouteKey)); err != nil {
			return &filer_pb.ObjectTransactionResponse{Error: err.Error()}, nil
		}
	}
	if req.RouteKey != "" && !req.IsMoved {
		// Rebuild rather than copy the request struct (it carries a mutex); the
		// pointer/slice fields are shared since the original is not mutated.
		forwarded := &filer_pb.ObjectTransactionRequest{
			LockKey:            req.LockKey,
			Condition:          req.Condition,
			Mutations:          req.Mutations,
			IsFromOtherCluster: req.IsFromOtherCluster,
			Signatures:         req.Signatures,
			ConditionKey:       req.ConditionKey,
			RouteKey:           req.RouteKey,
			IsMoved:            true,
			RequestId:          req.RequestId,
			ConditionAltKey:    req.ConditionAltKey,
			ConditionPreferAlt: req.ConditionPreferAlt,
		}
		var resp *filer_pb.ObjectTransactionResponse
		handled, err := fs.forwardToWriteOwner(ctx, req.RouteKey, func(owner pb.ServerAddress) error {
			glog.V(2).InfofCtx(ctx, "ObjectTransaction %s: forwarding to owner %s", req.LockKey, owner)
			return pb.WithFilerClient(false, 0, owner, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				var e error
				resp, e = client.ObjectTransaction(ctx, forwarded)
				return e
			})
		})
		if handled {
			if err != nil {
				return &filer_pb.ObjectTransactionResponse{}, err
			}
			return resp, nil
		}
	}

	lockPath := util.FullPath(req.LockKey)
	pathLock := fs.entryLockTable.AcquireLock("ObjectTransaction", lockPath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(lockPath, pathLock)

	// Under the same lock the first application committed with, a failover
	// replay of the same request replays its recorded outcome instead of
	// re-applying mutations over whatever superseded it.
	if resp, found := fs.recentObjectTxn(req.RequestId, lockPath); found {
		glog.V(2).InfofCtx(ctx, "ObjectTransaction %s: replaying recorded outcome for request %s", req.LockKey, req.RequestId)
		return resp, nil
	}

	resp := &filer_pb.ObjectTransactionResponse{}
	// Outcomes are replayed rather than re-run: a mutation error may follow a
	// partial apply, and a retry that re-runs could write the stale mutation
	// over whatever superseded it, while replaying the recorded failure at
	// least matches what the first attempt reported. The one exception is the
	// condition lookup below: it fails before any mutation runs, so the retry
	// must re-evaluate rather than replay an empty outcome.
	record := true
	defer func() {
		if record {
			fs.rememberObjectTxn(req.RequestId, lockPath, resp)
		}
	}()

	if conditionIsSet(req.Condition) {
		// The condition is evaluated against condition_key when set (e.g. a
		// version entry whose WORM guards gate the delete), while the lock stays
		// on lock_key (the object, serializing the pointer recompute).
		conditionPath := lockPath
		if req.ConditionKey != "" {
			conditionPath = util.FullPath(req.ConditionKey)
		}
		current, findErr := fs.filer.FindEntry(ctx, conditionPath)
		if findErr == nil && current != nil && current.IsDirectory() && req.ConditionAltKey != "" {
			// A bare directory is not the object: implicit directories can
			// share a path with an S3 key, so treat it as an absent slot. A
			// directory entry holding content or the prefix-object marker is
			// an object and stays. Only the alt-key callers want object-slot
			// semantics; an explicit condition_key may legitimately name a
			// directory.
			_, isPrefixObject := current.Extended[s3_constants.SeaweedFSPrefixObject]
			if current.Mime == "" && len(current.GetChunks()) == 0 && len(current.Content) == 0 && !current.IsInRemoteOnly() && !isPrefixObject {
				current, findErr = nil, filer_pb.ErrNotFound
			}
		}
		// The caller can name a fallback evaluated when the primary is absent —
		// a suspended-versioning write checks "the null slot if present, else
		// the latest version" atomically under this lock. condition_alt_key is
		// the object's .versions directory, and the latest it resolves to is
		// read from the pointer maintained by the same lock, so a concurrent
		// version delete cannot move which entry the condition sees.
		if req.ConditionAltKey != "" {
			altEntry, altPath, altErr := fs.latestVersionedConditionEntry(ctx, util.FullPath(req.ConditionAltKey))
			if req.ConditionPreferAlt {
				// The versioned path resolves "current" through the latest
				// pointer, so a stale null-version entry or a tombstoned
				// latest decides correctly. When the alt path cannot resolve
				// (no .versions at all), the primary entry stands.
				switch {
				case altErr == nil:
					current, conditionPath, findErr = altEntry, altPath, nil
				case errors.Is(altErr, filer_pb.ErrNotFound):
				default:
					record = false
					return nil, fmt.Errorf("ObjectTransaction condition %s: %w", conditionPath, altErr)
				}
			} else if findErr == filer_pb.ErrNotFound {
				current, conditionPath, findErr = altEntry, altPath, altErr
			}
		}
		if findErr != nil && findErr != filer_pb.ErrNotFound {
			record = false // a lookup error decides nothing; a retry must re-run, not replay
			return nil, fmt.Errorf("ObjectTransaction condition %s: %w", conditionPath, findErr)
		}
		if findErr == filer_pb.ErrNotFound {
			current = nil
		}
		if !writeConditionSatisfied(req.Condition, current) {
			glog.V(3).InfofCtx(ctx, "ObjectTransaction %s: precondition failed", conditionPath)
			resp.Error = "precondition failed"
			resp.ErrorCode = filer_pb.FilerError_PRECONDITION_FAILED
			return resp, nil
		}
	}

	for i, m := range req.Mutations {
		if code, err := fs.checkMutationCondition(ctx, m); err != nil {
			glog.V(2).InfofCtx(ctx, "ObjectTransaction %s mutation %d condition lookup: %v", lockPath, i, err)
			resp.Error = fmt.Sprintf("mutation %d: %v", i, err)
			failed := int32(i)
			resp.FailedMutation = &failed
			return resp, nil
		} else if code != filer_pb.FilerError_OK {
			glog.V(3).InfofCtx(ctx, "ObjectTransaction %s mutation %d precondition failed", lockPath, i)
			resp.Error = fmt.Sprintf("mutation %d: precondition failed", i)
			resp.ErrorCode = code
			failed := int32(i)
			resp.FailedMutation = &failed
			return resp, nil
		}
		if err := fs.applyObjectMutation(ctx, m, req.IsFromOtherCluster, req.Signatures); err != nil {
			glog.V(2).InfofCtx(ctx, "ObjectTransaction %s mutation %d (%v): %v", lockPath, i, m.Type, err)
			resp.Error = fmt.Sprintf("mutation %d: %v", i, err)
			resp.ErrorCode = filerErrorCode(err)
			failed := int32(i)
			resp.FailedMutation = &failed
			return resp, nil
		}
	}

	return resp, nil
}

// checkMutationCondition evaluates a mutation's optional precondition under the
// transaction's lock, against condition_key when set or the mutation's target
// (the PUT's entry path, else directory/name). Unlike the request-level
// condition there is no alt-key fallback: a mutation guard names the exact entry
// it guards.
func (fs *FilerServer) checkMutationCondition(ctx context.Context, m *filer_pb.ObjectMutation) (filer_pb.FilerError, error) {
	if !conditionIsSet(m.Condition) {
		return filer_pb.FilerError_OK, nil
	}
	target := util.FullPath(m.ConditionKey)
	if target == "" {
		name := m.Name
		if m.Type == filer_pb.ObjectMutation_PUT && m.Entry != nil {
			name = m.Entry.Name
		}
		target = util.JoinPath(m.Directory, name)
	}
	current, err := fs.filer.FindEntry(ctx, target)
	if err != nil && err != filer_pb.ErrNotFound {
		return filer_pb.FilerError_OK, fmt.Errorf("condition check %s: %w", target, err)
	}
	if err == filer_pb.ErrNotFound {
		current = nil
	}
	if !writeConditionSatisfied(m.Condition, current) {
		return filer_pb.FilerError_PRECONDITION_FAILED, nil
	}
	return filer_pb.FilerError_OK, nil
}

const (
	recentObjectTxnTTL         = 2 * time.Minute
	recentObjectTxnSweepCap    = 10000
	recentObjectTxnSweepPeriod = 30 * time.Second
)

type recentObjectTxn struct {
	resp      *filer_pb.ObjectTransactionResponse
	expiresAt time.Time
}

// recentObjectTxnKey scopes an idempotency token to the transaction's lock
// path, so a token reused on a different object cannot replay this outcome.
func recentObjectTxnKey(requestID string, lockPath util.FullPath) string {
	return requestID + "\x00" + string(lockPath)
}

// recentObjectTxn returns the recorded outcome of an already-applied request,
// so a failover replay of the same ObjectTransaction does not re-apply it.
func (fs *FilerServer) recentObjectTxn(requestID string, lockPath util.FullPath) (*filer_pb.ObjectTransactionResponse, bool) {
	if requestID == "" {
		return nil, false
	}
	fs.recentObjectTxnsMu.Lock()
	defer fs.recentObjectTxnsMu.Unlock()

	key := recentObjectTxnKey(requestID, lockPath)
	txn, found := fs.recentObjectTxns[key]
	if !found {
		return nil, false
	}
	if time.Now().After(txn.expiresAt) {
		delete(fs.recentObjectTxns, key)
		return nil, false
	}
	return txn.resp, true
}

func (fs *FilerServer) rememberObjectTxn(requestID string, lockPath util.FullPath, resp *filer_pb.ObjectTransactionResponse) {
	if requestID == "" || resp == nil {
		return
	}
	fs.recentObjectTxnsMu.Lock()
	defer fs.recentObjectTxnsMu.Unlock()
	if fs.recentObjectTxns == nil {
		fs.recentObjectTxns = make(map[string]recentObjectTxn)
	}
	// A live record is never evicted early: a replay that misses re-applies the
	// transaction. The sweep is amortized so a hot cache does not scan on every
	// write; between sweeps the map may exceed the cap, still bounded by the
	// insert rate over the TTL.
	now := time.Now()
	if len(fs.recentObjectTxns) >= recentObjectTxnSweepCap {
		if now.After(fs.recentObjectTxnsSweepAt) {
			fs.recentObjectTxnsSweepAt = now.Add(recentObjectTxnSweepPeriod)
			for id, txn := range fs.recentObjectTxns {
				if now.After(txn.expiresAt) {
					delete(fs.recentObjectTxns, id)
				}
			}
		}
		if len(fs.recentObjectTxns) >= recentObjectTxnSweepCap {
			// Full of unexpired records: drop the new one rather than evict
			// an outcome a replay may still need.
			return
		}
	}
	fs.recentObjectTxns[recentObjectTxnKey(requestID, lockPath)] = recentObjectTxn{
		resp:      resp,
		expiresAt: now.Add(recentObjectTxnTTL),
	}
}

// latestVersionedConditionEntry resolves the object a conditional write should
// be checked against inside a .versions directory: the entry the latest-version
// pointer names, or nothing when the pointer names a delete marker — a
// tombstone is not a current version. A suspended-versioning write clears the
// pointer and stamps null-version-is-latest instead, which reports not-found so
// the regular path's null object decides. Any other missing pointer, or a
// pointer to a file that is gone, means the metadata cannot answer the
// question, which reports an error rather than guessing at either branch.
func (fs *FilerServer) latestVersionedConditionEntry(ctx context.Context, versionsDir util.FullPath) (*filer.Entry, util.FullPath, error) {
	versionsEntry, err := fs.filer.FindEntry(ctx, versionsDir)
	if err != nil {
		return nil, versionsDir, err
	}
	if versionsEntry == nil {
		return nil, versionsDir, filer_pb.ErrNotFound
	}
	latestName := string(versionsEntry.Extended[s3_constants.ExtLatestVersionFileNameKey])
	if latestName == "" {
		if string(versionsEntry.Extended[s3_constants.ExtNullVersionIsLatestKey]) == "true" {
			return nil, versionsDir, filer_pb.ErrNotFound
		}
		return nil, versionsDir, fmt.Errorf("%s has no latest-version pointer", versionsDir)
	}
	latestPath := util.NewFullPath(string(versionsDir), latestName)
	entry, err := fs.filer.FindEntry(ctx, latestPath)
	if err == filer_pb.ErrNotFound {
		return nil, latestPath, fmt.Errorf("latest-version pointer %s names a missing entry", latestPath)
	}
	if err != nil {
		return nil, latestPath, err
	}
	if string(entry.Extended[s3_constants.ExtDeleteMarkerKey]) == "true" {
		return nil, latestPath, nil
	}
	return entry, latestPath, nil
}

// ObjectTransactionBatch applies several object transactions in one round trip,
// each under its own per-path lock and independent of the others. A failed
// transaction (precondition or mutation error) is reported in its own response
// without aborting the rest, matching S3 multi-object semantics where each key
// succeeds or fails on its own.
func (fs *FilerServer) ObjectTransactionBatch(ctx context.Context, req *filer_pb.ObjectTransactionBatchRequest) (*filer_pb.ObjectTransactionBatchResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	resp := &filer_pb.ObjectTransactionBatchResponse{
		Responses: make([]*filer_pb.ObjectTransactionResponse, 0, len(req.Transactions)),
	}
	for _, txn := range req.Transactions {
		// Stop early if the caller went away; the request still holds the
		// unprocessed transactions, so it is retried rather than lost.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if txn == nil {
			resp.Responses = append(resp.Responses, &filer_pb.ObjectTransactionResponse{Error: "nil transaction"})
			continue
		}
		one, err := fs.ObjectTransaction(ctx, txn)
		if err != nil {
			// A transport-level error on one transaction is surfaced as that
			// transaction's error; the batch RPC itself still succeeds.
			one = &filer_pb.ObjectTransactionResponse{Error: err.Error()}
		}
		resp.Responses = append(resp.Responses, one)
	}
	return resp, nil
}

// applyStorageDefaultsToEntry enforces the path's storage rule (read-only
// prefixes reject the write) and fills in the rule TTL when the entry carries
// none. The returned option carries the same TTL as the entry, so chunks a
// caller still has to place expire with it.
func (fs *FilerServer) applyStorageDefaultsToEntry(ctx context.Context, entry *filer.Entry) (*operation.StorageOption, error) {
	if entry.Remote != nil {
		entry.TtlSec = 0
	}
	so, err := fs.detectStorageOption(ctx, string(entry.FullPath), "", "", entry.TtlSec, "", "", "", "")
	if err != nil {
		return nil, err
	}
	entry.ApplyStorageTtl(so.TtlSeconds)
	return so, nil
}

// applyObjectMutation applies a single mutation while the transaction's path
// lock is held. PUT entries are expected to be fully prepared by the caller
// (chunks resolved); mutations here are metadata-scoped. A DELETE of an absent
// entry and a PATCH of an absent entry are no-ops, so transactions are
// idempotent on replay.
func (fs *FilerServer) applyObjectMutation(ctx context.Context, m *filer_pb.ObjectMutation, fromOtherCluster bool, signatures []int32) error {
	switch m.Type {
	case filer_pb.ObjectMutation_PUT:
		if m.Entry == nil {
			return fmt.Errorf("PUT requires an entry")
		}
		newEntry := filer.FromPbEntry(m.Directory, m.Entry)
		so, err := fs.applyStorageDefaultsToEntry(ctx, newEntry)
		if err != nil {
			return err
		}
		return fs.filer.CreateEntry(ctx, newEntry, nil, false, fromOtherCluster, signatures, false, so.MaxFileNameLength)

	case filer_pb.ObjectMutation_DELETE:
		fullpath := util.NewFullPath(m.Directory, m.Name)
		if m.KeepRemoteObject {
			ctx = filer.WithKeepRemoteObject(ctx)
		}
		err := fs.filer.DeleteEntryMetaAndData(ctx, fullpath, m.IsRecursive, false, m.IsDeleteData, fromOtherCluster, signatures, time.Time{})
		if err != nil && err != filer_pb.ErrNotFound {
			return err
		}
		if m.RemoveEmptyParent {
			// A parent that exists only to hold this child (e.g. a .versions/
			// directory losing its last version) is torn down in the same locked
			// transaction. Best-effort: a non-empty or already-removed parent is
			// the expected no-op, and a failed teardown must not fail the
			// already-applied delete.
			parentErr := fs.filer.DeleteEntryMetaAndData(ctx, util.FullPath(m.Directory), false, false, false, fromOtherCluster, signatures, time.Time{})
			if parentErr != nil && parentErr != filer_pb.ErrNotFound && !errors.Is(parentErr, filer.ErrNonEmptyFolder) {
				glog.V(1).InfofCtx(ctx, "remove empty parent %s: %v", m.Directory, parentErr)
			}
		}
		return nil

	case filer_pb.ObjectMutation_PATCH_EXTENDED:
		fullpath := util.NewFullPath(m.Directory, m.Name)
		oldEntry, err := fs.filer.FindEntry(ctx, fullpath)
		if err == filer_pb.ErrNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		// Patch a copy so oldEntry still reflects the pre-update state for the
		// metadata notification's diff.
		newEntry := oldEntry.ShallowClone()
		newEntry.Extended = make(map[string][]byte, len(oldEntry.Extended))
		for k, v := range oldEntry.Extended {
			newEntry.Extended[k] = v
		}
		for k, v := range m.SetExtended {
			newEntry.Extended[k] = v
		}
		for _, k := range m.DeleteExtended {
			delete(newEntry.Extended, k)
		}
		if m.SetContent {
			newEntry.Content = m.Content
			// Keep FileSize consistent with content for files; some stores and
			// tools read the attribute directly. Directories carry no file size.
			if !newEntry.IsDirectory() {
				newEntry.FileSize = uint64(len(m.Content))
			}
		}
		if m.TouchMtime {
			newEntry.Attr.Mtime = time.Now()
		}
		fs.filer.StampEntryCommitTsNs(newEntry)
		if err := fs.filer.UpdateEntry(ctx, oldEntry, newEntry, fromOtherCluster); err != nil {
			fs.filer.ReleaseEntryCommitTsNs(newEntry.CommitTsNs)
			return err
		}
		// Emit the metadata event so the update replicates and subscribers see it,
		// matching the UpdateEntry handler.
		fs.filer.NotifyUpdateEvent(ctx, oldEntry, newEntry, true, fromOtherCluster, signatures)
		return nil

	case filer_pb.ObjectMutation_RECOMPUTE_LATEST:
		return fs.applyRecomputeLatest(ctx, m, fromOtherCluster, signatures)

	default:
		return fmt.Errorf("unknown mutation type %v", m.Type)
	}
}

// applyRecomputeLatest re-derives the pointer entry (m.Directory/m.Name) from the
// current contents of recompute.scan_dir, under the transaction's lock. It is
// mechanical: pick the child that sorts last (descending) or first by name, copy
// the mapped extended keys from it into the pointer, and store its name under
// name_to_key. When the scanned directory is empty the pointer keys are cleared.
// The caller, which knows the versioning scheme, supplies the direction and the
// key mappings. A missing pointer entry is a no-op (idempotent on replay).
func (fs *FilerServer) applyRecomputeLatest(ctx context.Context, m *filer_pb.ObjectMutation, fromOtherCluster bool, signatures []int32) error {
	rc := m.Recompute
	if rc == nil {
		return fmt.Errorf("RECOMPUTE_LATEST requires recompute parameters")
	}

	pointer, err := fs.filer.FindEntry(ctx, util.NewFullPath(m.Directory, m.Name))
	if err == filer_pb.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	if rc.OnlyIfCurrentName != "" && string(pointer.Extended[rc.NameToKey]) != rc.OnlyIfCurrentName {
		return nil
	}

	// Capture the pre-update image so the metadata notification carries a correct
	// diff; pointer.Extended is mutated in place below.
	oldPointer := pointer.ShallowClone()
	oldPointer.Extended = make(map[string][]byte, len(pointer.Extended))
	for k, v := range pointer.Extended {
		oldPointer.Extended[k] = v
	}

	if pointer.Extended == nil {
		pointer.Extended = make(map[string][]byte)
	}

	// Remember the prior chosen child so it can be demoted once the pointer moves.
	var priorName string
	if rc.NameToKey != "" {
		priorName = string(pointer.Extended[rc.NameToKey])
	}

	// The store streams entries ascending by name. For the lowest-name pick we
	// only need the first entry, so cap the listing at one; for the highest-name
	// pick we must scan all and keep the last (the store has no reverse order).
	// With exclude_name set the first child may be the excluded one, so the cap
	// is lifted to find the first non-excluded entry.
	limit := int64(math.MaxInt32)
	if !rc.Descending && rc.ExcludeName == "" {
		limit = 1
	}
	var chosen *filer.Entry
	_, listErr := fs.filer.StreamListDirectoryEntries(ctx, util.FullPath(rc.ScanDir), "", false, limit, "", "", "", func(entry *filer.Entry) (bool, error) {
		if rc.ExcludeName != "" && entry.Name() == rc.ExcludeName {
			return true, nil
		}
		chosen = entry
		return rc.Descending, nil
	})
	if listErr != nil {
		return listErr
	}

	cleared := []string{rc.NameToKey, rc.SizeToKey, rc.MtimeToKey}
	if chosen == nil {
		for pointerKey := range rc.CopyExtended {
			delete(pointer.Extended, pointerKey)
		}
		for _, k := range cleared {
			if k != "" {
				delete(pointer.Extended, k)
			}
		}
	} else {
		for pointerKey, sourceKey := range rc.CopyExtended {
			if v, ok := chosen.Extended[sourceKey]; ok {
				pointer.Extended[pointerKey] = v
			} else {
				delete(pointer.Extended, pointerKey)
			}
		}
		if rc.NameToKey != "" {
			pointer.Extended[rc.NameToKey] = []byte(chosen.Name())
		}
		if rc.SizeToKey != "" {
			pointer.Extended[rc.SizeToKey] = []byte(strconv.FormatUint(chosen.FileSize, 10))
		}
		if rc.MtimeToKey != "" {
			pointer.Extended[rc.MtimeToKey] = []byte(strconv.FormatInt(chosen.Mtime.Unix(), 10))
		}
	}

	fs.filer.StampEntryCommitTsNs(pointer)
	if err := fs.filer.UpdateEntry(ctx, oldPointer, pointer, fromOtherCluster); err != nil {
		fs.filer.ReleaseEntryCommitTsNs(pointer.CommitTsNs)
		return err
	}
	// Replicate the recomputed pointer to peer filers and subscribers. Without
	// this the latest-version pointer stays in this filer's store only, so other
	// filers never learn the current version and ListObjects undercounts.
	fs.filer.NotifyUpdateEvent(ctx, oldPointer, pointer, false, fromOtherCluster, signatures)

	// Stamp the displaced prior child (e.g. NoncurrentSinceNs for lifecycle).
	newName := ""
	if chosen != nil {
		newName = chosen.Name()
	}
	if rc.DemoteKey != "" && priorName != "" && priorName != newName {
		priorEntry, perr := fs.filer.FindEntry(ctx, util.NewFullPath(rc.ScanDir, priorName))
		if perr == filer_pb.ErrNotFound {
			return nil
		}
		if perr != nil {
			return perr
		}
		oldPrior := priorEntry.ShallowClone()
		oldPrior.Extended = make(map[string][]byte, len(priorEntry.Extended))
		for k, v := range priorEntry.Extended {
			oldPrior.Extended[k] = v
		}
		if priorEntry.Extended == nil {
			priorEntry.Extended = make(map[string][]byte)
		}
		priorEntry.Extended[rc.DemoteKey] = rc.DemoteValue
		fs.filer.StampEntryCommitTsNs(priorEntry)
		if err := fs.filer.UpdateEntry(ctx, oldPrior, priorEntry, fromOtherCluster); err != nil {
			fs.filer.ReleaseEntryCommitTsNs(priorEntry.CommitTsNs)
			return err
		}
		fs.filer.NotifyUpdateEvent(ctx, oldPrior, priorEntry, false, fromOtherCluster, signatures)
		return nil
	}

	return nil
}

func (fs *FilerServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {

	glog.V(4).InfofCtx(ctx, "UpdateEntry %s/%s", req.GetDirectory(), req.GetEntry().GetName())
	if len(req.Entry.HardLinkId) > 0 {
		glog.V(4).InfofCtx(ctx, "UpdateEntry %s/%s with HardLinkId %x counter=%d", req.Directory, req.Entry.Name, req.Entry.HardLinkId, req.Entry.HardLinkCounter)
	}

	fullpath := util.Join(req.Directory, req.Entry.Name)

	// A conditional or preconditioned update is a read-then-write that the
	// per-path lock below only makes atomic on this filer. Route it to the
	// entry's ring owner so one filer's lock arbitrates every writer
	// cluster-wide; is_moved bounds this to one hop.
	routed := conditionIsSet(req.Condition) || len(req.ExpectedExtended) > 0
	if !req.IsMoved && routed {
		var ownerResp *filer_pb.UpdateEntryResponse
		handled, forwardErr := fs.forwardToWriteOwner(ctx, entryRouteKey(util.FullPath(fullpath)), func(owner pb.ServerAddress) error {
			glog.V(2).InfofCtx(ctx, "UpdateEntry %s: forwarding to owner %s", fullpath, owner)
			req.IsMoved = true
			return pb.WithFilerClient(false, 0, owner, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				forwarded, e := client.UpdateEntry(ctx, req)
				if e != nil {
					return e
				}
				ownerResp = forwarded
				return nil
			})
		})
		if handled {
			if forwardErr != nil {
				return &filer_pb.UpdateEntryResponse{}, forwardErr
			}
			return ownerResp, nil
		}
	} else if req.IsMoved && routed {
		if err := fs.checkMovedMarker(ctx, req.IsMoved, fs.writeOwner(entryRouteKey(util.FullPath(fullpath)))); err != nil {
			return &filer_pb.UpdateEntryResponse{}, err
		}
	}

	// Serialize concurrent mutations to the same path on this filer so the
	// read (preconditions, garbage diff) and the write are atomic. Callers
	// route a key's writes to this owner filer, making this local lock
	// sufficient.
	lockPath := util.FullPath(fullpath)
	pathLock := fs.entryLockTable.AcquireLock("UpdateEntry", lockPath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(lockPath, pathLock)

	// Fence stamped under the lock: the no-change path below returns no event,
	// but the acknowledged state still reflects everything at or below this.
	logTsNs := time.Now().UnixNano()

	entry, err := fs.filer.FindEntry(ctx, lockPath)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return &filer_pb.UpdateEntryResponse{}, status.Errorf(codes.NotFound, "not found %s: %v", fullpath, err)
		}
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("not found %s: %v", fullpath, err)
	}
	if err := validateUpdateEntryPreconditions(entry, req.ExpectedExtended); err != nil {
		return &filer_pb.UpdateEntryResponse{}, err
	}
	if conditionIsSet(req.Condition) && !writeConditionSatisfied(req.Condition, entry) {
		glog.V(3).InfofCtx(ctx, "UpdateEntry %s: precondition failed: %v", fullpath, req.Condition)
		return &filer_pb.UpdateEntryResponse{}, status.Errorf(codes.FailedPrecondition, "precondition failed: %s", fullpath)
	}

	chunks, garbage, err2 := fs.cleanupChunks(ctx, fullpath, entry, req.Entry)
	if err2 != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("UpdateEntry cleanupChunks %s: %v", fullpath, err2)
	}

	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks

	// Don't apply TTL to remote entries - they're managed by remote storage
	if newEntry.Remote != nil {
		newEntry.TtlSec = 0
	}

	if filer.EqualEntry(entry, newEntry) {
		return &filer_pb.UpdateEntryResponse{LogTsNs: logTsNs, LogSignature: fs.filer.Signature}, err
	}

	ctx, eventSink := filer.WithMetadataEventSink(ctx)
	resp := &filer_pb.UpdateEntryResponse{LogTsNs: logTsNs, LogSignature: fs.filer.Signature}
	fs.filer.StampEntryCommitTsNs(newEntry)
	if err = fs.filer.UpdateEntry(ctx, entry, newEntry, req.IsFromOtherCluster); err == nil {
		fs.filer.DeleteChunksNotRecursive(garbage)

		fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, req.IsFromOtherCluster, req.Signatures)
		resp.MetadataEvent = eventSink.Last()

	} else {
		fs.filer.ReleaseEntryCommitTsNs(newEntry.CommitTsNs)
		glog.V(3).InfofCtx(ctx, "UpdateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), err)
	}

	return resp, err
}

func validateUpdateEntryPreconditions(entry *filer.Entry, expectedExtended map[string][]byte) error {
	if len(expectedExtended) == 0 {
		return nil
	}

	for key, expectedValue := range expectedExtended {
		var actualValue []byte
		var ok bool
		if entry != nil {
			actualValue, ok = entry.Extended[key]
		}
		if ok {
			if !bytes.Equal(actualValue, expectedValue) {
				return status.Errorf(codes.FailedPrecondition, "extended attribute %q changed", key)
			}
			continue
		}
		if len(expectedValue) > 0 {
			return status.Errorf(codes.FailedPrecondition, "extended attribute %q changed", key)
		}
	}

	return nil
}

func (fs *FilerServer) cleanupChunks(ctx context.Context, fullpath string, existingEntry *filer.Entry, newEntry *filer_pb.Entry) (chunks, garbage []*filer_pb.FileChunk, err error) {

	// remove old chunks if not included in the new ones
	if existingEntry != nil {
		garbage, err = filer.MinusChunks(ctx, fs.lookupFileId, existingEntry.GetChunks(), newEntry.GetChunks(), fs.filer.MasterClient)
		if err != nil {
			return newEntry.GetChunks(), nil, fmt.Errorf("MinusChunks: %w", err)
		}
	}

	// files with manifest chunks are usually large and append only, skip calculating covered chunks
	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(newEntry.GetChunks())

	chunks, coveredChunks, err := filer.CompactFileChunks(ctx, fs.lookupFileId, nonManifestChunks)
	if err != nil {
		return newEntry.GetChunks(), nil, fmt.Errorf("CompactFileChunks: %w", err)
	}
	garbage = append(garbage, coveredChunks...)

	if newEntry.Attributes != nil {
		so, _ := fs.detectStorageOption(ctx, fullpath,
			"",
			"",
			newEntry.Attributes.TtlSec,
			"",
			"",
			"",
			"",
		) // ignore readonly error for capacity needed to manifestize
		// A failed fold leaves a flat list, which is still a correct entry --
		// and the store rejects it by name if it cannot hold one this size.
		folded, manifestErr := filer.MaybeManifestize(fs.saveAsChunk(ctx, so), fs.filer.DeleteChunksNotRecursive, chunks)
		if manifestErr != nil {
			glog.V(0).InfofCtx(ctx, "MaybeManifestize %s: %v", fullpath, manifestErr)
		}
		chunks = folded
	}

	chunks = append(manifestChunks, chunks...)

	return
}

func (fs *FilerServer) AppendToEntry(ctx context.Context, req *filer_pb.AppendToEntryRequest) (*filer_pb.AppendToEntryResponse, error) {

	glog.V(4).InfofCtx(ctx, "AppendToEntry %s/%s", req.GetDirectory(), req.GetEntryName())
	fullpath := util.NewFullPath(req.Directory, req.EntryName)

	lockClient := cluster.NewLockClient(fs.grpcDialOption, fs.option.Host)
	lock := lockClient.NewShortLivedLock(string(fullpath), string(fs.option.Host))
	if lock == nil {
		return nil, fmt.Errorf("failed to acquire lock for %s", fullpath)
	}
	defer lock.StopShortLivedLock()

	// The cluster lock serializes appenders across filers; the path lock makes
	// this read-modify-write atomic against conditional updates and deletes on
	// the owner filer.
	pathLock := fs.entryLockTable.AcquireLock("AppendToEntry", fullpath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(fullpath, pathLock)

	var offset int64 = 0
	entry, err := fs.filer.FindEntry(ctx, fullpath)
	if err == filer_pb.ErrNotFound {
		entry = &filer.Entry{
			FullPath: fullpath,
			Attr: filer.Attr{
				Crtime: time.Now(),
				Mtime:  time.Now(),
				Mode:   os.FileMode(0644),
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}
	} else {
		offset = int64(filer.TotalSize(entry.GetChunks()))
	}

	for _, chunk := range req.Chunks {
		chunk.Offset = offset
		offset += int64(chunk.Size)
	}

	entry.Chunks = append(entry.GetChunks(), req.Chunks...)
	so, err := fs.applyStorageDefaultsToEntry(ctx, entry)
	if err != nil {
		glog.WarningfCtx(ctx, "applyStorageDefaultsToEntry: %v", err)
		return &filer_pb.AppendToEntryResponse{}, err
	}
	entry.Chunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), fs.filer.DeleteChunksNotRecursive, entry.GetChunks())
	if err != nil {
		// the append is still correct with the flat list
		glog.V(0).InfofCtx(ctx, "MaybeManifestize %s: %v", fullpath, err)
	}

	err = fs.filer.CreateEntry(context.Background(), entry, nil, false, false, nil, false, fs.filer.MaxFilenameLength)

	return &filer_pb.AppendToEntryResponse{}, err
}

// deleteCutoff merges the conditional-delete guards into one cutoff:
// if_not_modified_after_ns wins when set, and the whole-second
// if_not_modified_after maps to the last nanosecond of its second so that
// entry.Mtime.Unix() > sec keeps its exact meaning as a time comparison,
// even for dates int64 nanoseconds cannot hold.
func deleteCutoff(ifNotModifiedAfter, ifNotModifiedAfterNs int64) time.Time {
	if ifNotModifiedAfterNs > 0 {
		return time.Unix(0, ifNotModifiedAfterNs)
	}
	if ifNotModifiedAfter <= 0 {
		return time.Time{}
	}
	return time.Unix(ifNotModifiedAfter, 999_999_999)
}

func (fs *FilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (resp *filer_pb.DeleteEntryResponse, err error) {

	glog.V(4).InfofCtx(ctx, "DeleteEntry %v", req)

	fullpath := util.JoinPath(req.Directory, req.Name)

	// A delete carrying extended-attribute preconditions is a read-then-delete
	// that the per-path lock below only makes atomic on this filer. Route it
	// to the entry's ring owner so one filer's lock arbitrates every writer
	// cluster-wide; is_moved bounds this to one hop.
	if len(req.ExpectedExtended) > 0 {
		if !req.IsMoved {
			var ownerResp *filer_pb.DeleteEntryResponse
			handled, forwardErr := fs.forwardToWriteOwner(ctx, entryRouteKey(fullpath), func(owner pb.ServerAddress) error {
				glog.V(2).InfofCtx(ctx, "DeleteEntry %s: forwarding to owner %s", fullpath, owner)
				req.IsMoved = true
				return pb.WithFilerClient(false, 0, owner, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
					forwarded, e := client.DeleteEntry(ctx, req)
					if e != nil {
						return e
					}
					ownerResp = forwarded
					return nil
				})
			})
			if handled {
				if forwardErr != nil {
					return &filer_pb.DeleteEntryResponse{}, forwardErr
				}
				return ownerResp, nil
			}
		} else {
			if err := fs.checkMovedMarker(ctx, req.IsMoved, fs.writeOwner(entryRouteKey(fullpath))); err != nil {
				return &filer_pb.DeleteEntryResponse{}, err
			}
		}
	}

	// A delete queues the entry's chunks for deletion, so it must not
	// interleave with a conditional update's check-then-write on the same
	// path: the update would pass its precondition and then resurrect fids
	// that are already on the deletion queue.
	pathLock := fs.entryLockTable.AcquireLock("DeleteEntry", fullpath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(fullpath, pathLock)

	// Extended-attribute preconditions run here, under the lock the delete
	// runs under: an attribute write that keeps Mtime can no longer slip
	// between the check and the delete.
	if len(req.ExpectedExtended) > 0 {
		entry, err := fs.filer.FindEntry(ctx, fullpath)
		if err != nil {
			return &filer_pb.DeleteEntryResponse{}, status.Errorf(codes.FailedPrecondition, "extended attribute precondition %s: %v", fullpath, err)
		}
		if err := validateUpdateEntryPreconditions(entry, req.ExpectedExtended); err != nil {
			return &filer_pb.DeleteEntryResponse{}, err
		}
	}

	if req.KeepRemoteObject {
		ctx = filer.WithKeepRemoteObject(ctx)
	}
	ctx, eventSink := filer.WithMetadataEventSink(ctx)
	err = fs.filer.DeleteEntryMetaAndData(ctx, fullpath, req.IsRecursive, req.IgnoreRecursiveError, req.IsDeleteData, req.IsFromOtherCluster, req.Signatures, deleteCutoff(req.IfNotModifiedAfter, req.IfNotModifiedAfterNs))
	resp = &filer_pb.DeleteEntryResponse{}
	if err != nil && err != filer_pb.ErrNotFound {
		resp.Error = err.Error()
	} else {
		resp.MetadataEvent = eventSink.Last()
	}
	return resp, nil
}

func (fs *FilerServer) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (resp *filer_pb.AssignVolumeResponse, err error) {

	so, err := fs.resolveAssignStorageOption(ctx, req)
	if err != nil {
		glog.V(3).InfofCtx(ctx, "AssignVolume: %v", err)
		resp = &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}
		if errors.Is(err, ErrReadOnly) {
			// Still a successful RPC: clients treat gRPC errors as transport
			// failures and retry or fail over, but read-only is a verdict.
			resp.ErrorCode = filer_pb.FilerError_READ_ONLY
		}
		return resp, nil
	}

	assignRequest, altRequest := so.ToAssignRequests(int(req.Count))
	assignRequest.ExpectedDataSize = req.ExpectedDataSize
	if altRequest != nil {
		altRequest.ExpectedDataSize = req.ExpectedDataSize
	}

	assignResult, err := operation.Assign(ctx, fs.filer.GetMaster, fs.masterGrpcDialOption(), assignRequest, altRequest)
	if err != nil {
		glog.V(3).InfofCtx(ctx, "AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}
	if assignResult.Error != "" {
		glog.V(3).InfofCtx(ctx, "AssignVolume error: %v", assignResult.Error)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume result: %v", assignResult.Error)}, nil
	}

	resp = &filer_pb.AssignVolumeResponse{
		FileId: assignResult.Fid,
		Count:  int32(assignResult.Count),
		Location: &filer_pb.Location{
			Url:       assignResult.Url,
			PublicUrl: assignResult.PublicUrl,
			GrpcPort:  uint32(assignResult.GrpcPort),
		},
		Auth:        string(assignResult.Auth),
		Collection:  so.Collection,
		Replication: so.Replication,
		Fsync:       so.Fsync,
	}
	// Forward the replica holders so a client can write all copies directly.
	for _, replica := range assignResult.Replicas {
		resp.Replicas = append(resp.Replicas, &filer_pb.Location{
			Url:        replica.Url,
			PublicUrl:  replica.PublicUrl,
			DataCenter: replica.DataCenter,
		})
	}
	return resp, nil
}

func (fs *FilerServer) resolveAssignStorageOption(ctx context.Context, req *filer_pb.AssignVolumeRequest) (*operation.StorageOption, error) {
	so, err := fs.detectStorageOption(ctx, req.Path, req.Collection, req.Replication, req.TtlSec, req.DiskType, req.DataCenter, req.Rack, req.DataNode)
	if err != nil {
		return nil, err
	}

	// Mirror the HTTP write path: only apply the filer's default disk when the
	// matched locationPrefix rule did not already select one.
	if so.DiskType == "" {
		so.DiskType = fs.option.DiskType
	}

	return so, nil
}

func (fs *FilerServer) CollectionList(ctx context.Context, req *filer_pb.CollectionListRequest) (resp *filer_pb.CollectionListResponse, err error) {

	glog.V(4).InfofCtx(ctx, "CollectionList %v", req)
	resp = &filer_pb.CollectionListResponse{}

	err = fs.filer.MasterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
		masterResp, err := client.CollectionList(ctx, &master_pb.CollectionListRequest{
			IncludeNormalVolumes: req.IncludeNormalVolumes,
			IncludeEcVolumes:     req.IncludeEcVolumes,
		})
		if err != nil {
			return err
		}
		for _, c := range masterResp.Collections {
			resp.Collections = append(resp.Collections, &filer_pb.Collection{Name: c.Name})
		}
		return nil
	})

	return
}

func (fs *FilerServer) DeleteCollection(ctx context.Context, req *filer_pb.DeleteCollectionRequest) (resp *filer_pb.DeleteCollectionResponse, err error) {

	glog.V(4).InfofCtx(ctx, "DeleteCollection %v", req)

	err = fs.filer.DoDeleteCollection(ctx, req.GetCollection())

	return &filer_pb.DeleteCollectionResponse{}, err
}
