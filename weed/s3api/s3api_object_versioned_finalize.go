package s3api

import (
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// objectWriteOwner resolves the filer that owns all of an object's writes,
// regardless of versioning state, or "" when no ring view is available. Normal,
// suspended, and versioned writes to the same object hash to one owner and
// serialize on its per-path lock.
func (s3a *S3ApiServer) objectWriteOwner(bucket, object string) pb.ServerAddress {
	if s3a.objectWriteLockClient == nil {
		return ""
	}
	return s3a.objectWriteLockClient.PrimaryForKey(s3a.objectRouteKey(bucket, object))
}

// latestPointerRecompute builds the RECOMPUTE_LATEST mutation that re-derives an
// object's .versions pointer. excludeName, when set, omits a version about to be
// deleted (so the pointer is repointed before the blob is removed); demote, when
// set, stamps the displaced prior latest with NoncurrentSinceNs.
func (s3a *S3ApiServer) latestPointerRecompute(bucket, object string, useInvertedFormat bool, excludeName string, demote bool) *filer_pb.ObjectMutation {
	versionsPath := s3a.toFilerPath(bucket, object+s3_constants.VersionsFolder)
	vdir, vname := util.FullPath(versionsPath).DirAndName()
	rc := &filer_pb.Recompute{
		ScanDir: versionsPath,
		// Inverted ids sort newest-first, so the newest is the first ascending
		// entry; legacy ids sort oldest-first (scan to the last).
		Descending: !useInvertedFormat,
		NameToKey:  s3_constants.ExtLatestVersionFileNameKey,
		SizeToKey:  s3_constants.ExtLatestVersionSizeKey,
		MtimeToKey: s3_constants.ExtLatestVersionMtimeKey,
		CopyExtended: map[string]string{
			s3_constants.ExtLatestVersionIdKey:          s3_constants.ExtVersionIdKey,
			s3_constants.ExtLatestVersionETagKey:        s3_constants.ExtETagKey,
			s3_constants.ExtLatestVersionOwnerKey:       s3_constants.ExtAmzOwnerKey,
			s3_constants.ExtLatestVersionIsDeleteMarker: s3_constants.ExtDeleteMarkerKey,
		},
		ExcludeName: excludeName,
	}
	if demote {
		rc.DemoteKey = s3_constants.ExtNoncurrentSinceNsKey
		rc.DemoteValue = []byte(strconv.FormatInt(time.Now().UnixNano(), 10))
	}
	return &filer_pb.ObjectMutation{
		Type:      filer_pb.ObjectMutation_RECOMPUTE_LATEST,
		Directory: vdir,
		Name:      vname,
		Recompute: rc,
	}
}

// routedVersionedFinalize flips the .versions pointer to the newest version and
// demotes the prior latest, atomically under the object's per-path lock on the
// owner filer, via a single RECOMPUTE_LATEST. The version file is already
// written; the owner re-derives the pointer by scanning the directory.
func (s3a *S3ApiServer) routedVersionedFinalize(owner pb.ServerAddress, bucket, object string, useInvertedFormat bool) s3err.ErrorCode {
	req := &filer_pb.ObjectTransactionRequest{
		LockKey:   s3a.toFilerPath(bucket, object),
		RouteKey:  s3a.objectRouteKey(bucket, object),
		Mutations: []*filer_pb.ObjectMutation{s3a.latestPointerRecompute(bucket, object, useInvertedFormat, "", true)},
	}
	resp, err := s3a.objectTxnOnFiler(owner, req)
	switch {
	case err != nil:
		glog.Errorf("routedVersionedFinalize: %s/%s on %s: %v", bucket, object, owner, err)
		return s3err.ErrInternalError
	case resp.Error != "":
		glog.Errorf("routedVersionedFinalize: %s/%s: %s", bucket, object, resp.Error)
		return s3err.ErrInternalError
	default:
		return s3err.ErrNone
	}
}

// wormDeleteCondition returns the object-lock guards for a delete, or nil when
// the bucket has no object lock. Legal hold always blocks. Retention blocks
// while not elapsed; with governance bypass the retention guard is gated to
// COMPLIANCE mode, so a governance-mode version becomes deletable while a
// compliance-mode one stays protected — the filer decides from the version's
// mode under the lock, so the gateway never has to read it.
func wormDeleteCondition(worm, bypass bool) *filer_pb.WriteCondition {
	if !worm {
		return nil
	}
	retention := &filer_pb.WriteCondition_Clause{
		Kind:   filer_pb.WriteCondition_IF_EXTENDED_TIME_ELAPSED,
		ExtKey: s3_constants.ExtRetentionUntilDateKey,
	}
	if bypass {
		retention.GateKey = s3_constants.ExtObjectLockModeKey
		retention.GateValue = s3_constants.RetentionModeCompliance
	}
	return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
		{Kind: filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL, ExtKey: s3_constants.ExtLegalHoldKey, ExtValue: s3_constants.LegalHoldOn},
		retention,
	}}
}

// routedDeleteSpecificVersion deletes one version off the distributed lock: in a
// single transaction on the owner it recomputes the .versions pointer excluding
// the version (repoint-before-delete, so a crash leaves a recoverable orphan
// rather than a dangling pointer) and deletes the version file. lock_key is the
// object (serializing the pointer recompute); for object-lock buckets the
// condition gates the delete on the version's WORM guards evaluated on the owner.
// Deleting the last version also removes the emptied .versions/ directory —
// leaving it behind would keep re-triggering the read path's self-heal rescans
// on every GET of the key (Veeam probes its deleted lock objects forever).
func (s3a *S3ApiServer) routedDeleteSpecificVersion(owner pb.ServerAddress, bucket, object, versionId string, worm, bypass bool) s3err.ErrorCode {
	if !isValidVersionID(versionId) {
		return s3err.ErrInvalidRequest
	}
	versionFileName := s3a.getVersionFileName(versionId)
	versionsPath := s3a.toFilerPath(bucket, object+s3_constants.VersionsFolder)
	cond := wormDeleteCondition(worm, bypass)
	req := &filer_pb.ObjectTransactionRequest{
		LockKey:      s3a.toFilerPath(bucket, object),
		RouteKey:     s3a.objectRouteKey(bucket, object),
		ConditionKey: versionsPath + "/" + versionFileName,
		Condition:    cond,
		Mutations: []*filer_pb.ObjectMutation{
			s3a.latestPointerRecompute(bucket, object, isNewFormatVersionId(versionId), versionFileName, false),
			{Type: filer_pb.ObjectMutation_DELETE, Directory: versionsPath, Name: versionFileName, IsDeleteData: true, RemoveEmptyParent: true},
		},
	}
	resp, err := s3a.objectTxnOnFiler(owner, req)
	switch {
	case err != nil:
		glog.Errorf("routedDeleteSpecificVersion: %s/%s %s on %s: %v", bucket, object, versionId, owner, err)
		return s3err.ErrInternalError
	case resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED:
		// Legal hold or retention in force on the version.
		return s3err.ErrAccessDenied
	case resp.Error != "":
		glog.Errorf("routedDeleteSpecificVersion: %s/%s %s: %s", bucket, object, versionId, resp.Error)
		return s3err.ErrInternalError
	default:
		return s3err.ErrNone
	}
}

// routedDeleteNullVersion deletes the null version (the regular object entry, not
// a .versions file) off the distributed lock. There is no pointer to recompute;
// the WORM guards, when present, gate the delete on the object entry itself
// (condition defaults to lock_key).
func (s3a *S3ApiServer) routedDeleteNullVersion(owner pb.ServerAddress, bucket, object string, worm, bypass bool) s3err.ErrorCode {
	fullpath := util.NewFullPath(s3a.bucketDir(bucket), object)
	dir, name := fullpath.DirAndName()
	resp, err := s3a.objectTxnOnFiler(owner, &filer_pb.ObjectTransactionRequest{
		LockKey:   string(fullpath),
		RouteKey:  s3a.objectRouteKey(bucket, object),
		Condition: wormDeleteCondition(worm, bypass),
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_DELETE, Directory: dir, Name: name, IsDeleteData: true},
		},
	})
	switch {
	case err != nil:
		glog.Errorf("routedDeleteNullVersion: %s/%s on %s: %v", bucket, object, owner, err)
		return s3err.ErrInternalError
	case resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED:
		return s3err.ErrAccessDenied
	case resp.Error != "":
		glog.Errorf("routedDeleteNullVersion: %s/%s: %s", bucket, object, resp.Error)
		return s3err.ErrInternalError
	default:
		return s3err.ErrNone
	}
}

// versionedFinalize flips the .versions latest pointer for a versioned PutObject:
// on the routed path RECOMPUTE_LATEST rides in the version file's PUT transaction,
// committing atomically under the object's per-path lock; off the ring
// updateLatestVersionInDirectory does it under the object write lock.
func (s3a *S3ApiServer) versionedFinalize(bucket, object, versionId, versionFileName string, useInvertedFormat bool) *putFinalize {
	return &putFinalize{
		lockKey:   s3a.toFilerPath(bucket, object),
		mutations: []*filer_pb.ObjectMutation{s3a.latestPointerRecompute(bucket, object, useInvertedFormat, "", true)},
		afterCreate: func(versionEntry *filer_pb.Entry) s3err.ErrorCode {
			if err := s3a.updateLatestVersionInDirectory(bucket, object, versionId, versionFileName, versionEntry); err != nil {
				glog.Errorf("putVersionedObject: failed to update latest version in directory: %v", err)
				return s3err.ErrInternalError
			}
			return s3err.ErrNone
		},
	}
}
