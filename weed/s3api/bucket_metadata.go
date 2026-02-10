package s3api

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

var loadBucketMetadataFromFiler = func(r *BucketRegistry, bucketName string) (*BucketMetaData, error) {
	entry, err := r.s3a.getBucketEntry(bucketName)
	if err != nil {
		return nil, err
	}

	return buildBucketMetadata(r.s3a.iam, entry), nil
}

type BucketMetaData struct {
	_ struct{} `type:"structure"`

	Name string
	// Indicates the bucket is a table bucket.
	IsTableBucket bool

	//By default, when another AWS account uploads an object to S3 bucket,
	//that account (the object writer) owns the object, has access to it, and
	//can grant other users access to it through ACLs. You can use Object Ownership
	//to change this default behavior so that ACLs are disabled and you, as the
	//bucket owner, automatically own every object in your bucket.
	ObjectOwnership string

	// Container for the bucket owner's display name and ID.
	Owner *s3.Owner `type:"structure"`

	// A list of grants for access controls.
	Acl []*s3.Grant `locationName:"AccessControlList" locationNameList:"Grant" type:"list"`
}

type BucketRegistry struct {
	metadataCache     map[string]*BucketMetaData
	metadataCacheLock sync.RWMutex

	tableLocationCache map[string]string // Cache for table location mappings (external bucket -> mapped root path)
	tableLocationLock  sync.RWMutex

	notFound     map[string]struct{}
	notFoundLock sync.RWMutex
	s3a          *S3ApiServer
}

func NewBucketRegistry(s3a *S3ApiServer) *BucketRegistry {
	br := &BucketRegistry{
		metadataCache:      make(map[string]*BucketMetaData),
		tableLocationCache: make(map[string]string),
		notFound:           make(map[string]struct{}),
		s3a:                s3a,
	}
	err := br.init()
	if err != nil {
		glog.Fatal("init bucket registry failed", err)
		return nil
	}
	return br
}

func (r *BucketRegistry) init() error {
	var bucketCount int
	err := filer_pb.List(context.Background(), r.s3a, r.s3a.option.BucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry != nil && strings.HasPrefix(entry.Name, ".") {
			return nil
		}
		r.LoadBucketMetadata(entry)
		// Also warm the bucket config cache with Object Lock and versioning settings
		// This ensures cache consistency across multi-filer clusters after restart
		r.s3a.updateBucketConfigCacheFromEntry(entry)
		bucketCount++
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		glog.Errorf("BucketRegistry.init: failed to list buckets: %v", err)
		return err
	}
	glog.V(1).Infof("BucketRegistry.init: warmed config cache for %d buckets", bucketCount)
	return nil
}

func (r *BucketRegistry) LoadBucketMetadata(entry *filer_pb.Entry) {
	bucketMetadata := buildBucketMetadata(r.s3a.iam, entry)
	r.metadataCacheLock.Lock()
	r.metadataCache[entry.Name] = bucketMetadata
	r.metadataCacheLock.Unlock()
	// Remove from notFound cache since bucket now exists
	r.unMarkNotFound(entry.Name)
}

func buildBucketMetadata(accountManager AccountManager, entry *filer_pb.Entry) *BucketMetaData {
	entryJson, _ := json.Marshal(entry)
	glog.V(3).Infof("build bucket metadata,entry=%s", entryJson)
	bucketMetadata := &BucketMetaData{
		Name:          entry.Name,
		IsTableBucket: s3tables.IsTableBucketEntry(entry),

		//Default ownership: OwnershipBucketOwnerEnforced, which means Acl is disabled
		ObjectOwnership: s3_constants.OwnershipBucketOwnerEnforced,

		// Default owner: `AccountAdmin`
		Owner: &s3.Owner{
			ID:          &AccountAdmin.Id,
			DisplayName: &AccountAdmin.DisplayName,
		},
	}
	if entry.Extended != nil {
		//ownership control
		ownership, ok := entry.Extended[s3_constants.ExtOwnershipKey]
		if ok {
			ownership := string(ownership)
			valid := s3_constants.ValidateOwnership(ownership)
			if valid {
				bucketMetadata.ObjectOwnership = ownership
			} else {
				glog.Warningf("Invalid ownership: %s, bucket: %s", ownership, bucketMetadata.Name)
			}
		}

		//access control policy
		//owner
		acpOwnerBytes, ok := entry.Extended[s3_constants.ExtAmzOwnerKey]
		if ok && len(acpOwnerBytes) > 0 {
			ownerAccountId := string(acpOwnerBytes)
			ownerAccountName := accountManager.GetAccountNameById(ownerAccountId)
			if ownerAccountName == "" {
				glog.Warningf("owner[id=%s] is invalid, bucket: %s", ownerAccountId, bucketMetadata.Name)
			} else {
				bucketMetadata.Owner = &s3.Owner{
					ID:          &ownerAccountId,
					DisplayName: &ownerAccountName,
				}
			}
		}
		//grants
		acpGrantsBytes, ok := entry.Extended[s3_constants.ExtAmzAclKey]
		if ok && len(acpGrantsBytes) > 0 {
			var grants []*s3.Grant
			err := json.Unmarshal(acpGrantsBytes, &grants)
			if err == nil {
				bucketMetadata.Acl = grants
			} else {
				glog.Warningf("Unmarshal ACP grants: %s(%v), bucket: %s", string(acpGrantsBytes), err, bucketMetadata.Name)
			}
		}
	}
	return bucketMetadata
}

func (r *BucketRegistry) RemoveBucketMetadata(entry *filer_pb.Entry) {
	r.removeMetadataCache(entry.Name)
	r.unMarkNotFound(entry.Name)
	r.removeTableLocationCache(entry.Name)
}

func (r *BucketRegistry) GetBucketMetadata(bucketName string) (*BucketMetaData, s3err.ErrorCode) {
	r.metadataCacheLock.RLock()
	bucketMetadata, ok := r.metadataCache[bucketName]
	r.metadataCacheLock.RUnlock()
	if ok {
		return bucketMetadata, s3err.ErrNone
	}

	r.notFoundLock.RLock()
	_, ok = r.notFound[bucketName]
	r.notFoundLock.RUnlock()
	if ok {
		return nil, s3err.ErrNoSuchBucket
	}

	bucketMetadata, errCode := r.LoadBucketMetadataFromFiler(bucketName)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	r.setMetadataCache(bucketMetadata)
	r.unMarkNotFound(bucketName)
	return bucketMetadata, s3err.ErrNone
}

func (r *BucketRegistry) LoadBucketMetadataFromFiler(bucketName string) (*BucketMetaData, s3err.ErrorCode) {
	r.notFoundLock.Lock()
	defer r.notFoundLock.Unlock()

	//check if already exists
	r.metadataCacheLock.RLock()
	bucketMetaData, ok := r.metadataCache[bucketName]
	r.metadataCacheLock.RUnlock()
	if ok {
		return bucketMetaData, s3err.ErrNone
	}

	//if not exists, load from filer
	bucketMetadata, err := loadBucketMetadataFromFiler(r, bucketName)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			// The bucket doesn't actually exist and should no longer loaded from the filer
			r.notFound[bucketName] = struct{}{}
			return nil, s3err.ErrNoSuchBucket
		}
		return nil, s3err.ErrInternalError
	}
	return bucketMetadata, s3err.ErrNone
}

func (r *BucketRegistry) setMetadataCache(metadata *BucketMetaData) {
	r.metadataCacheLock.Lock()
	defer r.metadataCacheLock.Unlock()
	r.metadataCache[metadata.Name] = metadata
}

func (r *BucketRegistry) removeMetadataCache(bucket string) {
	r.metadataCacheLock.Lock()
	defer r.metadataCacheLock.Unlock()
	delete(r.metadataCache, bucket)
}

func (r *BucketRegistry) removeTableLocationCache(bucket string) {
	r.tableLocationLock.Lock()
	defer r.tableLocationLock.Unlock()
	delete(r.tableLocationCache, bucket)
}

func (r *BucketRegistry) markNotFound(bucket string) {
	r.notFoundLock.Lock()
	defer r.notFoundLock.Unlock()
	r.notFound[bucket] = struct{}{}
}

func (r *BucketRegistry) unMarkNotFound(bucket string) {
	r.notFoundLock.Lock()
	defer r.notFoundLock.Unlock()
	delete(r.notFound, bucket)
}
