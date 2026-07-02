package s3api

import (
	"encoding/json"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3"
	lru "github.com/hashicorp/golang-lru/v2"
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
	metadataCache *lru.Cache[string, *BucketMetaData]

	notFound *lru.Cache[string, struct{}]
	// notFoundLock also serializes filer loads so a hot bucket is fetched only once
	notFoundLock sync.RWMutex
	s3a          *S3ApiServer
}

// NewBucketRegistry creates a lazy registry: nothing is listed at startup,
// buckets load from the filer on first access and stay fresh via the
// metadata subscription.
func NewBucketRegistry(s3a *S3ApiServer) *BucketRegistry {
	metadataCache, _ := lru.New[string, *BucketMetaData](bucketCacheCapacity)
	notFound, _ := lru.New[string, struct{}](bucketCacheCapacity)
	return &BucketRegistry{
		metadataCache: metadataCache,
		notFound:      notFound,
		s3a:           s3a,
	}
}

// LoadBucketMetadata refreshes a bucket already resident in the cache from a
// subscription event. Cold buckets are left to lazy-load on first access so
// the cache holds only this gateway's working set.
func (r *BucketRegistry) LoadBucketMetadata(entry *filer_pb.Entry) {
	if r.metadataCache.Contains(entry.Name) {
		r.metadataCache.Add(entry.Name, buildBucketMetadata(r.s3a.iam, entry))
	}
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
}

func (r *BucketRegistry) GetBucketMetadata(bucketName string) (*BucketMetaData, s3err.ErrorCode) {
	bucketMetadata, ok := r.metadataCache.Get(bucketName)
	if ok {
		return bucketMetadata, s3err.ErrNone
	}

	r.notFoundLock.RLock()
	ok = r.notFound.Contains(bucketName)
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
	bucketMetaData, ok := r.metadataCache.Get(bucketName)
	if ok {
		return bucketMetaData, s3err.ErrNone
	}

	//if not exists, load from filer
	bucketMetadata, err := loadBucketMetadataFromFiler(r, bucketName)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			// The bucket doesn't actually exist and should no longer loaded from the filer
			r.notFound.Add(bucketName, struct{}{})
			return nil, s3err.ErrNoSuchBucket
		}
		return nil, s3err.ErrInternalError
	}
	return bucketMetadata, s3err.ErrNone
}

func (r *BucketRegistry) setMetadataCache(metadata *BucketMetaData) {
	r.metadataCache.Add(metadata.Name, metadata)
}

func (r *BucketRegistry) removeMetadataCache(bucket string) {
	r.metadataCache.Remove(bucket)
}

func (r *BucketRegistry) unMarkNotFound(bucket string) {
	r.notFoundLock.Lock()
	defer r.notFoundLock.Unlock()
	r.notFound.Remove(bucket)
}
