package s3api

import (
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/private/protocol/json/jsonutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	//"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"math"
	"sync"
)

type BucketMetaData struct {
	_ struct{} `type:"structure"`

	Name            *string
	ObjectOwnership *string

	// Container for the bucket owner's display name and ID.
	Owner *s3.Owner `type:"structure"`

	// A list of grants.
	Acl []*s3.Grant `locationName:"AccessControlList" locationNameList:"Grant" type:"list"`

	Policy *string
}

func (bm BucketMetaData) validate() error {
	if bm.ObjectOwnership == nil {

	}
	return nil
}

type BucketRegistry struct {
	sync.RWMutex
	metadataCache map[string]*BucketMetaData

	notFound map[string]struct{}
	s3a      *S3ApiServer
	loadLock sync.RWMutex
}

func NewBucketRegistry(s3a *S3ApiServer) *BucketRegistry {
	br := &BucketRegistry{
		metadataCache: make(map[string]*BucketMetaData),
		s3a:           s3a,
	}
	err := br.initRegistry()
	if err != nil {
		//todo: exit program?
		return nil
	}
	return br
}

func (r *BucketRegistry) initRegistry() error {
	err := filer_pb.List(r.s3a, r.s3a.Option.BucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		r.LoadBucketMetadata(entry)
		return nil
	}, "", false, math.MaxUint32)
	return err
}

func (r *BucketRegistry) LoadBucketMetadata(entry *filer_pb.Entry) {
	bucketMetadata := r.buildBucketMetadata(entry)
	r.Lock()
	defer r.Unlock()
	r.metadataCache[entry.Name] = bucketMetadata
}

func (r *BucketRegistry) RemoveBucketMetadata(entry *filer_pb.Entry) {
	r.Lock()
	defer r.Unlock()
	delete(r.metadataCache, entry.Name)
}

func (r *BucketRegistry) GetBucketMetadata(bucketName *string) (bucketMetadata *BucketMetaData, errCode s3err.ErrorCode) {
	r.RLock()
	bucketMetadata = r.metadataCache[*bucketName]
	r.RUnlock()
	if bucketMetadata == nil {
		//tried before?
		r.loadLock.RLock()
		_, ok := r.notFound[*bucketName]
		if ok {
			return nil, s3err.ErrNoSuchBucket
		}
		r.loadLock.Unlock()

		//if not, load from filer
		r.loadLock.Lock()
		defer r.loadLock.Unlock()

		var err error
		bucketMetadata, err = r.getBucketMetaFromFiler(*bucketName)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				r.notFound[*bucketName] = struct{}{}
				return nil, s3err.ErrNoSuchBucket
			}
			return nil, s3err.ErrInternalError
		}

		r.Lock()
		defer r.Unlock()
		delete(r.notFound, *bucketName)
		r.metadataCache[*bucketName] = bucketMetadata
	}
	return
}

func (r *BucketRegistry) getBucketMetaFromFiler(bucketName string) (*BucketMetaData, error) {
	entry, err := filer_pb.GetEntry(r.s3a, util.NewFullPath(r.s3a.Option.BucketsPath, bucketName))
	if err != nil {
		return nil, err
	}

	return r.buildBucketMetadata(entry), nil
}

func (r *BucketRegistry) buildBucketMetadata(entry *filer_pb.Entry) *BucketMetaData {
	//default OwnershipBucketOwnerEnforced, means Acl is disabled
	entryJson, _ := json.Marshal(entry)
	glog.V(4).Infof("build bucket metadata,entry=%s", entryJson)
	bucketMetadata := &BucketMetaData{
		Name: &entry.Name,
		Owner: &s3.Owner{
			ID:          &AccountAdminId,
			DisplayName: &AccountAdminName,
		},
		ObjectOwnership: &DefaultOwnershipForExistingBucket,
		Policy:          nil,
	}
	if entry.Extended != nil {
		//ownership control
		ownership, ok := entry.Extended[s3_constants.ExtOwnershipKey]
		if ok {
			ownership := string(ownership)
			bucketMetadata.ObjectOwnership = &ownership
		}

		//access control policy
		acpBytes, ok := entry.Extended[s3_constants.ExtAcpKey]
		if ok {
			err := jsonutil.UnmarshalJSON(bucketMetadata, bytes.NewReader(acpBytes))
			if err != nil {
				//error log
			}
		}
		if bucketMetadata.Owner == nil || bucketMetadata.Owner.ID == nil {
			bucketMetadata.Owner = &s3.Owner{
				DisplayName: &AccountAdmin.Name,
				ID:          &AccountAdmin.CanonicalId,
			}
		}

		//policy
		policy, ok := entry.Extended[s3_constants.ExtPolicyKey]
		if ok {
			policy := string(policy)
			bucketMetadata.Policy = &policy
		}
	}
	return bucketMetadata
}
