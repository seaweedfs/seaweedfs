package s3api

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"reflect"
	"sync"
	"testing"
	"time"
)

type BucketMetadataTestCase struct {
	filerEntry           *filer_pb.Entry
	expectBucketMetadata *BucketMetaData
}

var (
	//bad entry
	badEntry = &filer_pb.Entry{
		Name: "badEntry",
	}

	//good entry
	goodEntryAcl, _ = json.Marshal(s3_constants.PublicRead)
	goodEntry       = &filer_pb.Entry{
		Name: "entryWithValidAcp",
		Extended: map[string][]byte{
			s3_constants.ExtOwnershipKey: []byte(s3_constants.OwnershipBucketOwnerEnforced),
			s3_constants.ExtAmzOwnerKey:  []byte(AccountAdmin.DisplayName),
			s3_constants.ExtAmzAclKey:    goodEntryAcl,
		},
	}

	//ownership is ""
	ownershipEmptyStr = &filer_pb.Entry{
		Name: "ownershipEmptyStr",
		Extended: map[string][]byte{
			s3_constants.ExtOwnershipKey: []byte(""),
		},
	}

	//ownership valid
	ownershipValid = &filer_pb.Entry{
		Name: "ownershipValid",
		Extended: map[string][]byte{
			s3_constants.ExtOwnershipKey: []byte(s3_constants.OwnershipBucketOwnerEnforced),
		},
	}

	//owner is ""
	acpEmptyStr = &filer_pb.Entry{
		Name: "acpEmptyStr",
		Extended: map[string][]byte{
			s3_constants.ExtAmzOwnerKey: []byte(""),
		},
	}

	//owner not exists
	acpEmptyObject = &filer_pb.Entry{
		Name: "acpEmptyObject",
		Extended: map[string][]byte{
			s3_constants.ExtAmzOwnerKey: []byte("xxxxx"),
		},
	}

	//grants is nil
	acpOwnerNilAcp, _ = json.Marshal(make([]*s3.Grant, 0))
	acpOwnerNil       = &filer_pb.Entry{
		Name: "acpOwnerNil",
		Extended: map[string][]byte{
			s3_constants.ExtAmzAclKey: acpOwnerNilAcp,
		},
	}

	//load filer is
	loadFilerBucket = make(map[string]int, 1)
	//override `loadBucketMetadataFromFiler` to avoid really load from filer
)

var tcs = []*BucketMetadataTestCase{
	{
		badEntry, &BucketMetaData{
			Name:            badEntry.Name,
			ObjectOwnership: s3_constants.DefaultOwnershipForExists,
			Owner: &s3.Owner{
				DisplayName: &AccountAdmin.DisplayName,
				ID:          &AccountAdmin.Id,
			},
			Acl: nil,
		},
	},
	{
		goodEntry, &BucketMetaData{
			Name:            goodEntry.Name,
			ObjectOwnership: s3_constants.OwnershipBucketOwnerEnforced,
			Owner: &s3.Owner{
				DisplayName: &AccountAdmin.DisplayName,
				ID:          &AccountAdmin.Id,
			},
			Acl: s3_constants.PublicRead,
		},
	},
	{
		ownershipEmptyStr, &BucketMetaData{
			Name:            ownershipEmptyStr.Name,
			ObjectOwnership: s3_constants.DefaultOwnershipForExists,
			Owner: &s3.Owner{
				DisplayName: &AccountAdmin.DisplayName,
				ID:          &AccountAdmin.Id,
			},
			Acl: nil,
		},
	},
	{
		ownershipValid, &BucketMetaData{
			Name:            ownershipValid.Name,
			ObjectOwnership: s3_constants.OwnershipBucketOwnerEnforced,
			Owner: &s3.Owner{
				DisplayName: &AccountAdmin.DisplayName,
				ID:          &AccountAdmin.Id,
			},
			Acl: nil,
		},
	},
	{
		acpEmptyStr, &BucketMetaData{
			Name:            acpEmptyStr.Name,
			ObjectOwnership: s3_constants.DefaultOwnershipForExists,
			Owner: &s3.Owner{
				DisplayName: &AccountAdmin.DisplayName,
				ID:          &AccountAdmin.Id,
			},
			Acl: nil,
		},
	},
	{
		acpEmptyObject, &BucketMetaData{
			Name:            acpEmptyObject.Name,
			ObjectOwnership: s3_constants.DefaultOwnershipForExists,
			Owner: &s3.Owner{
				DisplayName: &AccountAdmin.DisplayName,
				ID:          &AccountAdmin.Id,
			},
			Acl: nil,
		},
	},
	{
		acpOwnerNil, &BucketMetaData{
			Name:            acpOwnerNil.Name,
			ObjectOwnership: s3_constants.DefaultOwnershipForExists,
			Owner: &s3.Owner{
				DisplayName: &AccountAdmin.DisplayName,
				ID:          &AccountAdmin.Id,
			},
			Acl: make([]*s3.Grant, 0),
		},
	},
}

func TestBuildBucketMetadata(t *testing.T) {
	iam := &IdentityAccessManagement{}
	_ = iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{})
	for _, tc := range tcs {
		resultBucketMetadata := buildBucketMetadata(iam, tc.filerEntry)
		if !reflect.DeepEqual(resultBucketMetadata, tc.expectBucketMetadata) {
			t.Fatalf("result is unexpect: \nresult: %v, \nexpect: %v", resultBucketMetadata, tc.expectBucketMetadata)
		}
	}
}

func TestGetBucketMetadata(t *testing.T) {
	loadBucketMetadataFromFiler = func(r *BucketRegistry, bucketName string) (*BucketMetaData, error) {
		time.Sleep(time.Second)
		loadFilerBucket[bucketName] = loadFilerBucket[bucketName] + 1
		return &BucketMetaData{
			Name: bucketName,
		}, nil
	}

	br := &BucketRegistry{
		metadataCache: make(map[string]*BucketMetaData),
		notFound:      make(map[string]struct{}),
		s3a:           nil,
	}

	//start 40 goroutine for
	var wg sync.WaitGroup
	closeCh := make(chan struct{})
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
		outLoop:
			for {
				for j := 0; j < 5; j++ {
					select {
					case <-closeCh:
						break outLoop
					default:
						reqBucket := fmt.Sprintf("%c", 67+j)
						_, errCode := br.GetBucketMetadata(reqBucket)
						if errCode != s3err.ErrNone {
							close(closeCh)
							t.Error("not expect")
						}
					}
				}
				time.Sleep(10 * time.Microsecond)
			}
		}()
	}
	time.Sleep(time.Second)
	close(closeCh)
	wg.Wait()

	//Each bucket is loaded from the filer only once
	for bucketName, loadCount := range loadFilerBucket {
		if loadCount != 1 {
			t.Fatalf("lock is uneffict: %s, %d", bucketName, loadCount)
		}
	}
}
