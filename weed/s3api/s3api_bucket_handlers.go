package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"math"
	"net/http"
	"time"

	xhttp "github.com/chrislusf/seaweedfs/weed/s3api/http"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   *s3.Owner
	Buckets []*s3.Bucket `xml:"Buckets>Bucket"`
}

func (s3a *S3ApiServer) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {

	var identity *Identity
	var s3Err s3err.ErrorCode
	if s3a.iam.isEnabled() {
		identity, s3Err = s3a.iam.authUser(r)
		if s3Err != s3err.ErrNone {
			writeErrorResponse(w, s3Err, r.URL)
			return
		}
	}

	var response ListAllMyBucketsResult

	entries, _, err := s3a.list(s3a.option.BucketsPath, "", "", false, math.MaxInt32)

	if err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}

	identityId := r.Header.Get(xhttp.AmzIdentityId)

	var buckets []*s3.Bucket
	for _, entry := range entries {
		if entry.IsDirectory {
			if identity!=nil && !identity.canDo(s3_constants.ACTION_ADMIN, entry.Name) {
				continue
			}
			buckets = append(buckets, &s3.Bucket{
				Name:         aws.String(entry.Name),
				CreationDate: aws.Time(time.Unix(entry.Attributes.Crtime, 0).UTC()),
			})
		}
	}

	response = ListAllMyBucketsResult{
		Owner: &s3.Owner{
			ID:          aws.String(identityId),
			DisplayName: aws.String(identityId),
		},
		Buckets: buckets,
	}

	writeSuccessResponseXML(w, encodeResponse(response))
}

func (s3a *S3ApiServer) PutBucketHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := getBucketAndObject(r)

	// avoid duplicated buckets
	errCode := s3err.ErrNone
	if err := s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		if resp, err := client.CollectionList(context.Background(), &filer_pb.CollectionListRequest{
			IncludeEcVolumes:     true,
			IncludeNormalVolumes: true,
		}); err != nil {
			glog.Errorf("list collection: %v", err)
			return fmt.Errorf("list collections: %v", err)
		} else {
			for _, c := range resp.Collections {
				if bucket == c.Name {
					errCode = s3err.ErrBucketAlreadyExists
					break
				}
			}
		}
		return nil
	}); err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}
	if exist, err := s3a.exists(s3a.option.BucketsPath, bucket, true); err == nil && exist {
		errCode = s3err.ErrBucketAlreadyExists
	}
	if errCode != s3err.ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	fn := func(entry *filer_pb.Entry) {
		if identityId := r.Header.Get(xhttp.AmzIdentityId); identityId != "" {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[xhttp.AmzIdentityId] = []byte(identityId)
		}
	}

	// create the folder for bucket, but lazily create actual collection
	if err := s3a.mkdir(s3a.option.BucketsPath, bucket, fn); err != nil {
		glog.Errorf("PutBucketHandler mkdir: %v", err)
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseEmpty(w)
}

func (s3a *S3ApiServer) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := getBucketAndObject(r)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		writeErrorResponse(w, err, r.URL)
		return
	}

	err := s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		// delete collection
		deleteCollectionRequest := &filer_pb.DeleteCollectionRequest{
			Collection: bucket,
		}

		glog.V(1).Infof("delete collection: %v", deleteCollectionRequest)
		if _, err := client.DeleteCollection(context.Background(), deleteCollectionRequest); err != nil {
			return fmt.Errorf("delete collection %s: %v", bucket, err)
		}

		return nil
	})

	err = s3a.rm(s3a.option.BucketsPath, bucket, false, true)

	if err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}

	writeResponse(w, http.StatusNoContent, nil, mimeNone)
}

func (s3a *S3ApiServer) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := getBucketAndObject(r)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		writeErrorResponse(w, err, r.URL)
		return
	}

	writeSuccessResponseEmpty(w)
}

func (s3a *S3ApiServer) checkBucket(r *http.Request, bucket string) s3err.ErrorCode {
	entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if entry == nil || err == filer_pb.ErrNotFound {
		return s3err.ErrNoSuchBucket
	}

	if !s3a.hasAccess(r, entry) {
		return s3err.ErrAccessDenied
	}
	return s3err.ErrNone
}

func (s3a *S3ApiServer) hasAccess(r *http.Request, entry *filer_pb.Entry) bool {
	isAdmin := r.Header.Get(xhttp.AmzIsAdmin) != ""
	if isAdmin {
		return true
	}
	if entry.Extended == nil {
		return true
	}

	identityId := r.Header.Get(xhttp.AmzIdentityId)
	if id, ok := entry.Extended[xhttp.AmzIdentityId]; ok {
		if identityId != string(id) {
			return false
		}
	}
	return true
}
