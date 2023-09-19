package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3acl"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"math"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   *s3.Owner
	Buckets []*s3.Bucket `xml:"Buckets>Bucket"`
}

func (s3a *S3ApiServer) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {

	glog.V(3).Infof("ListBucketsHandler")

	var identity *Identity
	var s3Err s3err.ErrorCode
	if s3a.iam.isEnabled() {
		identity, s3Err = s3a.iam.authUser(r)
		if s3Err != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, s3Err)
			return
		}
	}

	var response ListAllMyBucketsResult

	entries, _, err := s3a.list(s3a.option.BucketsPath, "", "", false, math.MaxInt32)

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	identityId := r.Header.Get(s3_constants.AmzIdentityId)

	var buckets []*s3.Bucket
	for _, entry := range entries {
		if entry.IsDirectory {
			if identity != nil && !identity.canDo(s3_constants.ACTION_LIST, entry.Name, "") {
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

	writeSuccessResponseXML(w, r, response)
}

func (s3a *S3ApiServer) PutBucketHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketHandler %s", bucket)

	// validate the bucket name
	err := s3bucket.VerifyS3BucketName(bucket)
	if err != nil {
		glog.Errorf("put invalid bucket name: %v %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidBucketName)
		return
	}

	// avoid duplicated buckets
	errCode := s3err.ErrNone
	if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if resp, err := client.CollectionList(context.Background(), &filer_pb.CollectionListRequest{
			IncludeEcVolumes:     true,
			IncludeNormalVolumes: true,
		}); err != nil {
			glog.Errorf("list collection: %v", err)
			return fmt.Errorf("list collections: %v", err)
		} else {
			for _, c := range resp.Collections {
				if s3a.getCollectionName(bucket) == c.Name {
					errCode = s3err.ErrBucketAlreadyExists
					break
				}
			}
		}
		return nil
	}); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if exist, err := s3a.exists(s3a.option.BucketsPath, bucket, true); err == nil && exist {
		errCode = s3err.ErrBucketAlreadyExists
	}
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// create the folder for bucket, but lazily create actual collection
	if err := s3a.mkdir(s3a.option.BucketsPath, bucket, nil); err != nil {
		glog.Errorf("PutBucketHandler mkdir: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	objectOwnership := r.Header.Get("X-Amz-Object-Ownership")
	requestDisplayName := r.Header.Get(s3_constants.AmzIdentityId)
	requestAccountId := s3acl.GetAccountId(r)
	acp := s3a.bacp.GetAccessControlPolicy(bucket)
	grants, errCode := s3acl.ExtractBucketAcl(r, s3a.accountManager, objectOwnership, requestAccountId, requestAccountId, true)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}
	if len(grants) == 0 {
		grants = append(grants, &s3.Grant{
			Grantee: &s3.Grantee{
				Type:        &s3_constants.GrantTypeCanonicalUser,
				ID:          &requestAccountId,
				DisplayName: &requestDisplayName,
			},
			Permission: &s3_constants.PermissionFullControl,
		})
	}
	acp.Lock()
	acp.ObjectOwnership = objectOwnership
	acp.Owner.ID = &requestAccountId
	acp.Owner.DisplayName = &requestDisplayName
	acp.Grants = grants
	acp.Unlock()
	glog.V(4).Infof("save owner: %s, bucket: %s, ACL: %+v", requestDisplayName, bucket, *acp)
	if err := s3a.SaveBucketAccessControlPoliciesConfig(); err != nil {
		glog.Errorf("Failed save bucket access control policies config to filer: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	writeSuccessResponseEmpty(w, r)
}

func (s3a *S3ApiServer) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteBucketHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if !s3a.option.AllowDeleteBucketNotEmpty {
			entries, _, err := s3a.list(s3a.option.BucketsPath+"/"+bucket, "", "", false, 2)
			if err != nil {
				return fmt.Errorf("failed to list bucket %s: %v", bucket, err)
			}
			for _, entry := range entries {
				if entry.Name != s3_constants.MultipartUploadsFolder {
					return errors.New(s3err.GetAPIError(s3err.ErrBucketNotEmpty).Code)
				}
			}
		}

		// delete collection
		deleteCollectionRequest := &filer_pb.DeleteCollectionRequest{
			Collection: s3a.getCollectionName(bucket),
		}

		glog.V(1).Infof("delete collection: %v", deleteCollectionRequest)
		if _, err := client.DeleteCollection(context.Background(), deleteCollectionRequest); err != nil {
			return fmt.Errorf("delete collection %s: %v", bucket, err)
		}

		return nil
	})

	if err != nil {
		s3ErrorCode := s3err.ErrInternalError
		if err.Error() == s3err.GetAPIError(s3err.ErrBucketNotEmpty).Code {
			s3ErrorCode = s3err.ErrBucketNotEmpty
		}
		s3err.WriteErrorResponse(w, r, s3ErrorCode)
		return
	}

	err = s3a.rm(s3a.option.BucketsPath, bucket, false, true)
	if err != nil {
		glog.Errorf("Failed remove bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	s3a.bacp.DeleteAccessControlPolicy(bucket)
	if err := s3a.SaveBucketAccessControlPoliciesConfig(); err != nil {
		glog.Errorf("Failed save bucket access control policies config to filer: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	s3err.WriteEmptyResponse(w, r, http.StatusNoContent)
}

func (s3a *S3ApiServer) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("HeadBucketHandler %s", bucket)

	_, errorCode := s3a.checkAccessForReadBucket(r, bucket, s3_constants.PermissionRead)
	if errorCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errorCode)
		return
	}
	if entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket); entry == nil || err == filer_pb.ErrNotFound {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	writeSuccessResponseEmpty(w, r)
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
	isAdmin := r.Header.Get(s3_constants.AmzIsAdmin) != ""
	if isAdmin {
		return true
	}
	if entry.Extended == nil {
		return true
	}

	identityId := r.Header.Get(s3_constants.AmzIdentityId)
	if id, ok := entry.Extended[s3_constants.AmzIdentityId]; ok {
		if identityId != string(id) {
			return false
		}
	}
	return true
}

// PutBucketAclHandler Put bucket ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
func (s3a *S3ApiServer) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketAclHandler %s", bucket)

	var errCode s3err.ErrorCode
	objectOwnership := r.Header.Get("X-Amz-Object-Ownership")
	accountId := r.Header.Get(s3_constants.AmzAccountId)
	requestAccountId := s3acl.GetAccountId(r)
	grants, errCode := s3acl.ExtractBucketAcl(r, s3a.accountManager, objectOwnership, accountId, requestAccountId, true)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}
	acp := s3a.bacp.GetAccessControlPolicy(bucket)
	acp.Lock()
	// Todo maybe need mege grants
	acp.Grants = grants
	acp.Unlock()
	s3err.WriteEmptyResponse(w, r, http.StatusOK)
}

// GetBucketAclHandler Get Bucket ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
func (s3a *S3ApiServer) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	acp := s3a.bacp.GetAccessControlPolicy(bucket)
	acl := s3.PutBucketAclInput{AccessControlPolicy: &s3.AccessControlPolicy{
		Grants: acp.Grants,
		Owner:  acp.Owner,
	}}
	glog.V(3).Infof("GetBucketAclHandler %s, acl: %+v", bucket, acl)

	s3err.WriteAwsXMLResponse(w, r, http.StatusOK, &acl)
}

// GetBucketLifecycleConfigurationHandler Get Bucket Lifecycle configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
func (s3a *S3ApiServer) GetBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketLifecycleConfigurationHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}
	fc, err := filer.ReadFilerConf(s3a.option.Filer, s3a.option.GrpcDialOption, nil)
	if err != nil {
		glog.Errorf("GetBucketLifecycleConfigurationHandler: %s", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	ttls := fc.GetCollectionTtls(s3a.getCollectionName(bucket))
	if len(ttls) == 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchLifecycleConfiguration)
		return
	}
	response := Lifecycle{}
	for prefix, internalTtl := range ttls {
		ttl, _ := needle.ReadTTL(internalTtl)
		days := int(ttl.Minutes() / 60 / 24)
		if days == 0 {
			continue
		}
		response.Rules = append(response.Rules, Rule{
			Status: Enabled, Filter: Filter{
				Prefix: Prefix{string: prefix, set: true},
				set:    true,
			},
			Expiration: Expiration{Days: days, set: true},
		})
	}
	writeSuccessResponseXML(w, r, response)
}

// PutBucketLifecycleConfigurationHandler Put Bucket Lifecycle configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
func (s3a *S3ApiServer) PutBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {

	s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)

}

// DeleteBucketMetricsConfiguration Delete Bucket Lifecycle
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
func (s3a *S3ApiServer) DeleteBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {

	s3err.WriteEmptyResponse(w, r, http.StatusNoContent)

}

// GetBucketLocationHandler Get bucket location
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (s3a *S3ApiServer) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseXML(w, r, LocationConstraint{})
}

// GetBucketRequestPaymentHandler Get bucket location
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketRequestPayment.html
func (s3a *S3ApiServer) GetBucketRequestPaymentHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseXML(w, r, RequestPaymentConfiguration{Payer: "BucketOwner"})
}

// PutBucketOwnershipControls https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketOwnershipControls.html
func (s3a *S3ApiServer) PutBucketOwnershipControls(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketOwnershipControls %s", bucket)

	acp := s3a.bacp.GetAccessControlPolicy(bucket)

	errCode := s3a.checkAccessByOwnership(r, acp)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	if r.Body == nil || r.Body == http.NoBody {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	defer util.CloseRequest(r)
	var v s3.OwnershipControls
	err := xmlutil.UnmarshalXML(&v, xml.NewDecoder(r.Body), "")
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	if len(v.Rules) != 1 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	newObjectOwnership := *v.Rules[0].ObjectOwnership
	switch newObjectOwnership {
	case s3_constants.OwnershipObjectWriter:
	case s3_constants.OwnershipBucketOwnerPreferred:
	case s3_constants.OwnershipBucketOwnerEnforced:
	default:
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	bucketEntry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	oldOwnership, ok := bucketEntry.Extended[s3_constants.ExtOwnershipKey]
	if !ok || string(oldOwnership) != newObjectOwnership {

		// must reset bucket acl to default(bucket owner with full control permission) before setting ownership
		// to `OwnershipBucketOwnerEnforced` (bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting)
		if newObjectOwnership == s3_constants.OwnershipBucketOwnerEnforced {
			acpGrants := s3acl.GetAcpGrants(nil, bucketEntry.Extended)
			if len(acpGrants) > 1 {
				s3err.WriteErrorResponse(w, r, s3err.InvalidBucketAclWithObjectOwnership)
				return
			} else if len(acpGrants) == 1 {
				bucketOwner := s3acl.GetAcpOwner(bucketEntry.Extended, s3account.AccountAdmin.Id)
				expectGrant := s3acl.GrantWithFullControl(bucketOwner)
				if !s3acl.GrantEquals(acpGrants[0], expectGrant) {
					s3err.WriteErrorResponse(w, r, s3err.InvalidBucketAclWithObjectOwnership)
					return
				}
			}
		}

		if bucketEntry.Extended == nil {
			bucketEntry.Extended = make(map[string][]byte)
		}
		//update local cache
		bucketMetadata, eCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
		if eCode == s3err.ErrNone {
			bucketMetadata.ObjectOwnership = newObjectOwnership
		}
		bucketEntry.Extended[s3_constants.ExtOwnershipKey] = []byte(newObjectOwnership)
		err = s3a.updateEntry(s3a.option.BucketsPath, bucketEntry)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	}
	result := &s3.PutBucketOwnershipControlsInput{
		OwnershipControls: &v,
	}
	s3err.WriteAwsXMLResponse(w, r, http.StatusOK, result)
}

func (s3a *S3ApiServer) GetBucketOwnership(bucket string) (error, string) {
	bucketEntry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		return err, ""
	}
	if v, ok := bucketEntry.Extended[s3_constants.ExtOwnershipKey]; ok {
		return nil, string(v)
	}
	return nil, ""
}

// GetBucketOwnershipControls https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketOwnershipControls.html
func (s3a *S3ApiServer) GetBucketOwnershipControls(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketOwnershipControls %s", bucket)

	acp := s3a.bacp.GetAccessControlPolicy(bucket)
	errCode := s3a.checkAccessByOwnership(r, acp)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	err, ownership := s3a.GetBucketOwnership(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		} else {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}
	if ownership == "" {
		s3err.WriteErrorResponse(w, r, s3err.OwnershipControlsNotFoundError)
		return
	}

	result := &s3.PutBucketOwnershipControlsInput{
		OwnershipControls: &s3.OwnershipControls{
			Rules: []*s3.OwnershipControlsRule{
				{
					ObjectOwnership: &ownership,
				},
			},
		},
	}

	s3err.WriteAwsXMLResponse(w, r, http.StatusOK, result)
}

// DeleteBucketOwnershipControls https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketOwnershipControls.html
func (s3a *S3ApiServer) DeleteBucketOwnershipControls(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketOwnershipControls %s", bucket)

	acp := s3a.bacp.GetAccessControlPolicy(bucket)
	errCode := s3a.checkAccessByOwnership(r, acp)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}
	acp.ObjectOwnership = ""
	if err := s3a.SaveBucketAccessControlPoliciesConfig(); err != nil {
		glog.Errorf("Failed save bucket access control policies config to filer: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
	}
	s3err.WriteAwsXMLResponse(w, r, http.StatusOK, &s3.OwnershipControls{
		Rules: []*s3.OwnershipControlsRule{},
	})
}
