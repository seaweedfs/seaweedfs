package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// putBucketRequestPaymentMaxBodyBytes caps the request body for PutBucketRequestPayment
// to prevent DoS via large payloads. The valid payload is a few hundred bytes.
const putBucketRequestPaymentMaxBodyBytes = 64 * 1024

type policyStatusResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PolicyStatus"`
	IsPublic bool     `xml:"IsPublic"`
}

type accelerateConfigurationResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccelerateConfiguration"`
	Status  string   `xml:"Status"`
}

type bucketLoggingStatusResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ BucketLoggingStatus"`
}

type notificationConfigurationResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ NotificationConfiguration"`
}

// GetBucketPolicyStatusHandler reports whether the bucket policy grants public access.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html
func (s3a *S3ApiServer) GetBucketPolicyStatusHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	policyDoc, err := s3a.getBucketPolicy(bucket)
	if err != nil {
		if errors.Is(err, ErrPolicyNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketPolicy)
		} else if errors.Is(err, ErrBucketNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		} else {
			glog.Errorf("GetBucketPolicyStatusHandler load policy %s: %v", bucket, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}

	writeSuccessResponseXML(w, r, policyStatusResponse{IsPublic: isPolicyPublic(policyDoc)})
}

// isPolicyPublic returns true if any Allow statement grants access to "*" without restricting conditions.
func isPolicyPublic(doc *policy_engine.PolicyDocument) bool {
	if doc == nil {
		return false
	}
	for _, st := range doc.Statement {
		if st.Effect != policy_engine.PolicyEffectAllow {
			continue
		}
		if len(st.Condition) > 0 {
			continue
		}
		for _, p := range st.Principal.Strings() {
			if p == "*" {
				return true
			}
		}
	}
	return false
}

// PutBucketRequestPaymentHandler accepts only Payer=BucketOwner; Requester is rejected.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketRequestPayment.html
func (s3a *S3ApiServer) PutBucketRequestPaymentHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, putBucketRequestPaymentMaxBodyBytes)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	var cfg RequestPaymentConfiguration
	if err := xml.Unmarshal(body, &cfg); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	if cfg.Payer != "BucketOwner" {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	writeSuccessResponseEmpty(w, r)
}

// GetBucketAccelerateConfigurationHandler returns a static Suspended status.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAccelerateConfiguration.html
func (s3a *S3ApiServer) GetBucketAccelerateConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}
	writeSuccessResponseXML(w, r, accelerateConfigurationResponse{Status: "Suspended"})
}

// GetBucketLoggingHandler returns an empty BucketLoggingStatus (logging disabled).
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLogging.html
func (s3a *S3ApiServer) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}
	writeSuccessResponseXML(w, r, bucketLoggingStatusResponse{})
}

// GetBucketNotificationConfigurationHandler returns an empty configuration; SeaweedFS
// has no bucket event notifications, and AWS answers an unconfigured bucket the same way.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html
func (s3a *S3ApiServer) GetBucketNotificationConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}
	writeSuccessResponseXML(w, r, notificationConfigurationResponse{})
}

// GetBucketReplicationHandler reports that no replication configuration exists.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketReplication.html
func (s3a *S3ApiServer) GetBucketReplicationHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}
	s3err.WriteErrorResponse(w, r, s3err.ErrReplicationConfigurationNotFound)
}

// GetBucketWebsiteHandler reports that no website configuration exists.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketWebsite.html
func (s3a *S3ApiServer) GetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}
	s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchWebsiteConfiguration)
}

// listObjectsQueryParams are the query keys a ListObjects/ListObjectsV2 request may
// carry. x-id is stamped by the AWS SDKs and carries no meaning for the server; the
// rest of the non-listing keys are what a presigned URL signs into the query string.
var listObjectsQueryParams = map[string]bool{
	"prefix": true, "delimiter": true, "marker": true, "max-keys": true,
	"encoding-type": true, "list-type": true, "continuation-token": true,
	"start-after": true, "fetch-owner": true, "expected-bucket-owner": true,
	"allow-unordered": true, "x-id": true,
	// SigV2 presigned URLs.
	"AWSAccessKeyId": true, "Signature": true, "Expires": true,
}

// unroutedBucketSubresource names a query key on a bucket GET that no route claimed.
// Everything SeaweedFS implements is matched by its own route before the ListObjects
// catch-all, so anything left is a subresource it does not implement - and answering
// it with a bucket listing is worse than saying so.
func unroutedBucketSubresource(r *http.Request) (string, bool) {
	for key := range r.URL.Query() {
		if listObjectsQueryParams[key] || strings.HasPrefix(key, "X-Amz-") {
			continue
		}
		return key, true
	}
	return "", false
}
