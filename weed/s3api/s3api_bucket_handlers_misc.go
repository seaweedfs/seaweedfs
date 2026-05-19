package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}
	defer r.Body.Close()

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
	writeSuccessResponseXML(w, r, accelerateConfigurationResponse{Status: "Suspended"})
}

// GetBucketLoggingHandler returns an empty BucketLoggingStatus (logging disabled).
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLogging.html
func (s3a *S3ApiServer) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseXML(w, r, bucketLoggingStatusResponse{})
}
