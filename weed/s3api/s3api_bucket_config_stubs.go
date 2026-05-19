package s3api

import (
	"encoding/xml"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Stub responses for bucket configuration sub-resources that SeaweedFS does
// not store (analytics, inventory, intelligent-tiering, metrics). AWS SDKs
// probe these on bucket discovery; returning a well-formed empty list keeps
// them happy instead of failing with MethodNotAllowed.

const s3XMLNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type listBucketAnalyticsConfigurationsResult struct {
	XMLName     xml.Name `xml:"ListBucketAnalyticsConfigurationsResult"`
	Xmlns       string   `xml:"xmlns,attr"`
	IsTruncated bool     `xml:"IsTruncated"`
}

type listInventoryConfigurationsResult struct {
	XMLName     xml.Name `xml:"ListInventoryConfigurationsResult"`
	Xmlns       string   `xml:"xmlns,attr"`
	IsTruncated bool     `xml:"IsTruncated"`
}

type listBucketIntelligentTieringConfigurationsResult struct {
	XMLName     xml.Name `xml:"ListBucketIntelligentTieringConfigurationsResult"`
	Xmlns       string   `xml:"xmlns,attr"`
	IsTruncated bool     `xml:"IsTruncated"`
}

type listBucketMetricsConfigurationsResult struct {
	XMLName     xml.Name `xml:"ListBucketMetricsConfigurationsResult"`
	Xmlns       string   `xml:"xmlns,attr"`
	IsTruncated bool     `xml:"IsTruncated"`
}

// stubBucketGuard fails with NoSuchBucket when the bucket does not exist, so
// that AWS's documented precedence (bucket lookup before sub-resource lookup)
// is preserved across these stub endpoints.
func (s3a *S3ApiServer) stubBucketGuard(w http.ResponseWriter, r *http.Request) bool {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return false
	}
	return true
}

func (s3a *S3ApiServer) GetAnalyticsConfiguration(w http.ResponseWriter, r *http.Request) {
	if !s3a.stubBucketGuard(w, r) {
		return
	}
	s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchConfiguration)
}

func (s3a *S3ApiServer) ListBucketAnalyticsConfigurations(w http.ResponseWriter, r *http.Request) {
	if !s3a.stubBucketGuard(w, r) {
		return
	}
	writeSuccessResponseXML(w, r, listBucketAnalyticsConfigurationsResult{Xmlns: s3XMLNamespace})
}

func (s3a *S3ApiServer) GetInventoryConfiguration(w http.ResponseWriter, r *http.Request) {
	if !s3a.stubBucketGuard(w, r) {
		return
	}
	s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchConfiguration)
}

func (s3a *S3ApiServer) ListBucketInventoryConfigurations(w http.ResponseWriter, r *http.Request) {
	if !s3a.stubBucketGuard(w, r) {
		return
	}
	writeSuccessResponseXML(w, r, listInventoryConfigurationsResult{Xmlns: s3XMLNamespace})
}

func (s3a *S3ApiServer) GetIntelligentTieringConfiguration(w http.ResponseWriter, r *http.Request) {
	if !s3a.stubBucketGuard(w, r) {
		return
	}
	s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchConfiguration)
}

func (s3a *S3ApiServer) ListBucketIntelligentTieringConfigurations(w http.ResponseWriter, r *http.Request) {
	if !s3a.stubBucketGuard(w, r) {
		return
	}
	writeSuccessResponseXML(w, r, listBucketIntelligentTieringConfigurationsResult{Xmlns: s3XMLNamespace})
}

func (s3a *S3ApiServer) GetMetricsConfiguration(w http.ResponseWriter, r *http.Request) {
	if !s3a.stubBucketGuard(w, r) {
		return
	}
	s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchConfiguration)
}

func (s3a *S3ApiServer) ListBucketMetricsConfigurations(w http.ResponseWriter, r *http.Request) {
	if !s3a.stubBucketGuard(w, r) {
		return
	}
	writeSuccessResponseXML(w, r, listBucketMetricsConfigurationsResult{Xmlns: s3XMLNamespace})
}
