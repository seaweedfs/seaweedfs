package s3api

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestPutObjectRetention(t *testing.T) {
	// Create a new S3ApiServer for testing
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/tmp/buckets",
		},
	}

	// Create a test request with a valid retention configuration
	retention := ObjectRetention{
		Mode:            s3_constants.RetentionModeCompliance,
		RetainUntilDate: &time.Time{},
	}
	xmlData, _ := xml.Marshal(retention)
	req := httptest.NewRequest(http.MethodPut, "/bucket/object?retention", bytes.NewReader(xmlData))
	req.Header.Set("Content-Type", "application/xml")

	// Create a test response recorder
	rr := httptest.NewRecorder()

	// Call the handler
	s3a.PutObjectRetentionHandler(rr, req)

	// Check the response status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
}