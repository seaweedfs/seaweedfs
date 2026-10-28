package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// CopyObject reports the source version it read in x-amz-copy-source-version-id.
func TestCopyObjectReportsSourceVersionId(t *testing.T) {
	const src, dst, object, versionId = "versioned-src", "plain-dst", "object", "3a8f2c1d9e7b4a6f"
	filer := &copyACLFiler{ambiguousPutFiler: &ambiguousPutFiler{apply: true, entries: map[string]*filer_pb.Entry{}}}
	s3a := newPutTestServer(t, startFakeFiler(t, filer))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(src, &BucketConfig{Name: src, Versioning: s3_constants.VersioningEnabled, Ownership: s3_constants.OwnershipObjectWriter})
	s3a.bucketConfigCache.Set(dst, &BucketConfig{Name: dst, Ownership: s3_constants.OwnershipObjectWriter})
	filer.entries["/buckets/"+src+"/"+object+s3_constants.VersionsFolder+"/v_"+versionId] = &filer_pb.Entry{
		Name: "v_" + versionId, Content: []byte("data"),
		Attributes: &filer_pb.FuseAttributes{FileMode: defaultFileMode, FileSize: 4},
		Extended:   map[string][]byte{s3_constants.ExtVersionIdKey: []byte(versionId)},
	}

	req := httptest.NewRequest(http.MethodPut, "/"+dst+"/"+object, nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": dst, "object": object})
	req.Header.Set("X-Amz-Copy-Source", "/"+src+"/"+object+"?versionId="+versionId)
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("x-amz-copy-source-version-id"); got != versionId {
		t.Fatalf("x-amz-copy-source-version-id = %q, want %q", got, versionId)
	}
}
