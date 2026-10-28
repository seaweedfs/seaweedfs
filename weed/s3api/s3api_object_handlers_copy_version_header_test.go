package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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

func versionedCopyFixture(src, object, versionId string, filer *copyACLFiler) {
	filer.entries["/buckets/"+src+"/"+object+s3_constants.VersionsFolder] = &filer_pb.Entry{
		Name: object + s3_constants.VersionsFolder, IsDirectory: true,
		Extended: map[string][]byte{
			s3_constants.ExtLatestVersionIdKey:       []byte(versionId),
			s3_constants.ExtLatestVersionFileNameKey: []byte("v_" + versionId),
		},
	}
	filer.entries["/buckets/"+src+"/"+object+s3_constants.VersionsFolder+"/v_"+versionId] = &filer_pb.Entry{
		Name: "v_" + versionId, Content: []byte("data"),
		Attributes: &filer_pb.FuseAttributes{FileMode: defaultFileMode, FileSize: 4},
		Extended:   map[string][]byte{s3_constants.ExtVersionIdKey: []byte(versionId)},
	}
}

// A copy naming no version reads the latest one, resolved from the .versions
// pointer, and still reports it in x-amz-copy-source-version-id — even when the
// version file predates the version-id attribute.
func TestCopyObjectReportsLatestSourceVersionId(t *testing.T) {
	const src, dst, object, versionId = "versioned-src", "plain-dst", "object", "3a8f2c1d9e7b4a6f"
	filer := &copyACLFiler{ambiguousPutFiler: &ambiguousPutFiler{apply: true, entries: map[string]*filer_pb.Entry{}}}
	s3a := newPutTestServer(t, startFakeFiler(t, filer))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(src, &BucketConfig{Name: src, Versioning: s3_constants.VersioningEnabled, Ownership: s3_constants.OwnershipObjectWriter})
	s3a.bucketConfigCache.Set(dst, &BucketConfig{Name: dst, Ownership: s3_constants.OwnershipObjectWriter})
	versionedCopyFixture(src, object, versionId, filer)
	delete(filer.entries["/buckets/"+src+"/"+object+s3_constants.VersionsFolder+"/v_"+versionId].Extended, s3_constants.ExtVersionIdKey)

	req := httptest.NewRequest(http.MethodPut, "/"+dst+"/"+object, nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": dst, "object": object})
	req.Header.Set("X-Amz-Copy-Source", "/"+src+"/"+object)
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("x-amz-copy-source-version-id"); got != versionId {
		t.Fatalf("x-amz-copy-source-version-id = %q, want %q", got, versionId)
	}
}

// UploadPartCopy answers with the same header for a versioned source.
func TestCopyObjectPartReportsSourceVersionId(t *testing.T) {
	const src, dst, object, versionId = "versioned-src", "plain-dst", "object", "3a8f2c1d9e7b4a6f"
	filer := &copyACLFiler{ambiguousPutFiler: &ambiguousPutFiler{volume: startFakeVolumeServer(t), apply: true, entries: map[string]*filer_pb.Entry{}}}
	s3a := newPutTestServer(t, startFakeFiler(t, filer))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(src, &BucketConfig{Name: src, Versioning: s3_constants.VersioningEnabled, Ownership: s3_constants.OwnershipObjectWriter})
	s3a.bucketConfigCache.Set(dst, &BucketConfig{Name: dst, Ownership: s3_constants.OwnershipObjectWriter})
	util_http.InitGlobalHttpClient()
	versionedCopyFixture(src, object, versionId, filer)
	delete(filer.entries["/buckets/"+src+"/"+object+s3_constants.VersionsFolder+"/v_"+versionId].Extended, s3_constants.ExtVersionIdKey)

	uploadID := s3a.generateUploadID(object)
	filer.entries["/buckets/"+dst+"/.uploads/"+uploadID] = &filer_pb.Entry{
		Name:        uploadID,
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte(object)},
	}

	req := httptest.NewRequest(http.MethodPut, "/"+dst+"/"+object+"?partNumber=1&uploadId="+uploadID, nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": dst, "object": object})
	req.Header.Set("X-Amz-Copy-Source", "/"+src+"/"+object)
	rr := httptest.NewRecorder()
	s3a.CopyObjectPartHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("x-amz-copy-source-version-id"); got != versionId {
		t.Fatalf("x-amz-copy-source-version-id = %q, want %q", got, versionId)
	}
}

// A copy whose source is the null version — a regular-path object in a
// versioning-configured bucket — reports it as AWS does: "null".
func TestCopyObjectReportsNullSourceVersionId(t *testing.T) {
	const src, dst, object = "suspended-src", "plain-dst", "object"
	filer := &copyACLFiler{ambiguousPutFiler: &ambiguousPutFiler{apply: true, entries: map[string]*filer_pb.Entry{}}}
	s3a := newPutTestServer(t, startFakeFiler(t, filer))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(src, &BucketConfig{Name: src, Versioning: s3_constants.VersioningSuspended, Ownership: s3_constants.OwnershipObjectWriter})
	s3a.bucketConfigCache.Set(dst, &BucketConfig{Name: dst, Ownership: s3_constants.OwnershipObjectWriter})
	filer.entries["/buckets/"+src+"/"+object] = &filer_pb.Entry{
		Name: object, Content: []byte("data"),
		Attributes: &filer_pb.FuseAttributes{FileMode: defaultFileMode, FileSize: 4},
	}

	for _, copySource := range []string{"/" + src + "/" + object, "/" + src + "/" + object + "?versionId=null"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/"+dst+"/"+object, nil)
		req = mux.SetURLVars(req, map[string]string{"bucket": dst, "object": object})
		req.Header.Set("X-Amz-Copy-Source", copySource)
		s3a.CopyObjectHandler(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("copySource %q: status = %d; body: %s", copySource, rr.Code, rr.Body.String())
		}
		if got := rr.Header().Get("x-amz-copy-source-version-id"); got != "null" {
			t.Fatalf("copySource %q: x-amz-copy-source-version-id = %q, want %q", copySource, got, "null")
		}
	}
}

// An unversioned source reports no copy-source version header at all.
func TestCopyObjectOmitsSourceVersionIdWhenUnversioned(t *testing.T) {
	const src, dst, object = "plain-src", "plain-dst", "object"
	filer := &copyACLFiler{ambiguousPutFiler: &ambiguousPutFiler{apply: true, entries: map[string]*filer_pb.Entry{}}}
	s3a := newPutTestServer(t, startFakeFiler(t, filer))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(src, &BucketConfig{Name: src, Ownership: s3_constants.OwnershipObjectWriter})
	s3a.bucketConfigCache.Set(dst, &BucketConfig{Name: dst, Ownership: s3_constants.OwnershipObjectWriter})
	filer.entries["/buckets/"+src+"/"+object] = &filer_pb.Entry{
		Name: object, Content: []byte("data"),
		Attributes: &filer_pb.FuseAttributes{FileMode: defaultFileMode, FileSize: 4},
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/"+dst+"/"+object, nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": dst, "object": object})
	req.Header.Set("X-Amz-Copy-Source", "/"+src+"/"+object)
	s3a.CopyObjectHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("x-amz-copy-source-version-id"); got != "" {
		t.Fatalf("x-amz-copy-source-version-id = %q, want empty", got)
	}
}
