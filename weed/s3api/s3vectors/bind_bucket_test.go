package s3vectors

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// A vector bucket policy write authorizes against the bucket it read: when the
// bucket is deleted and created again by another account before the write's
// compare-and-swap reads the entry, the write must not land on the new bucket.
func TestVectorBucketPolicyWritesKeepToAuthorizedBucket(t *testing.T) {
	const policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectorBucket","Resource":"*"}]}`
	cases := []struct {
		name string
		call func(s *Server) *httptest.ResponseRecorder
	}{
		{"put", func(s *Server) *httptest.ResponseRecorder {
			return postJSONAs(t, "", s.handlePutVectorBucketPolicy, &PutVectorBucketPolicyRequest{VectorBucketName: "vec-bucket", Policy: policy})
		}},
		{"delete", func(s *Server) *httptest.ResponseRecorder {
			return postJSONAs(t, "", s.handleDeleteVectorBucketPolicy, &DeleteVectorBucketPolicyRequest{VectorBucketName: "vec-bucket"})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := s3tablestest.Start(t)
			s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
			if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
				t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
			}
			if w := postJSONAs(t, "", s.handlePutVectorBucketPolicy, &PutVectorBucketPolicyRequest{VectorBucketName: "vec-bucket", Policy: policy}); w.Code != http.StatusOK {
				t.Fatalf("seed policy: %d %s", w.Code, w.Body.String())
			}

			dir, name := splitEntryPath(s3tables.GetTableBucketPath("vec-bucket"))
			lookups := 0
			fs.OnLookup = func(d, n string) {
				if d == dir && n == name {
					lookups++
				}
			}
			if w := tc.call(s); w.Code != http.StatusOK {
				t.Fatalf("baseline %s: %d %s", tc.name, w.Code, w.Body.String())
			}
			fs.OnLookup = nil
			if w := postJSONAs(t, "", s.handlePutVectorBucketPolicy, &PutVectorBucketPolicyRequest{VectorBucketName: "vec-bucket", Policy: policy}); w.Code != http.StatusOK {
				t.Fatalf("reseed policy: %d %s", w.Code, w.Body.String())
			}
			// The last bucket lookup is the compare-and-swap read; replace the
			// bucket with another account's right before it.
			last, seen := lookups, 0
			fs.OnLookup = func(d, n string) {
				if d != dir || n != name {
					return
				}
				if seen++; seen != last {
					return
				}
				entry := fs.Get(dir, name)
				var meta map[string]interface{}
				if err := json.Unmarshal(entry.Extended[s3tables.ExtendedKeyMetadata], &meta); err != nil {
					t.Errorf("decode bucket metadata: %v", err)
					return
				}
				meta["ownerAccountId"] = "bob"
				meta["createdAt"] = time.Now().Add(time.Hour).UTC()
				data, _ := json.Marshal(meta)
				ext := make(map[string][]byte, len(entry.Extended))
				for k, v := range entry.Extended {
					ext[k] = v
				}
				ext[s3tables.ExtendedKeyMetadata] = data
				ext[vectorPolicyKey] = []byte("bob-policy")
				fs.Put(dir, name, ext)
			}
			w := tc.call(s)
			fs.OnLookup = nil
			if w.Code != http.StatusConflict {
				t.Fatalf("%s over a replaced bucket = %d %s, want 409", tc.name, w.Code, w.Body.String())
			}
			if got := string(fs.Get(dir, name).Extended[vectorPolicyKey]); got != "bob-policy" {
				t.Fatalf("%s rewrote the replacing bucket's policy: %q", tc.name, got)
			}
		})
	}
}

// replaceBucket swaps the bucket's record for another account's bucket of the
// same name, keeping its contents.
func replaceBucket(t *testing.T, fs *s3tablestest.MemFiler, bucket string) {
	dir, name := splitEntryPath(s3tables.GetTableBucketPath(bucket))
	entry := fs.Get(dir, name)
	var meta map[string]interface{}
	if err := json.Unmarshal(entry.Extended[s3tables.ExtendedKeyMetadata], &meta); err != nil {
		t.Errorf("decode bucket metadata: %v", err)
		return
	}
	meta["ownerAccountId"] = "bob"
	meta["createdAt"] = time.Now().Add(time.Hour).UTC()
	data, _ := json.Marshal(meta)
	ext := make(map[string][]byte, len(entry.Extended))
	for k, v := range entry.Extended {
		ext[k] = v
	}
	ext[s3tables.ExtendedKeyMetadata] = data
	fs.Put(dir, name, ext)
}

// replaceBucketAfterAuthorization replaces bucket right after the lookups
// authorizeVector makes, counted on a probe bucket of the same shape.
func replaceBucketAfterAuthorization(t *testing.T, fs *s3tablestest.MemFiler, s *Server, bucket string) {
	if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-probe"}); w.Code != http.StatusOK {
		t.Fatalf("create probe bucket: %d %s", w.Code, w.Body.String())
	}
	probeDir, probeName := splitEntryPath(s3tables.GetTableBucketPath("vec-probe"))
	authLookups := 0
	fs.OnLookup = func(d, n string) {
		if d == probeDir && n == probeName {
			authLookups++
		}
	}
	probe := httptest.NewRequest(http.MethodPost, "/", nil)
	probe = probe.WithContext(contextWithIdentity(probe.Context(), ""))
	if _, err := s.authorizeVector(probe.Context(), probe, "s3vectors:DeleteVectorBucket", "vec-probe", ""); err != nil {
		t.Fatalf("authorize probe: %v", err)
	}

	dir, name := splitEntryPath(s3tables.GetTableBucketPath(bucket))
	seen := 0
	fs.OnLookup = func(d, n string) {
		if d == dir && n == name {
			if seen++; seen == authLookups+1 {
				replaceBucket(t, fs, bucket)
			}
		}
	}
}

// DeleteVectorBucket deletes only the bucket it authorized: one deleted and
// created again by another account in between survives.
func TestDeleteVectorBucketKeepsReplacingBucket(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	replaceBucketAfterAuthorization(t, fs, s, "vec-bucket")
	w := postJSONAs(t, "", s.handleDeleteVectorBucket, &DeleteVectorBucketRequest{VectorBucketName: "vec-bucket"})
	fs.OnLookup = nil
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete over a replaced bucket = %d %s, want 404", w.Code, w.Body.String())
	}
	dir, name := splitEntryPath(s3tables.GetTableBucketPath("vec-bucket"))
	if fs.Get(dir, name) == nil {
		t.Fatal("DeleteVectorBucket removed the bucket that replaced the authorized one")
	}
}

// CreateIndex and DeleteIndex write only into the bucket they authorized.
func TestIndexWritesKeepToAuthorizedBucket(t *testing.T) {
	newServer := func(t *testing.T) (*s3tablestest.MemFiler, *Server) {
		fs := s3tablestest.Start(t)
		s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
		if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
			t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
		}
		return fs, s
	}
	createIndex := func(s *Server) *httptest.ResponseRecorder {
		return postJSONAs(t, "", s.handleCreateIndex, &CreateIndexRequest{
			VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "euclidean",
		})
	}
	indexDir, indexName := splitEntryPath(s3tables.GetTablePath("vec-bucket", indexNamespace, "idx"))

	t.Run("create", func(t *testing.T) {
		fs, s := newServer(t)
		replaceBucketAfterAuthorization(t, fs, s, "vec-bucket")
		w := createIndex(s)
		fs.OnLookup = nil
		if w.Code != http.StatusNotFound {
			t.Fatalf("create over a replaced bucket = %d %s, want 404", w.Code, w.Body.String())
		}
		if fs.Get(indexDir, indexName) != nil {
			t.Fatal("CreateIndex wrote an index into the bucket that replaced the authorized one")
		}
	})
	t.Run("delete", func(t *testing.T) {
		fs, s := newServer(t)
		if w := createIndex(s); w.Code != http.StatusOK {
			t.Fatalf("create index: %d %s", w.Code, w.Body.String())
		}
		replaceBucketAfterAuthorization(t, fs, s, "vec-bucket")
		w := postJSONAs(t, "", s.handleDeleteIndex, &DeleteIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx"})
		fs.OnLookup = nil
		if w.Code != http.StatusNotFound {
			t.Fatalf("delete over a replaced bucket = %d %s, want 404", w.Code, w.Body.String())
		}
		if fs.Get(indexDir, indexName) == nil {
			t.Fatal("DeleteIndex removed an index of the bucket that replaced the authorized one")
		}
	})
}

// DeleteVectorBucket drops the empty index namespace only from the bucket it
// authorized: a bucket created again before that cleanup keeps its own.
func TestDeleteVectorBucketCleanupKeepsReplacingBucket(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "euclidean",
	}); w.Code != http.StatusOK {
		t.Fatalf("create index: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleDeleteIndex, &DeleteIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx"}); w.Code != http.StatusOK {
		t.Fatalf("delete index: %d %s", w.Code, w.Body.String())
	}

	bucketPath := s3tables.GetTableBucketPath("vec-bucket")
	fs.OnLookup = func(d, n string) {
		if d == bucketPath && n == indexNamespace {
			fs.OnLookup = nil
			replaceBucket(t, fs, "vec-bucket")
		}
	}
	w := postJSONAs(t, "", s.handleDeleteVectorBucket, &DeleteVectorBucketRequest{VectorBucketName: "vec-bucket"})
	fs.OnLookup = nil
	if w.Code == http.StatusOK {
		t.Fatalf("delete over a replaced bucket succeeded: %s", w.Body.String())
	}
	if fs.Get(bucketPath, indexNamespace) == nil {
		t.Fatal("DeleteVectorBucket removed the index namespace of the bucket that replaced the authorized one")
	}
}
