package s3api

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/protobuf/proto"
)

// AWS changes an object's storage class by copying the object onto itself
// with the COPY directive and a new x-amz-storage-class. Only a self-copy that
// changes nothing is rejected, and a bucket's default encryption is no change.
// A self-copy that would also change the encryption stays rejected.
func TestCopyObjectToItselfChangingStorageClass(t *testing.T) {
	const bucket, object = "self-copy", "object"
	kmsObject := map[string]string{
		s3_constants.AmzServerSideEncryption:            s3_constants.SSEAlgorithmKMS,
		s3_constants.AmzServerSideEncryptionAwsKmsKeyId: "key-a",
		s3_constants.AmzServerSideEncryptionContext:     base64.StdEncoding.EncodeToString([]byte(`{"env":"old"}`)),
	}
	for _, tt := range []struct {
		name              string
		stored            map[string]string // extra object metadata
		storageClass      string
		headers           map[string]string // extra request headers
		defaultEncryption bool
		wantCode          int
		wantErr           string
	}{
		{name: "no change", wantCode: http.StatusBadRequest, wantErr: "InvalidRequest"},
		{name: "no change under default encryption", defaultEncryption: true, wantCode: http.StatusBadRequest, wantErr: "InvalidRequest"},
		{name: "invalid storage class", storageClass: "NOT_A_CLASS", wantCode: http.StatusBadRequest, wantErr: "InvalidStorageClass"},
		{name: "new storage class", storageClass: "STANDARD_IA", wantCode: http.StatusOK},
		{name: "current storage class", stored: map[string]string{s3_constants.AmzStorageClass: "STANDARD_IA"},
			storageClass: "STANDARD_IA", wantCode: http.StatusBadRequest, wantErr: "InvalidRequest"},
		{name: "STANDARD without a stored class", storageClass: "STANDARD", wantCode: http.StatusBadRequest, wantErr: "InvalidRequest"},
		{name: "encryption only", headers: map[string]string{s3_constants.AmzServerSideEncryption: "AES256"},
			wantCode: http.StatusBadRequest, wantErr: "InvalidRequest"},
		{name: "new storage class and SSE-KMS context", stored: kmsObject, storageClass: "STANDARD_IA", headers: map[string]string{
			s3_constants.AmzServerSideEncryption:            s3_constants.SSEAlgorithmKMS,
			s3_constants.AmzServerSideEncryptionAwsKmsKeyId: "key-a",
			s3_constants.AmzServerSideEncryptionContext:     base64.StdEncoding.EncodeToString([]byte(`{"env":"new"}`)),
		}, wantCode: http.StatusBadRequest, wantErr: "InvalidRequest"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			filer := &copyACLFiler{ambiguousPutFiler: &ambiguousPutFiler{apply: true, entries: map[string]*filer_pb.Entry{}}}
			s3a := newPutTestServer(t, startFakeFiler(t, filer))
			s3a.iam = &IdentityAccessManagement{}
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			config := &BucketConfig{Name: bucket, Ownership: s3_constants.OwnershipObjectWriter}
			if tt.defaultEncryption {
				config.Encryption = &s3_pb.EncryptionConfiguration{SseAlgorithm: s3_constants.SSEAlgorithmAES256}
			}
			s3a.bucketConfigCache.Set(bucket, config)
			objectPath := "/buckets/" + bucket + "/" + object
			original := &filer_pb.Entry{Name: object, Content: []byte("data"),
				Attributes: &filer_pb.FuseAttributes{FileMode: defaultFileMode, FileSize: 4, Mime: "text/plain"},
				Extended:   map[string][]byte{"X-Amz-Meta-Owner": []byte("kept")}}
			for k, v := range tt.stored {
				original.Extended[k] = []byte(v)
			}
			filer.entries[objectPath] = proto.Clone(original).(*filer_pb.Entry)

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+object, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
			req.Header.Set("X-Amz-Copy-Source", "/"+bucket+"/"+object)
			req.Header.Set(s3_constants.AmzUserMetaDirective, DirectiveCopy)
			if tt.storageClass != "" {
				req.Header.Set(s3_constants.AmzStorageClass, tt.storageClass)
			}
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			rr := httptest.NewRecorder()
			s3a.CopyObjectHandler(rr, req)

			if rr.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d; body: %s", rr.Code, tt.wantCode, rr.Body.String())
			}
			filer.mu.Lock()
			defer filer.mu.Unlock()
			stored := filer.entries[objectPath]
			if tt.wantCode != http.StatusOK {
				if !strings.Contains(rr.Body.String(), "<Code>"+tt.wantErr+"</Code>") {
					t.Fatalf("body = %s, want %s", rr.Body.String(), tt.wantErr)
				}
				if !proto.Equal(stored, original) {
					t.Fatalf("rejected copy changed the object: %v", stored)
				}
				return
			}
			if got := string(stored.Extended[s3_constants.AmzStorageClass]); got != tt.storageClass {
				t.Fatalf("storage class = %q, want %q", got, tt.storageClass)
			}
			if got := string(stored.Extended["X-Amz-Meta-Owner"]); got != "kept" {
				t.Fatalf("user metadata = %q, want it kept under the COPY directive", got)
			}
			if got := string(stored.Content); got != "data" {
				t.Fatalf("content = %q, want %q", got, "data")
			}
		})
	}
}
