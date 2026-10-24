package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"google.golang.org/protobuf/proto"
)

// A bucket's tag set holds up to 50 tags, unlike an object's 10, and no key
// twice.
func TestPutBucketTaggingTagCountLimit(t *testing.T) {
	const bucket = "tag-limit-bucket"
	for _, tc := range []struct {
		tags      int
		repeatKey bool // the last tag repeats the first tag's key
		emptyKey  bool // the first tag has an empty key
		wantCode  int
	}{
		{tags: 10, wantCode: http.StatusOK},
		{tags: 11, wantCode: http.StatusOK},
		{tags: 50, wantCode: http.StatusOK},
		{tags: 51, wantCode: http.StatusBadRequest},
		{tags: 2, repeatKey: true, wantCode: http.StatusBadRequest},
		{tags: 51, repeatKey: true, wantCode: http.StatusBadRequest},
		{tags: 1, emptyKey: true, wantCode: http.StatusBadRequest},
	} {
		name := fmt.Sprintf("%d tags", tc.tags)
		if tc.repeatKey {
			name += " with a repeated key"
		}
		if tc.emptyKey {
			name += " with an empty key"
		}
		t.Run(name, func(t *testing.T) {
			filer := &fakeTxnFiler{}
			s3a := newHeadBucketTestServer(t, filer)
			s3a.iam = &IdentityAccessManagement{isAuthEnabled: true}
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket})

			var body strings.Builder
			body.WriteString("<Tagging><TagSet>")
			for i := 0; i < tc.tags; i++ {
				key := fmt.Sprintf("key%d", i)
				if tc.repeatKey && i == tc.tags-1 {
					key = "key0"
				}
				if tc.emptyKey && i == 0 {
					key = ""
				}
				fmt.Fprintf(&body, "<Tag><Key>%s</Key><Value>value%d</Value></Tag>", key, i)
			}
			body.WriteString("</TagSet></Tagging>")

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?tagging", strings.NewReader(body.String()))
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
			rr := httptest.NewRecorder()
			s3a.PutBucketTaggingHandler(rr, req)

			if rr.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantCode != http.StatusOK {
				if !strings.Contains(rr.Body.String(), "<Code>InvalidTag</Code>") {
					t.Fatalf("body = %s, want InvalidTag", rr.Body.String())
				}
				if filer.lastReq != nil {
					t.Fatalf("rejected tag set was written to the filer")
				}
				return
			}
			if filer.lastReq == nil || len(filer.lastReq.Mutations) != 1 {
				t.Fatalf("expected one bucket metadata write, got %+v", filer.lastReq)
			}
			var stored s3_pb.BucketMetadata
			if err := proto.Unmarshal(filer.lastReq.Mutations[0].Content, &stored); err != nil {
				t.Fatalf("decode stored bucket metadata: %v", err)
			}
			if len(stored.Tags) != tc.tags {
				t.Fatalf("stored %d tags, want %d", len(stored.Tags), tc.tags)
			}
		})
	}
}
