package s3api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/protobuf/proto"
)

func newMissingBucketConditionalTestServer(t *testing.T, filer filer_pb.SeaweedFilerServer) *S3ApiServer {
	t.Helper()
	s3a := newHeadBucketTestServer(t, filer)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	return s3a
}

func TestResolveObjectEntryMissingBucketError(t *testing.T) {
	s3a := newMissingBucketConditionalTestServer(t, &fakeLookupFiler{})

	entry, err := s3a.resolveObjectEntry("missing-bucket", "key", "")
	if entry != nil {
		t.Fatalf("entry = %v, want nil", entry)
	}
	if !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("err = %v, want errors.Is(err, ErrBucketNotFound)", err)
	}
	if !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("err = %v, want errors.Is(err, filer_pb.ErrNotFound)", err)
	}
}

func TestPutObjectConditionalHeadersOnMissingBucket(t *testing.T) {
	const bucket = "missing-bucket"
	const object = "obj"

	cases := []struct {
		name             string
		autoCreateBucket bool
		header           string
		value            string
	}{
		{
			name:             "If-Match ETag with AutoCreateBucket=false",
			autoCreateBucket: false,
			header:           "If-Match",
			value:            `"00000000000000000000000000000bad"`,
		},
		{
			name:             "If-Match wildcard with AutoCreateBucket=false",
			autoCreateBucket: false,
			header:           "If-Match",
			value:            "*",
		},
		{
			name:             "If-Match ETag with AutoCreateBucket=true",
			autoCreateBucket: true,
			header:           "If-Match",
			value:            `"00000000000000000000000000000bad"`,
		},
		{
			name:             "If-None-Match wildcard with AutoCreateBucket=false",
			autoCreateBucket: false,
			header:           "If-None-Match",
			value:            "*",
		},
		{
			name:             "If-Unmodified-Since with AutoCreateBucket=false",
			autoCreateBucket: false,
			header:           "If-Unmodified-Since",
			value:            time.Now().UTC().Add(24 * time.Hour).Format(http.TimeFormat),
		},
		{
			name:             "If-Modified-Since with AutoCreateBucket=false",
			autoCreateBucket: false,
			header:           "If-Modified-Since",
			value:            time.Now().UTC().Add(-24 * time.Hour).Format(http.TimeFormat),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s3a := newMissingBucketConditionalTestServer(t, &fakeLookupFiler{})
			s3a.option.AutoCreateBucket = tc.autoCreateBucket

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+object, strings.NewReader("payload"))
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
			req.Header.Set(tc.header, tc.value)
			rr := httptest.NewRecorder()

			s3a.PutObjectHandler(rr, req)

			if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "NoSuchBucket") {
				t.Fatalf("status = %d body = %q, want 404 NoSuchBucket", rr.Code, rr.Body.String())
			}
		})
	}

	// A non-If-Match condition passes against an absent object, so the request must
	// reach the handler's auto-create gate, which validates the bucket name. Answering
	// NoSuchBucket here would report a malformed name as merely missing.
	t.Run("malformed bucket name with If-None-Match wildcard reports InvalidBucketName", func(t *testing.T) {
		const malformedBucket = "ab"
		s3a := newMissingBucketConditionalTestServer(t, &fakeLookupFiler{})
		s3a.option.AutoCreateBucket = false

		req := httptest.NewRequest(http.MethodPut, "/"+malformedBucket+"/"+object, strings.NewReader("payload"))
		req = mux.SetURLVars(req, map[string]string{"bucket": malformedBucket, "object": object})
		req.Header.Set("If-None-Match", "*")
		rr := httptest.NewRecorder()

		s3a.PutObjectHandler(rr, req)

		if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "<Code>InvalidBucketName</Code>") {
			t.Fatalf("status = %d body = %q, want 400 InvalidBucketName", rr.Code, rr.Body.String())
		}
	})

	t.Run("checkConditionalHeaders with AutoCreateBucket=true leaves auto-create to the handler", func(t *testing.T) {
		future := time.Now().UTC().Add(24 * time.Hour).Format(http.TimeFormat)
		past := time.Now().UTC().Add(-24 * time.Hour).Format(http.TimeFormat)

		for _, header := range []struct {
			name  string
			value string
		}{
			{"If-None-Match", "*"},
			{"If-Unmodified-Since", future},
			{"If-Modified-Since", past},
		} {
			s3a := newMissingBucketConditionalTestServer(t, &fakeLookupFiler{})
			s3a.option.AutoCreateBucket = true

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+object, strings.NewReader("payload"))
			req.Header.Set(header.name, header.value)

			if errCode := s3a.checkConditionalHeaders(req, bucket, object); errCode != s3err.ErrNone {
				t.Fatalf("%s: errCode = %v, want ErrNone", header.name, errCode)
			}
		}
	})
}

func TestReadConditionalHeadersOnMissingBucket(t *testing.T) {
	const bucket = "missing-bucket"
	const object = "obj"

	future := time.Now().UTC().Add(24 * time.Hour).Format(http.TimeFormat)
	past := time.Now().UTC().Add(-24 * time.Hour).Format(http.TimeFormat)

	cases := []struct {
		name   string
		header string
		value  string
	}{
		{name: "If-Match ETag", header: "If-Match", value: `"00000000000000000000000000000bad"`},
		{name: "If-Match wildcard", header: "If-Match", value: "*"},
		{name: "If-Unmodified-Since", header: "If-Unmodified-Since", value: future},
		{name: "If-None-Match wildcard", header: "If-None-Match", value: "*"},
		{name: "If-Modified-Since", header: "If-Modified-Since", value: past},
	}

	for _, tc := range cases {
		t.Run("GET/"+tc.name, func(t *testing.T) {
			s3a := newMissingBucketConditionalTestServer(t, &fakeLookupFiler{})
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+object, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
			req.Header.Set(tc.header, tc.value)
			rr := httptest.NewRecorder()

			s3a.GetObjectHandler(rr, req)

			if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "NoSuchBucket") {
				t.Fatalf("GET status = %d body = %q, want 404 NoSuchBucket", rr.Code, rr.Body.String())
			}
		})

		t.Run("HEAD/"+tc.name, func(t *testing.T) {
			s3a := newMissingBucketConditionalTestServer(t, &fakeLookupFiler{})
			req := httptest.NewRequest(http.MethodHead, "/"+bucket+"/"+object, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
			req.Header.Set(tc.header, tc.value)

			res := s3a.checkConditionalHeadersForReads(req, bucket, object)
			if res.ErrorCode != s3err.ErrNoSuchBucket {
				t.Fatalf("checkConditionalHeadersForReads ErrorCode = %v, want ErrNoSuchBucket", res.ErrorCode)
			}

			rr := httptest.NewRecorder()
			s3a.HeadObjectHandler(rr, req)
			if rr.Code != http.StatusNotFound {
				t.Fatalf("HEAD status = %d, want 404", rr.Code)
			}
			if rr.Body.Len() != 0 {
				t.Fatalf("HEAD response must have an empty body, got %q", rr.Body.String())
			}
		})
	}

	t.Run("GET versionId with If-Match", func(t *testing.T) {
		const versionedObject = "some/obj"
		s3a := newMissingBucketConditionalTestServer(t, &fakeLookupFiler{})

		checkReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+versionedObject+"?versionId=v1", nil)
		checkReq = mux.SetURLVars(checkReq, map[string]string{"bucket": bucket, "object": versionedObject})
		checkReq.Header.Set("If-Match", `"00000000000000000000000000000bad"`)
		if res := s3a.checkConditionalHeadersForReads(checkReq, bucket, versionedObject); res.ErrorCode != s3err.ErrNoSuchBucket {
			t.Fatalf("checkConditionalHeadersForReads ErrorCode = %v, want ErrNoSuchBucket", res.ErrorCode)
		}

		req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+versionedObject+"?versionId=v1", nil)
		req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": versionedObject})
		req.Header.Set("If-Match", `"00000000000000000000000000000bad"`)
		rr := httptest.NewRecorder()

		s3a.GetObjectHandler(rr, req)

		if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "NoSuchBucket") {
			t.Fatalf("GET status = %d body = %q, want 404 NoSuchBucket", rr.Code, rr.Body.String())
		}
	})
}

func TestAnonymousGetConditionalOnMissingBucketReturnsAccessDenied(t *testing.T) {
	const bucket = "missing-bucket"
	const object = "obj"

	s3a, _ := newAnonymousReadTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+object, nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
	req.Header.Set("If-Match", `"00000000000000000000000000000bad"`)
	rr := httptest.NewRecorder()

	s3a.AuthWithPublicRead(s3a.GetObjectHandler, s3_constants.ACTION_READ)(rr, req)

	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "AccessDenied") {
		t.Fatalf("anonymous GET status = %d body = %q, want 403 AccessDenied", rr.Code, rr.Body.String())
	}
}

// A caller that may list the bucket can already learn which keys exist, so the
// missing bucket is answered as such rather than masked as AccessDenied.
func TestAnonymousGetConditionalOnMissingBucketListAllowed(t *testing.T) {
	const bucket = "missing-bucket"
	const object = "obj"

	future := time.Now().UTC().Add(24 * time.Hour).Format(http.TimeFormat)

	for _, header := range []struct {
		name  string
		value string
	}{
		{"If-Match", `"00000000000000000000000000000bad"`},
		{"If-Unmodified-Since", future},
	} {
		s3a := newMissingBucketConditionalTestServer(t, &fakeLookupFiler{})
		s3a.iam = &IdentityAccessManagement{isAuthEnabled: true}
		s3a.iam.identityAnonymous = &Identity{Name: s3_constants.AccountAnonymousId, Actions: []Action{s3_constants.ACTION_LIST}}
		s3a.policyEngine = NewBucketPolicyEngine()
		s3a.iam.policyEngine = s3a.policyEngine

		req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+object, nil)
		req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
		req.Header.Set(header.name, header.value)
		rr := httptest.NewRecorder()

		s3a.AuthWithPublicRead(s3a.GetObjectHandler, s3_constants.ACTION_READ)(rr, req)

		if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "NoSuchBucket") {
			t.Fatalf("%s: anonymous GET status = %d body = %q, want 404 NoSuchBucket", header.name, rr.Code, rr.Body.String())
		}

		headReq := httptest.NewRequest(http.MethodHead, "/"+bucket+"/"+object, nil)
		headReq = mux.SetURLVars(headReq, map[string]string{"bucket": bucket, "object": object})
		headReq.Header.Set(header.name, header.value)
		headRR := httptest.NewRecorder()

		s3a.AuthWithPublicRead(s3a.HeadObjectHandler, s3_constants.ACTION_READ)(headRR, headReq)

		if headRR.Code != http.StatusNotFound {
			t.Fatalf("%s: anonymous HEAD status = %d, want 404", header.name, headRR.Code)
		}
		if headRR.Body.Len() != 0 {
			t.Fatalf("%s: anonymous HEAD response must have an empty body, got %q", header.name, headRR.Body.String())
		}
	}
}

// autoCreateMissingBucketFiler keeps created entries, so a handler can resolve
// the bucket it just auto-created.
type autoCreateMissingBucketFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[string]*filer_pb.Entry
}

func (f *autoCreateMissingBucketFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := f.entries[path.Join(req.Directory, req.Name)]
	if entry == nil {
		return &filer_pb.LookupDirectoryEntryResponse{}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(entry).(*filer_pb.Entry)}, nil
}

func (f *autoCreateMissingBucketFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := proto.Clone(req.Entry).(*filer_pb.Entry)
	f.entries[path.Join(req.Directory, entry.Name)] = entry
	return &filer_pb.CreateEntryResponse{}, nil
}

// A conditional PUT the gate would auto-create for is not answered NoSuchBucket:
// the directory marker and the bucket are created by the same request.
func TestPutDirectoryMarkerConditionalAutoCreateMissingBucket(t *testing.T) {
	const bucket = "missing-bucket"

	filer := &autoCreateMissingBucketFiler{entries: make(map[string]*filer_pb.Entry)}
	s3a := newMissingBucketConditionalTestServer(t, filer)
	s3a.option.AutoCreateBucket = true
	s3a.iam = &IdentityAccessManagement{}

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/dir/", strings.NewReader(""))
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": "dir/"})
	req = req.WithContext(recordIdentityInContext(req, &Identity{Name: "admin", Actions: []Action{s3_constants.ACTION_ADMIN}}))
	req.Header.Set("If-None-Match", "*")
	rr := httptest.NewRecorder()

	s3a.PutObjectHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("PUT status = %d body = %q, want 200", rr.Code, rr.Body.String())
	}

	filer.mu.Lock()
	bucketEntry := filer.entries["/buckets/"+bucket]
	markerEntry := filer.entries["/buckets/"+bucket+"/dir"]
	filer.mu.Unlock()

	if bucketEntry == nil || !bucketEntry.IsDirectory {
		t.Fatalf("bucket entry = %v, want a created directory", bucketEntry)
	}
	if markerEntry == nil || !markerEntry.IsDirectory {
		t.Fatalf("object entry = %v, want a created directory marker", markerEntry)
	}
}
