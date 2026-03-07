package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestCopySourceWithExclamationMark(t *testing.T) {
	// Reproduce https://github.com/seaweedfs/seaweedfs/issues/8544
	testCases := []struct {
		name           string
		rawCopySource  string
		expectedBucket string
		expectedObject string
		dstBucket      string
		dstObject      string
		shouldBeEqual  bool
	}{
		{
			name:           "encoded exclamation mark - different dest",
			rawCopySource:  "my-bucket/path%2Fto%2FAnother%20test%21.odt",
			expectedBucket: "my-bucket",
			expectedObject: "path/to/Another test!.odt",
			dstBucket:      "my-bucket",
			dstObject:      "path/to/Hello.odt",
			shouldBeEqual:  false,
		},
		{
			name:           "unencoded exclamation mark - different dest",
			rawCopySource:  "my-bucket/path/to/Another%20test!.odt",
			expectedBucket: "my-bucket",
			expectedObject: "path/to/Another test!.odt",
			dstBucket:      "my-bucket",
			dstObject:      "path/to/Hello.odt",
			shouldBeEqual:  false,
		},
		{
			name:           "encoded exclamation mark - same dest",
			rawCopySource:  "my-bucket/path%2Fto%2FAnother%20test%21.odt",
			expectedBucket: "my-bucket",
			expectedObject: "path/to/Another test!.odt",
			dstBucket:      "my-bucket",
			dstObject:      "path/to/Another test!.odt",
			shouldBeEqual:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cpSrcPath, err := url.PathUnescape(tc.rawCopySource)
			if err != nil {
				cpSrcPath = tc.rawCopySource
			}

			srcBucket, srcObject, _ := pathToBucketObjectAndVersion(tc.rawCopySource, cpSrcPath)

			if srcBucket != tc.expectedBucket {
				t.Errorf("expected srcBucket=%q, got %q", tc.expectedBucket, srcBucket)
			}
			if srcObject != tc.expectedObject {
				t.Errorf("expected srcObject=%q, got %q", tc.expectedObject, srcObject)
			}

			isEqual := srcBucket == tc.dstBucket && srcObject == tc.dstObject
			if isEqual != tc.shouldBeEqual {
				t.Errorf("expected comparison result %v, got %v (srcObject=%q, dstObject=%q)",
					tc.shouldBeEqual, isEqual, srcObject, tc.dstObject)
			}
		})
	}
}

// TestCopySourceDecodingPlusSign verifies that + in the copy source header
// is treated as a literal plus sign (not a space), matching url.PathUnescape
// behavior. Using url.QueryUnescape would incorrectly convert + to space.
func TestCopySourceDecodingPlusSign(t *testing.T) {
	// Key: "path/to/file+name.odt"
	// With url.QueryUnescape, literal "+" → " " (WRONG for paths)
	// With url.PathUnescape, literal "+" → "+" (CORRECT)
	rawCopySource := "my-bucket/path/to/file+name.odt"

	// url.QueryUnescape would give "file name.odt" — WRONG
	queryDecoded, _ := url.QueryUnescape(rawCopySource)
	if queryDecoded == rawCopySource {
		t.Fatal("QueryUnescape should have changed + to space")
	}

	// url.PathUnescape preserves literal "+" — CORRECT
	pathDecoded, _ := url.PathUnescape(rawCopySource)
	if pathDecoded != rawCopySource {
		t.Fatalf("PathUnescape should have preserved +, got %q", pathDecoded)
	}

	_, srcObject, _ := pathToBucketObjectAndVersion(rawCopySource, pathDecoded)
	if srcObject != "path/to/file+name.odt" {
		t.Errorf("expected srcObject=%q, got %q", "path/to/file+name.odt", srcObject)
	}
}

// TestCopySourceRoutingWithSpecialChars tests that mux variable extraction
// correctly handles special characters like ! (%21) in both the URL path
// and the X-Amz-Copy-Source header.
func TestCopySourceRoutingWithSpecialChars(t *testing.T) {
	testCases := []struct {
		name          string
		dstURL        string // URL for the PUT request (destination)
		copySource    string // X-Amz-Copy-Source header value
		expectSameKey bool   // whether srcObject should equal dstObject
	}{
		{
			name:          "different keys, source has encoded !",
			dstURL:        "/my-bucket/path/to/Hello.odt",
			copySource:    "my-bucket/path%2Fto%2FAnother%20test%21.odt",
			expectSameKey: false,
		},
		{
			name:          "same key with !, source encoded, dest unencoded",
			dstURL:        "/my-bucket/path/to/Another%20test%21.odt",
			copySource:    "my-bucket/path%2Fto%2FAnother%20test%21.odt",
			expectSameKey: true,
		},
		{
			name:          "same key with !, both percent-encoded differently",
			dstURL:        "/my-bucket/path/to/Another%20test!.odt",
			copySource:    "my-bucket/path%2Fto%2FAnother%20test%21.odt",
			expectSameKey: true,
		},
		{
			name:          "key with + sign, source has literal +",
			dstURL:        "/my-bucket/path/to/file+name.odt",
			copySource:    "my-bucket/path/to/file+name.odt",
			expectSameKey: true,
		},
		{
			name:          "key with + sign, source has %2B",
			dstURL:        "/my-bucket/path/to/file+name.odt",
			copySource:    "my-bucket/path/to/file%2Bname.odt",
			expectSameKey: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedDstBucket, capturedDstObject string
			var capturedSrcBucket, capturedSrcObject string

			router := mux.NewRouter().SkipClean(true)
			bucket := router.PathPrefix("/{bucket}").Subrouter()
			bucket.Methods(http.MethodPut).Path("/{object:(?s).+}").
				HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").
				HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					capturedDstBucket, capturedDstObject = s3_constants.GetBucketAndObject(r)

					rawCopySource := r.Header.Get("X-Amz-Copy-Source")
					cpSrcPath, err := url.PathUnescape(rawCopySource)
					if err != nil {
						cpSrcPath = rawCopySource
					}
					capturedSrcBucket, capturedSrcObject, _ = pathToBucketObjectAndVersion(rawCopySource, cpSrcPath)

					if capturedSrcBucket == capturedDstBucket && capturedSrcObject == capturedDstObject {
						w.WriteHeader(http.StatusBadRequest)
						fmt.Fprintf(w, "ErrInvalidCopyDest")
					} else {
						w.WriteHeader(http.StatusOK)
						fmt.Fprintf(w, "OK")
					}
				})

			req, _ := http.NewRequest("PUT", tc.dstURL, nil)
			req.Header.Set("X-Amz-Copy-Source", tc.copySource)

			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			if tc.expectSameKey {
				if rr.Code != http.StatusBadRequest {
					t.Errorf("expected same key detection (400), got %d; src=%q dst=%q",
						rr.Code, capturedSrcObject, capturedDstObject)
				}
			} else {
				if rr.Code != http.StatusOK {
					t.Errorf("expected different keys (200), got %d; src=%q dst=%q",
						rr.Code, capturedSrcObject, capturedDstObject)
				}
			}
		})
	}
}
