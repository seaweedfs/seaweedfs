package s3api

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

func TestPutObjectMalformedCacheControlIsInvalidRequest(t *testing.T) {
	for _, value := range []string{"max-age=", "max-age=A", "public, max-age=abc"} {
		t.Run(value, func(t *testing.T) {
			s3a, filer, volume := newHeaderTestServer(t, "")
			req, probe := newHeaderTestRequest("/b/o", "o", "hello world", "Cache-Control", value)

			s3a.PutObjectHandler(probe, req)

			requireRejected(t, probe, volume, s3err.ErrInvalidRequest)
			filer.mu.Lock()
			defer filer.mu.Unlock()
			require.Empty(t, filer.entries)
		})
	}
}

func TestPutObjectStoresValidCacheControl(t *testing.T) {
	s3a, filer, _ := newHeaderTestServer(t, "")
	req, probe := newHeaderTestRequest("/b/o", "o", "hello world", "Cache-Control", "public, max-age=60")

	s3a.PutObjectHandler(probe, req)

	require.Equal(t, http.StatusOK, probe.Code, probe.Body.String())
	filer.mu.Lock()
	defer filer.mu.Unlock()
	entry := filer.entries["/buckets/b/o"]
	require.NotNil(t, entry)
	require.Equal(t, "public, max-age=60", string(entry.Extended["Cache-Control"]))
}
