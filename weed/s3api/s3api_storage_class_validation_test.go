package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

func (f *fakeVolumeServer) storedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.stored)
}

// An invalid x-amz-storage-class must be rejected before the body is
// uploaded, so a failed PUT or UploadPart leaves no chunks behind.
func TestPutToFilerStorageClass(t *testing.T) {
	for _, tt := range []struct {
		name       string
		class      string
		part       bool
		wantCode   s3err.ErrorCode
		wantStored int
	}{
		{name: "invalid", class: "BOGUS", wantCode: s3err.ErrInvalidStorageClass},
		{name: "invalid on a part", class: "BOGUS", part: true, wantCode: s3err.ErrInvalidStorageClass},
		{name: "valid", class: "STANDARD_IA", wantCode: s3err.ErrNone, wantStored: 1},
		{name: "absent", wantCode: s3err.ErrNone, wantStored: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			volume := startFakeVolumeServer(t)
			filer := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
			s3a := newPutTestServer(t, startFakeFiler(t, filer))

			r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
			if tt.class != "" {
				r.Header.Set(s3_constants.AmzStorageClass, tt.class)
			}
			filePath, object, dest := "/buckets/b/o", "o", ""
			if tt.part {
				r = withMultipartPart(r)
				filePath, object, dest = "/buckets/b/.uploads/u/0001.part", "", "/buckets/b/o"
			}
			_, code, _ := s3a.putToFiler(r, filePath, strings.NewReader("hello world"), "b", object, 1, 0, nil, false, dest)

			require.Equal(t, tt.wantCode, code)
			require.Equal(t, tt.wantStored, volume.storedCount(), "chunks stored on the volume server")
			if tt.wantCode != s3err.ErrNone {
				require.Empty(t, filer.entries)
				return
			}
			entry := filer.entries["/buckets/b/o"]
			require.NotNil(t, entry)
			require.Equal(t, tt.class, string(entry.Extended[s3_constants.AmzStorageClass]))
		})
	}
}
