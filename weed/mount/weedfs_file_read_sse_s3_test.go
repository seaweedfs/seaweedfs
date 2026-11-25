package mount

import (
	"bytes"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestNativeReadSSES3(t *testing.T) {
	content := []byte("test payload")
	header := s3_constants.AmzServerSideEncryption
	key := s3_constants.SeaweedFSSSES3Key
	for _, tc := range []struct {
		name     string
		extended map[string][]byte
		chunks   []*filer_pb.FileChunk
		want     fuse.Status
	}{
		{name: "plain", want: fuse.OK},
		{name: "stale algorithm header", extended: map[string][]byte{header: []byte("AES256")}, want: fuse.OK},
		{name: "native cipher key", chunks: []*filer_pb.FileChunk{{CipherKey: []byte("native test key")}}, want: fuse.OK},
		{name: "object metadata", extended: map[string][]byte{header: []byte("AES256"), key: []byte("test metadata")}, want: fuse.ENOTSUP},
		{name: "multipart chunk metadata", chunks: []*filer_pb.FileChunk{{}, {SseType: filer_pb.SSEType_SSE_S3, SseMetadata: []byte("test metadata")}}, want: fuse.ENOTSUP},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wfs := newInvalidateTestWFS(t)
			wfs.openMtimeCache = make(map[uint64][2]int64)
			inode := wfs.inodeToPath.Lookup(util.FullPath("/file"), 1, false, false, 0, false)
			entry := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(content)), FileMode: 0o644}, Content: content}
			fh, id, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, entry, 0, 0)
			defer wfs.ReleaseHandle(id)

			read := &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: uint64(id), Offset: 1, Size: 4}
			buf := make([]byte, read.Size)
			if _, status := wfs.Read(nil, read, buf); status != fuse.OK {
				t.Fatalf("initial read: %v", status)
			}
			// A reader may already hold the inode when S3 replaces the object.
			fh.SetEntry(&filer_pb.Entry{Name: "file", Attributes: entry.Attributes, Content: content, Extended: tc.extended, Chunks: tc.chunks})
			clear(buf)
			data, status := wfs.Read(nil, read, buf)
			if status != tc.want {
				t.Fatalf("read status = %v, want %v", status, tc.want)
			}
			if tc.want == fuse.OK {
				if !bytes.Equal(buf, content[1:5]) {
					t.Errorf("read = %q, want %q", buf, content[1:5])
				}
			} else if data != nil || !bytes.Equal(buf, make([]byte, len(buf))) {
				t.Error("unsupported read returned file bytes")
			}

			out := &fuse.OpenOut{}
			status = wfs.Open(nil, &fuse.OpenIn{InHeader: read.InHeader, Flags: syscall.O_RDONLY}, out)
			if status == fuse.OK {
				defer wfs.ReleaseHandle(FileHandleId(out.Fh))
			}
			if status != tc.want {
				t.Errorf("open status = %v, want %v", status, tc.want)
			}
			if tc.want == fuse.ENOTSUP {
				copyRange := &fuse.CopyFileRangeIn{InHeader: read.InHeader, FhIn: uint64(id), FhOut: uint64(id), Len: 1}
				if n, status := wfs.CopyFileRange(nil, copyRange); n != 0 || status != fuse.ENOTSUP {
					t.Errorf("copy = (%d, %v), want (0, ENOTSUP)", n, status)
				}
			}
		})
	}
}
