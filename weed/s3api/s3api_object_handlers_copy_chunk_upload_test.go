package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestNewChunkUploadOption_AvoidsBytePool is a regression guard for the
// s3-side retention amplifier in https://github.com/seaweedfs/seaweedfs/issues/6541.
//
// operation.upload_content falls back to the package-global
// valyala/bytebufferpool (which retains high-water buffers for the
// process's lifetime) whenever UploadOption.BytesBuffer is nil. Concurrent
// UploadPartCopy calls then hoard one chunk-sized buffer per concurrent
// upload in that pool, and RSS never recedes. The chunk-copy path must
// therefore always provide its own buffer; this test pins that contract.
func TestNewChunkUploadOption_AvoidsBytePool(t *testing.T) {
	cases := []struct {
		name     string
		chunkLen int
	}{
		{"empty chunk", 0},
		{"small chunk", 1024},
		{"typical 8 MiB chunk", 8 * 1024 * 1024},
		{"large 64 MiB chunk", 64 * 1024 * 1024},
	}

	assign := &filer_pb.AssignVolumeResponse{
		FileId: "1,foo",
		Location: &filer_pb.Location{
			Url:       "127.0.0.1:8080",
			PublicUrl: "127.0.0.1:8080",
		},
		Auth: "",
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data := make([]byte, c.chunkLen)
			opt := newChunkUploadOption(data, assign, false)

			if opt.BytesBuffer == nil {
				t.Fatalf("BytesBuffer must be non-nil so chunk uploads bypass " +
					"the package-global bytebufferpool (regression: #6541)")
			}
			if got, want := opt.BytesBuffer.Cap(), c.chunkLen+multipartFramingOverhead; got < want {
				t.Errorf("BytesBuffer cap=%d, want >=%d (one alloc-and-grow "+
					"per upload defeats the purpose of pre-sizing)", got, want)
			}
			if opt.BytesBuffer.Len() != 0 {
				t.Errorf("BytesBuffer not empty: len=%d", opt.BytesBuffer.Len())
			}
			if opt.Cipher {
				t.Errorf("Cipher must be false: chunk-copy data is already " +
					"encrypted if the source had a CipherKey")
			}
		})
	}
}

// TestNewChunkUploadOption_PerCallIsolation verifies each call returns a
// distinct buffer. Sharing one buffer across concurrent uploads would
// corrupt the multipart bodies under concurrent UploadPartCopy.
func TestNewChunkUploadOption_PerCallIsolation(t *testing.T) {
	assign := &filer_pb.AssignVolumeResponse{
		FileId:   "1,foo",
		Location: &filer_pb.Location{Url: "127.0.0.1:8080"},
	}

	o1 := newChunkUploadOption(nil, assign, false)
	o2 := newChunkUploadOption(nil, assign, false)
	if o1.BytesBuffer == o2.BytesBuffer {
		t.Fatalf("newChunkUploadOption must hand each caller a distinct buffer; " +
			"sharing one across concurrent uploads would corrupt the multipart bodies")
	}
}
