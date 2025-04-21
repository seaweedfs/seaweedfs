package util

import (
	"io"

	pb "github.com/seaweedfs/seaweedfs/weed/storage/backend/udm/api/private/v1"
	"google.golang.org/grpc"
)

const (
	HeaderChecksum = "x-checksum"
)

type ReaderWithChecksum interface {
	io.Reader
	Checksum() string
}

type FileStream struct {
	Stream   grpc.ServerStreamingClient[pb.DownloadFileResponse]
	buffer   []byte
	checksum string
}

func (d *FileStream) Read(p []byte) (n int, err error) {
	if d.Stream == nil {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	if len(d.buffer) == 0 {
		resp, err := d.Stream.Recv()
		if err != nil {
			if err == io.EOF {
				ts := d.Stream.Trailer()
				if ts != nil && len(ts[HeaderChecksum]) > 0 {
					d.checksum = ts[HeaderChecksum][0]
				}
			}
			d.Stream = nil
			return 0, err
		}
		d.buffer = resp.Data
	}
	n = copy(p, d.buffer)
	d.buffer = d.buffer[n:]
	return n, nil
}

func (d *FileStream) Checksum() string {
	return d.checksum
}
