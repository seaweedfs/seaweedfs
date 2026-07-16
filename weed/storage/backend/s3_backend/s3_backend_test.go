package s3_backend

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

type stubS3Client struct {
	s3iface.S3API
	getObject func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

func (s *stubS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return s.getObject(input)
}

func TestReadAtReturnsEOFForShortRead(t *testing.T) {
	client := &stubS3Client{getObject: func(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("abc"))}, nil
	}}
	file := S3BackendStorageFile{
		backendStorage: &S3BackendStorage{conn: client, bucket: "bucket"},
		key:            "key",
		tierInfo: &volume_server_pb.VolumeInfo{Files: []*volume_server_pb.RemoteFile{
			{FileSize: 10},
		}},
	}

	buffer := make([]byte, 5)
	n, err := file.ReadAt(buffer, 0)
	if n != 3 {
		t.Fatalf("ReadAt() read %d bytes, want 3", n)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt() error = %v, want io.EOF", err)
	}
}

func TestReadAtClearsEOFOnFullRead(t *testing.T) {
	client := &stubS3Client{getObject: func(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{Body: io.NopCloser(iotest.DataErrReader(strings.NewReader("abcde")))}, nil
	}}
	file := S3BackendStorageFile{
		backendStorage: &S3BackendStorage{conn: client, bucket: "bucket"},
		key:            "key",
		tierInfo: &volume_server_pb.VolumeInfo{Files: []*volume_server_pb.RemoteFile{
			{FileSize: 10},
		}},
	}

	buffer := make([]byte, 5)
	n, err := file.ReadAt(buffer, 0)
	if n != 5 || err != nil {
		t.Fatalf("ReadAt() = (%d, %v), want (5, nil)", n, err)
	}
	if string(buffer) != "abcde" {
		t.Fatalf("ReadAt() buffer = %q, want %q", buffer, "abcde")
	}
}

func TestReadAtRejectsInvalidRequestsLocally(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		offset  int64
		wantErr bool
	}{
		{name: "empty buffer", buffer: nil},
		{name: "negative offset", buffer: make([]byte, 1), offset: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			client := &stubS3Client{getObject: func(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
				called = true
				return nil, errors.New("unexpected GetObject")
			}}
			file := S3BackendStorageFile{
				backendStorage: &S3BackendStorage{conn: client, bucket: "bucket"},
				key:            "key",
				tierInfo: &volume_server_pb.VolumeInfo{Files: []*volume_server_pb.RemoteFile{
					{FileSize: 10},
				}},
			}

			n, err := file.ReadAt(tt.buffer, tt.offset)
			if n != 0 || (err != nil) != tt.wantErr {
				t.Fatalf("ReadAt() = (%d, %v), want error %t", n, err, tt.wantErr)
			}
			if called {
				t.Fatal("ReadAt() called S3 for an invalid request")
			}
		})
	}
}
