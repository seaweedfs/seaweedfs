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

type testProperties map[string]string

func (m testProperties) GetString(key string) string {
	return m[key]
}

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

func TestParseConcurrency(t *testing.T) {
	cases := []struct {
		value    string
		def      int
		expected int
	}{
		{"", 5, 5},    // unset -> default
		{"0", 5, 5},   // explicit zero means default
		{"-3", 5, 5},  // invalid -> default
		{"abc", 5, 5}, // invalid -> default
		{"1", 5, 1},   // override
		{"64", 5, 64}, // override
	}
	for _, tt := range cases {
		if got := parseConcurrency(tt.value, tt.def); got != tt.expected {
			t.Errorf("parseConcurrency(%q, %d) = %d, want %d", tt.value, tt.def, got, tt.expected)
		}
	}
}

func TestS3BackendStorageConcurrencyConfigRoundTrip(t *testing.T) {
	s, err := newS3BackendStorage(testProperties{}, "", "test")
	if err != nil {
		t.Fatal(err)
	}
	if s.uploadConcurrency != defaultUploadConcurrency || s.downloadConcurrency != defaultDownloadConcurrency {
		t.Fatalf("defaults: upload=%d download=%d, want %d/%d",
			s.uploadConcurrency, s.downloadConcurrency, defaultUploadConcurrency, defaultDownloadConcurrency)
	}

	s, err = newS3BackendStorage(testProperties{
		"upload_concurrency":   "1",
		"download_concurrency": "17",
	}, "", "test")
	if err != nil {
		t.Fatal(err)
	}
	if s.uploadConcurrency != 1 || s.downloadConcurrency != 17 {
		t.Fatalf("configured: upload=%d download=%d, want 1/17", s.uploadConcurrency, s.downloadConcurrency)
	}

	props := s.ToProperties()
	if props["upload_concurrency"] != "1" || props["download_concurrency"] != "17" {
		t.Fatalf("ToProperties: %v", props)
	}
}
