package weed_server

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// fakeCopyFileStream is a synthetic VolumeServer_CopyFileClient used to
// drive writeToFile's failure paths in tests. The pre-fix code left a
// partial / 0-byte destination file on the disk when the stream errored
// mid-copy; with the fix, writeToFile now removes the incomplete file
// so callers (notably VolumeEcShardsCopy distributing .ecx) don't end
// up with stubs that mount-time code mistakes for valid empty indexes.
type fakeCopyFileStream struct {
	responses []*volume_server_pb.CopyFileResponse
	finalErr  error
	index     int
}

func (s *fakeCopyFileStream) Recv() (*volume_server_pb.CopyFileResponse, error) {
	if s.index >= len(s.responses) {
		if s.finalErr != nil {
			return nil, s.finalErr
		}
		return nil, io.EOF
	}
	r := s.responses[s.index]
	s.index++
	return r, nil
}

func (s *fakeCopyFileStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *fakeCopyFileStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *fakeCopyFileStream) CloseSend() error             { return nil }
func (s *fakeCopyFileStream) Context() context.Context     { return context.Background() }
func (s *fakeCopyFileStream) SendMsg(any) error            { return nil }
func (s *fakeCopyFileStream) RecvMsg(any) error            { return nil }

func TestWriteToFile_RemovesPartialFileOnStreamError(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "vol_42.ecx")

	stream := &fakeCopyFileStream{
		responses: []*volume_server_pb.CopyFileResponse{
			// Real bytes flow first — modifiedTsNs is non-zero so the
			// existing "source file not found" cleanup at the bottom of
			// writeToFile does NOT fire; the new mid-stream cleanup is
			// the only path that can remove the file.
			{FileContent: []byte("partial data"), ModifiedTsNs: 1234567890},
		},
		finalErr: errors.New("simulated mid-stream failure"),
	}

	_, err := writeToFile(stream, dst, util.NewWriteThrottler(0), false, nil)
	if err == nil {
		t.Fatalf("writeToFile should propagate the stream error")
	}

	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		t.Errorf("incomplete file should be removed; stat err = %v", statErr)
	}
}

func TestWriteToFile_RemovesEmptyFileOnImmediateStreamError(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "vol_42.ecx")

	stream := &fakeCopyFileStream{
		// No FileContent at all; stream errors on the first Recv.
		// progressedBytes == 0 and modifiedTsNs == 0, so without the
		// mid-stream cleanup this would leave a 0-byte file from the
		// O_TRUNC at OpenFile time.
		finalErr: errors.New("simulated immediate failure"),
	}

	_, err := writeToFile(stream, dst, util.NewWriteThrottler(0), false, nil)
	if err == nil {
		t.Fatalf("writeToFile should propagate the stream error")
	}

	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		t.Errorf("0-byte file should be removed; stat err = %v", statErr)
	}
}

func TestWriteToFile_PreservesAppendModeOnError(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "vol_42.ecj")

	// Pre-existing content the caller owns — isAppend=true tells
	// writeToFile not to touch it on cleanup.
	if err := os.WriteFile(dst, []byte("pre-existing journal data"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	stream := &fakeCopyFileStream{
		finalErr: errors.New("simulated failure"),
	}

	_, err := writeToFile(stream, dst, util.NewWriteThrottler(0), true, nil)
	if err == nil {
		t.Fatalf("writeToFile should propagate the stream error")
	}

	info, statErr := os.Stat(dst)
	if statErr != nil {
		t.Fatalf("append-mode file should be preserved on error; stat: %v", statErr)
	}
	if info.Size() == 0 {
		t.Errorf("append-mode file unexpectedly truncated to 0 bytes")
	}
}

func TestWriteToFile_SucceedsOnCleanStream(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "vol_42.ecx")

	want := []byte("hello ecx index")
	stream := &fakeCopyFileStream{
		responses: []*volume_server_pb.CopyFileResponse{
			{FileContent: want, ModifiedTsNs: 9},
		},
	}

	if _, err := writeToFile(stream, dst, util.NewWriteThrottler(0), false, nil); err != nil {
		t.Fatalf("writeToFile failed on clean stream: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("contents = %q, want %q", got, want)
	}
}
