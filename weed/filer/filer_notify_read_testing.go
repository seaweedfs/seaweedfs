package filer

import (
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// SetLogReadHooksForTesting swaps the volume-touching pieces of persisted log
// reading - chunk decode, byte streaming, volume liveness - for fakes, so loop
// tests can run the real subscribe machinery against an in-memory volume
// layer. Returns a restore func. Test support only.
func SetLogReadHooksForTesting(
	load func(chunk *filer_pb.FileChunk) ([]*filer_pb.LogEntry, error),
	stream func(chunks []*filer_pb.FileChunk) io.Reader,
	lookup func(fileId string) error,
) (restore func()) {
	prevLoad, prevStream, prevLookup := loadLogFileEntriesFn, newLogFileStreamReader, lookupLogChunkFn
	loadLogFileEntriesFn = func(masterClient *wdclient.MasterClient, chunk *filer_pb.FileChunk) ([]*filer_pb.LogEntry, bool, error) {
		entries, err := load(chunk)
		return entries, true, err
	}
	newLogFileStreamReader = func(masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) io.Reader {
		return stream(chunks)
	}
	lookupLogChunkFn = func(f *Filer, fileId string) error {
		return lookup(fileId)
	}
	return func() {
		loadLogFileEntriesFn, newLogFileStreamReader, lookupLogChunkFn = prevLoad, prevStream, prevLookup
	}
}
