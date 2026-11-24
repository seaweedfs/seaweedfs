package mount

import (
	"slices"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

type LockedEntry struct {
	*filer_pb.Entry
	sync.RWMutex
	sseS3Check sync.Once
	sseS3      bool
}

// Native reads cannot decrypt S3 objects. Check the entry once per metadata
// change so reads of ordinary files do not repeatedly scan their chunk list.
func (le *LockedEntry) hasSSES3Data() bool {
	le.RLock()
	defer le.RUnlock()
	return le.hasSSES3DataLocked()
}

func (le *LockedEntry) hasSSES3DataLocked() bool {
	le.sseS3Check.Do(func() {
		metadata := le.Entry.GetExtended()
		_, hasKey := metadata[s3_constants.SeaweedFSSSES3Key]
		le.sseS3 = hasKey && string(metadata[s3_constants.AmzServerSideEncryption]) == s3_constants.SSEAlgorithmAES256
		if !le.sseS3 {
			le.sseS3 = slices.ContainsFunc(le.Entry.GetChunks(), func(chunk *filer_pb.FileChunk) bool {
				return chunk.GetSseType() == filer_pb.SSEType_SSE_S3
			})
		}
	})
	return le.sseS3
}

func (le *LockedEntry) GetEntry() *filer_pb.Entry {
	le.RLock()
	defer le.RUnlock()
	return le.Entry
}

// SetEntry sets the entry of the LockedEntry
// entry should never be nil
func (le *LockedEntry) SetEntry(entry *filer_pb.Entry) {
	le.Lock()
	defer le.Unlock()
	le.Entry = entry
	le.sseS3Check = sync.Once{}
}

func (le *LockedEntry) UpdateEntry(fn func(entry *filer_pb.Entry)) *filer_pb.Entry {
	le.Lock()
	defer le.Unlock()
	fn(le.Entry)
	le.sseS3Check = sync.Once{}
	return le.Entry
}

func (le *LockedEntry) GetChunks() []*filer_pb.FileChunk {
	le.RLock()
	defer le.RUnlock()
	return le.Entry.Chunks
}

func (le *LockedEntry) AppendChunks(newChunks []*filer_pb.FileChunk) {
	le.Lock()
	defer le.Unlock()
	le.Entry.Chunks = append(le.Entry.Chunks, newChunks...)
	le.sseS3Check = sync.Once{}
}
