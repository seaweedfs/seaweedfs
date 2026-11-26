package mount

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

type LockedEntry struct {
	*filer_pb.Entry
	sync.RWMutex
	sseCheck sync.Once
	sse      filer_pb.SSEType
}

// Native reads cannot decrypt S3 objects. Check the entry once per metadata
// change so reads of ordinary files do not repeatedly scan their chunk list.
// Any type other than SSE-S3 wins, since no mount option can read it.
func (le *LockedEntry) sseType() filer_pb.SSEType {
	le.RLock()
	defer le.RUnlock()
	return le.sseTypeLocked()
}

func (le *LockedEntry) sseTypeLocked() filer_pb.SSEType {
	le.sseCheck.Do(func() {
		metadata := le.Entry.GetExtended()
		algorithm := string(metadata[s3_constants.AmzServerSideEncryption])
		_, sseC := metadata[s3_constants.SeaweedFSSSEIV]
		_, sseKMS := metadata[s3_constants.SeaweedFSSSEKMSKey]
		_, sseS3 := metadata[s3_constants.SeaweedFSSSES3Key]
		switch {
		case sseC && metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] != nil:
			le.sse = filer_pb.SSEType_SSE_C
		case sseKMS && algorithm == s3_constants.SSEAlgorithmKMS:
			le.sse = filer_pb.SSEType_SSE_KMS
		case sseS3 && algorithm == s3_constants.SSEAlgorithmAES256:
			le.sse = filer_pb.SSEType_SSE_S3
		default:
			le.sse = filer_pb.SSEType_NONE
		}
		for _, chunk := range le.Entry.GetChunks() {
			if t := chunk.GetSseType(); t != filer_pb.SSEType_NONE && (le.sse == filer_pb.SSEType_NONE || le.sse == filer_pb.SSEType_SSE_S3) {
				le.sse = t
			}
		}
	})
	return le.sse
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
	le.sseCheck = sync.Once{}
}

func (le *LockedEntry) UpdateEntry(fn func(entry *filer_pb.Entry)) *filer_pb.Entry {
	le.Lock()
	defer le.Unlock()
	fn(le.Entry)
	le.sseCheck = sync.Once{}
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
	le.sseCheck = sync.Once{}
}
