package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (b *MessageQueueBroker) appendToFile(targetFile string, data []byte) error {
	return b.appendToFileWithBufferIndex(targetFile, data, 0)
}

func (b *MessageQueueBroker) appendToFileWithBufferIndex(targetFile string, data []byte, bufferIndex int64) error {

	fileId, uploadResult, err2 := b.assignAndUpload(targetFile, data)
	if err2 != nil {
		return err2
	}

	// find out existing entry
	fullpath := util.FullPath(targetFile)
	dir, name := fullpath.DirAndName()
	entry, err := filer_pb.GetEntry(context.Background(), b, fullpath)
	var offset int64 = 0
	if err == filer_pb.ErrNotFound {
		entry = &filer_pb.Entry{
			Name:        name,
			IsDirectory: false,
			Attributes: &filer_pb.FuseAttributes{
				Crtime:   time.Now().Unix(),
				Mtime:    time.Now().Unix(),
				FileMode: uint32(os.FileMode(0644)),
				Uid:      uint32(os.Getuid()),
				Gid:      uint32(os.Getgid()),
			},
		}

		// Add buffer start index for deduplication tracking (binary format)
		if bufferIndex != 0 {
			entry.Extended = make(map[string][]byte)
			bufferStartBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(bufferStartBytes, uint64(bufferIndex))
			entry.Extended["buffer_start"] = bufferStartBytes
		}
	} else if err != nil {
		return fmt.Errorf("find %s: %v", fullpath, err)
	} else {
		offset = int64(filer.TotalSize(entry.GetChunks()))

		// Verify buffer index continuity for existing files (append operations)
		if bufferIndex != 0 {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}

			// Check for existing buffer start (binary format)
			if existingData, exists := entry.Extended["buffer_start"]; exists {
				if len(existingData) == 8 {
					existingStartIndex := int64(binary.BigEndian.Uint64(existingData))

					// Verify that the new buffer index is consecutive
					// Expected index = start + number of existing chunks
					expectedIndex := existingStartIndex + int64(len(entry.GetChunks()))
					if bufferIndex != expectedIndex {
						// This shouldn't happen in normal operation
						// Log warning but continue (don't crash the system)
						glog.Warningf("non-consecutive buffer index for %s. Expected %d, got %d",
							fullpath, expectedIndex, bufferIndex)
					}
					// Note: We don't update the start index - it stays the same
				}
			} else {
				// No existing buffer start, create new one (shouldn't happen for existing files)
				bufferStartBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(bufferStartBytes, uint64(bufferIndex))
				entry.Extended["buffer_start"] = bufferStartBytes
			}
		}
	}

	// append to existing chunks
	entry.Chunks = append(entry.GetChunks(), uploadResult.ToPbFileChunk(fileId, offset, time.Now().UnixNano()))

	// update the entry
	return b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.CreateEntry(context.Background(), client, &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
	})
}

func (b *MessageQueueBroker) assignAndUpload(targetFile string, data []byte) (fileId string, uploadResult *operation.UploadResult, err error) {

	reader := util.NewBytesReader(data)

	uploader, err := operation.NewUploader()
	if err != nil {
		return
	}

	fileId, uploadResult, err, _ = uploader.UploadWithRetry(
		b,
		&filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: b.option.DefaultReplication,
			Collection:  "topics",
			// TtlSec:      wfs.option.TtlSec,
			// DiskType:    string(wfs.option.DiskType),
			DataCenter: b.option.DataCenter,
			Path:       targetFile,
		},
		&operation.UploadOption{
			Cipher: b.option.Cipher,
		},
		func(host, fileId string) string {
			fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
			if b.option.VolumeServerAccess == "filerProxy" {
				fileUrl = fmt.Sprintf("http://%s/?proxyChunkId=%s", b.currentFiler, fileId)
			}
			return fileUrl
		},
		reader,
	)
	return
}
