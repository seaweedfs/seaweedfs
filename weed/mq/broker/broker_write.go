package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (b *MessageQueueBroker) appendToFile(targetFile string, data []byte) error {
	return b.appendToFileWithBufferIndex(targetFile, data, 0)
}

func (b *MessageQueueBroker) appendToFileWithBufferIndex(targetFile string, data []byte, bufferOffset int64, offsetArgs ...int64) error {
	// Extract optional offset parameters (minOffset, maxOffset)
	var minOffset, maxOffset int64
	if len(offsetArgs) >= 2 {
		minOffset = offsetArgs[0]
		maxOffset = offsetArgs[1]
	}

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

		// Add buffer start offset for deduplication tracking (binary format)
		if bufferOffset != 0 {
			entry.Extended = make(map[string][]byte)
			bufferStartBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(bufferStartBytes, uint64(bufferOffset))
			entry.Extended[mq.ExtendedAttrBufferStart] = bufferStartBytes
		}

		// Add offset range metadata for Kafka integration
		if minOffset > 0 && maxOffset >= minOffset {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			minOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(minOffsetBytes, uint64(minOffset))
			entry.Extended[mq.ExtendedAttrOffsetMin] = minOffsetBytes

			maxOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(maxOffsetBytes, uint64(maxOffset))
			entry.Extended[mq.ExtendedAttrOffsetMax] = maxOffsetBytes
		}
	} else if err != nil {
		return fmt.Errorf("find %s: %v", fullpath, err)
	} else {
		offset = int64(filer.TotalSize(entry.GetChunks()))

		// Verify buffer offset continuity for existing files (append operations)
		if bufferOffset != 0 {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}

			// Check for existing buffer start (binary format)
			if existingData, exists := entry.Extended[mq.ExtendedAttrBufferStart]; exists {
				if len(existingData) == 8 {
					existingStartIndex := int64(binary.BigEndian.Uint64(existingData))

					// Verify that the new buffer offset is consecutive
					// Expected offset = start + number of existing chunks
					expectedOffset := existingStartIndex + int64(len(entry.GetChunks()))
					if bufferOffset != expectedOffset {
						// This shouldn't happen in normal operation
						// Log warning but continue (don't crash the system)
						glog.Warningf("non-consecutive buffer offset for %s. Expected %d, got %d",
							fullpath, expectedOffset, bufferOffset)
					}
					// Note: We don't update the start offset - it stays the same
				}
			} else {
				// No existing buffer start, create new one (shouldn't happen for existing files)
				bufferStartBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(bufferStartBytes, uint64(bufferOffset))
				entry.Extended[mq.ExtendedAttrBufferStart] = bufferStartBytes
			}
		}

		// Update offset range metadata for existing files
		if minOffset > 0 && maxOffset >= minOffset {
			// Update minimum offset if this chunk has a lower minimum
			if existingMinData, exists := entry.Extended[mq.ExtendedAttrOffsetMin]; exists && len(existingMinData) == 8 {
				existingMin := int64(binary.BigEndian.Uint64(existingMinData))
				if minOffset < existingMin {
					minOffsetBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(minOffsetBytes, uint64(minOffset))
					entry.Extended[mq.ExtendedAttrOffsetMin] = minOffsetBytes
				}
			} else {
				// No existing minimum, set it
				minOffsetBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(minOffsetBytes, uint64(minOffset))
				entry.Extended[mq.ExtendedAttrOffsetMin] = minOffsetBytes
			}

			// Update maximum offset if this chunk has a higher maximum
			if existingMaxData, exists := entry.Extended[mq.ExtendedAttrOffsetMax]; exists && len(existingMaxData) == 8 {
				existingMax := int64(binary.BigEndian.Uint64(existingMaxData))
				if maxOffset > existingMax {
					maxOffsetBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(maxOffsetBytes, uint64(maxOffset))
					entry.Extended[mq.ExtendedAttrOffsetMax] = maxOffsetBytes
				}
			} else {
				// No existing maximum, set it
				maxOffsetBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(maxOffsetBytes, uint64(maxOffset))
				entry.Extended[mq.ExtendedAttrOffsetMax] = maxOffsetBytes
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
