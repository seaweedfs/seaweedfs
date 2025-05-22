package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"os"
	"time"
)

func (b *MessageQueueBroker) appendToFile(targetFile string, data []byte) error {

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
	} else if err != nil {
		return fmt.Errorf("find %s: %v", fullpath, err)
	} else {
		offset = int64(filer.TotalSize(entry.GetChunks()))
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
