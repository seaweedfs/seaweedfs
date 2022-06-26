package filer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (f *Filer) appendToFile(targetFile string, data []byte) error {

	assignResult, uploadResult, err2 := f.assignAndUpload(targetFile, data)
	if err2 != nil {
		return err2
	}

	// find out existing entry
	fullpath := util.FullPath(targetFile)
	entry, err := f.FindEntry(context.Background(), fullpath)
	var offset int64 = 0
	if err == filer_pb.ErrNotFound {
		entry = &Entry{
			FullPath: fullpath,
			Attr: Attr{
				Crtime: time.Now(),
				Mtime:  time.Now(),
				Mode:   os.FileMode(0644),
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}
	} else if err != nil {
		return fmt.Errorf("find %s: %v", fullpath, err)
	} else {
		offset = int64(TotalSize(entry.Chunks))
	}

	// append to existing chunks
	entry.Chunks = append(entry.Chunks, uploadResult.ToPbFileChunk(assignResult.Fid, offset))

	// update the entry
	err = f.CreateEntry(context.Background(), entry, false, false, nil, false)

	return err
}

func (f *Filer) assignAndUpload(targetFile string, data []byte) (*operation.AssignResult, *operation.UploadResult, error) {
	// assign a volume location
	rule := f.FilerConf.MatchStorageRule(targetFile)
	assignRequest := &operation.VolumeAssignRequest{
		Count:               1,
		Collection:          util.Nvl(f.metaLogCollection, rule.Collection),
		Replication:         util.Nvl(f.metaLogReplication, rule.Replication),
		WritableVolumeCount: rule.VolumeGrowthCount,
	}

	assignResult, err := operation.Assign(f.GetMaster, f.GrpcDialOption, assignRequest)
	if err != nil {
		return nil, nil, fmt.Errorf("AssignVolume: %v", err)
	}
	if assignResult.Error != "" {
		return nil, nil, fmt.Errorf("AssignVolume error: %v", assignResult.Error)
	}

	// upload data
	targetUrl := "http://" + assignResult.Url + "/" + assignResult.Fid
	uploadOption := &operation.UploadOption{
		UploadUrl:         targetUrl,
		Filename:          "",
		Cipher:            f.Cipher,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               assignResult.Auth,
	}
	uploadResult, err := operation.UploadData(data, uploadOption)
	if err != nil {
		return nil, nil, fmt.Errorf("upload data %s: %v", targetUrl, err)
	}
	// println("uploaded to", targetUrl)
	return assignResult, uploadResult, nil
}
