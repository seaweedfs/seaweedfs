package filer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
		offset = int64(TotalSize(entry.GetChunks()))
	}

	// append to existing chunks
	entry.Chunks = append(entry.GetChunks(), uploadResult.ToPbFileChunk(assignResult.Fid, offset, time.Now().UnixNano()))

	// update the entry
	err = f.CreateEntry(context.Background(), entry, nil, false, false, nil, false, f.MaxFilenameLength)

	return err
}

// resolveMetadataLogAssignDiskType returns the disk type and matched path rule for
// metadata log volume assigns. Disk type uses the rule when set, otherwise
// Filer.DefaultDiskType (from -filer.disk), mirroring resolveAssignStorageOption.
func (f *Filer) resolveMetadataLogAssignDiskType(targetFile string) (string, *filer_pb.FilerConf_PathConf) {
	if f.FilerConf == nil {
		return f.DefaultDiskType, &filer_pb.FilerConf_PathConf{}
	}
	rule := f.FilerConf.MatchStorageRule(targetFile)
	return util.Nvl(rule.DiskType, f.DefaultDiskType), rule
}

func (f *Filer) assignAndUpload(targetFile string, data []byte) (*operation.AssignResult, *operation.UploadResult, error) {
	// assign a volume location
	diskType, rule := f.resolveMetadataLogAssignDiskType(targetFile)
	assignRequest := &operation.VolumeAssignRequest{
		Count:               1,
		Collection:          util.Nvl(f.metaLogCollection, rule.Collection),
		Replication:         util.Nvl(f.metaLogReplication, rule.Replication),
		DiskType:            diskType,
		WritableVolumeCount: rule.VolumeGrowthCount,
		ExpectedDataSize:    uint64(len(data)),
	}

	assignResult, err := operation.Assign(context.Background(), f.GetMaster, f.GrpcDialOption, assignRequest)
	if err != nil {
		return nil, nil, fmt.Errorf("AssignVolume: %w", err)
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

	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, nil, fmt.Errorf("upload data %s: %v", targetUrl, err)
	}

	uploadResult, err := uploader.UploadData(context.Background(), data, uploadOption)
	if err != nil {
		return nil, nil, fmt.Errorf("upload data %s: %v", targetUrl, err)
	}
	// println("uploaded to", targetUrl)
	return assignResult, uploadResult, nil
}
