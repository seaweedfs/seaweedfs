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

func (f *Filer) appendToFile(ctx context.Context, targetFile string, data []byte) error {

	assignResult, uploadResult, err2 := f.assignAndUpload(ctx, targetFile, data)
	if err2 != nil {
		return err2
	}

	// The piece is already uploaded; commit it on a detached context so an
	// expired shutdown deadline does not strand the chunk.
	ctx = context.WithoutCancel(ctx)

	// find out existing entry
	fullpath := util.FullPath(targetFile)
	entry, err := f.FindEntry(ctx, fullpath)
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
	return f.commitAppendedChunk(ctx, fullpath, entry, assignResult, uploadResult, offset)
}

// commitAppendedChunk commits an entry whose chunk has already been uploaded,
// releasing that chunk if the commit fails.
//
// The chunk is uploaded before the entry is committed. When the commit fails
// nothing references the chunk: no entry points at that fid, so no later delete
// can ever mark it, and neither vacuum nor fsck can see it because both work
// from the entry index. The volume server keeps counting those bytes as live
// data, which is how a cluster with a few hundred megabytes of real logs ends
// up carrying tens of gigabytes of "live" volume data.
func (f *Filer) commitAppendedChunk(ctx context.Context, fullpath util.FullPath, entry *Entry, assignResult *operation.AssignResult, uploadResult *operation.UploadResult, offset int64) error {
	err := f.CreateEntry(ctx, entry, nil, false, false, nil, false, f.MaxFilenameLength)
	if err != nil {
		f.DeleteChunks(ctx, fullpath, []*filer_pb.FileChunk{uploadResult.ToPbFileChunk(assignResult.Fid, offset, time.Now().UnixNano())})
	}
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

// metaLogCollectionFor resolves the system metadata log's collection: the
// filer.options.metaLog.collection override, then the filer default, then the
// matched storage rule.
func (f *Filer) metaLogCollectionFor(ruleCollection string) string {
	return util.Nvl(f.metaLogTargetCollection, f.metaLogCollection, ruleCollection)
}

func (f *Filer) metaLogReplicationFor(ruleReplication string) string {
	return util.Nvl(f.metaLogTargetReplication, f.metaLogReplication, ruleReplication)
}

func (f *Filer) assignAndUpload(ctx context.Context, targetFile string, data []byte) (*operation.AssignResult, *operation.UploadResult, error) {
	// assign a volume location
	diskType, rule := f.resolveMetadataLogAssignDiskType(targetFile)
	assignRequest := &operation.VolumeAssignRequest{
		Count:               1,
		Collection:          f.metaLogCollectionFor(rule.Collection),
		Replication:         f.metaLogReplicationFor(rule.Replication),
		DiskType:            diskType,
		WritableVolumeCount: rule.VolumeGrowthCount,
		ExpectedDataSize:    uint64(len(data)),
	}

	assignResult, err := operation.Assign(ctx, f.GetMaster, f.masterGrpcDialOption(), assignRequest)
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

	uploadResult, err := uploader.UploadData(ctx, data, uploadOption)
	if err != nil {
		return nil, nil, fmt.Errorf("upload data %s: %v", targetUrl, err)
	}
	// println("uploaded to", targetUrl)
	return assignResult, uploadResult, nil
}
