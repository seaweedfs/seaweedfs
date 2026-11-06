package filer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

	return f.commitAppendedChunk(ctx, util.FullPath(targetFile), uploadResult.ToPbFileChunk(assignResult.Fid, 0, time.Now().UnixNano()))
}

// commitAppendedChunk appends an uploaded chunk to the file, releasing the
// chunk when it cannot be committed: the flush retries with a new upload.
func (f *Filer) commitAppendedChunk(ctx context.Context, fullpath util.FullPath, chunk *filer_pb.FileChunk) error {
	// find out existing entry
	entry, err := f.FindEntry(ctx, fullpath)
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
		f.DeleteUncommittedChunks(ctx, []*filer_pb.FileChunk{chunk})
		return fmt.Errorf("find %s: %v", fullpath, err)
	} else {
		chunk.Offset = int64(TotalSize(entry.GetChunks()))
	}

	// append to existing chunks
	entry.Chunks = append(entry.GetChunks(), chunk)

	// update the entry
	if err := f.CreateEntry(ctx, entry, nil, false, false, nil, false, f.MaxFilenameLength); err != nil {
		// A create can store the entry and then fail on its parent folders;
		// a retry would only append the piece again.
		stored, findErr := f.FindEntry(ctx, fullpath)
		if findErr == nil && slices.ContainsFunc(stored.GetChunks(), func(c *filer_pb.FileChunk) bool {
			return c.GetFileIdString() == chunk.GetFileIdString()
		}) {
			glog.V(0).Infof("append to %s landed despite: %v", fullpath, err)
			return nil
		}
		if findErr == nil || errors.Is(findErr, filer_pb.ErrNotFound) {
			f.DeleteUncommittedChunks(ctx, []*filer_pb.FileChunk{chunk})
		}
		return err
	}
	return nil
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
