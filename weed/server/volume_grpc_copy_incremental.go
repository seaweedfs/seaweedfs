package weed_server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (vs *VolumeServer) VolumeIncrementalCopy(req *volume_server_pb.VolumeIncrementalCopyRequest, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}
	if err := v.UnavailableError(); err != nil {
		return err
	}

	// capture the revision and the .dat/.idx bounds before the resume reads,
	// so the resume scans only rows the streams below can actually carry
	revision := v.SuperBlock.CompactionRevision
	stopOffset, stopIndex, _ := v.FileStat()

	// a since_ns resume from an old client has no verified prefix, so the
	// index rows are only safe to replay when the whole file is sent
	streamIndex := req.EntryCount > 0 || req.SinceNs == 0
	if streamIndex {
		// bound the .dat tail by the records the streamed rows cover: a
		// record staged but not yet indexed would reach the backup without
		// its row and be appended again on the next run
		if end := v.DatSizeCoveredByIndex(int64(stopIndex)); end > 0 && end < int64(stopOffset) {
			stopOffset = uint64(end)
		}
	}

	var startOffset int64
	var hasData bool
	if req.EntryCount > 0 {
		foundOffset, upToDate, err := v.ResumeByLastEntry(
			req.EntryCount,
			types.NeedleId(req.LastNeedleKey),
			req.LastNeedleCookie,
			types.Size(req.LastNeedleSize),
			req.EntriesHash,
			stopIndex,
		)
		if err != nil {
			if errors.Is(err, storage.ErrIncrementalCopyDiverged) {
				return status.Error(codes.FailedPrecondition, err.Error())
			}
			return fmt.Errorf("fail to locate by entry %d: %s", req.EntryCount, err)
		}
		if upToDate {
			return nil
		}
		hasData = !foundOffset.IsZero()
		startOffset = foundOffset.ToActualOffset()
	} else {
		foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(req.SinceNs)
		if err != nil {
			return fmt.Errorf("fail to locate by appendAtNs %d: %s", req.SinceNs, err)
		}
		if isLastOne {
			return nil
		}
		hasData = true
		startOffset = foundOffset.ToActualOffset()
	}

	var indexFile *os.File
	if streamIndex && int64(stopIndex) > int64(req.EntryCount)*types.NeedleMapEntrySize {
		var err error
		if indexFile, err = os.Open(v.FileName(".idx")); err != nil {
			return fmt.Errorf("cannot read %s: %v", v.FileName(".idx"), err)
		}
		defer indexFile.Close()
		if v.SuperBlock.CompactionRevision != revision {
			return status.Error(codes.FailedPrecondition, storage.ErrIncrementalCopyDiverged.Error())
		}
	}

	buf := make([]byte, 1024*1024*2)
	if hasData {
		if err := sendFileContent(v, buf, startOffset, int64(stopOffset), stream); err != nil {
			return err
		}
	}
	if indexFile != nil {
		if err := sendIndexContent(v, indexFile, buf, int64(req.EntryCount)*types.NeedleMapEntrySize, int64(stopIndex), stream); err != nil {
			return err
		}
	}
	if v.SuperBlock.CompactionRevision != revision {
		return status.Error(codes.FailedPrecondition, storage.ErrIncrementalCopyDiverged.Error())
	}
	return nil

}

func (vs *VolumeServer) VolumeSyncStatus(ctx context.Context, req *volume_server_pb.VolumeSyncStatusRequest) (*volume_server_pb.VolumeSyncStatusResponse, error) {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	resp := v.GetVolumeSyncStatus()

	return resp, nil

}

func sendIndexContent(v *storage.Volume, indexFile *os.File, buf []byte, startOffset, stopOffset int64, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {
	var blockSizeLimit = int64(len(buf))
	for i := int64(0); i < stopOffset-startOffset; i += blockSizeLimit {
		if err := v.UnavailableError(); err != nil {
			return err
		}
		read := stopOffset - startOffset - i
		if read > blockSizeLimit {
			read = blockSizeLimit
		}
		n, readErr := indexFile.ReadAt(buf[:read], startOffset+i)
		if n > 0 {
			resp := &volume_server_pb.VolumeIncrementalCopyResponse{}
			resp.IdxContent = buf[:int64(n)]
			if sendErr := stream.Send(resp); sendErr != nil {
				return sendErr
			}
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func sendFileContent(v *storage.Volume, buf []byte, startOffset, stopOffset int64, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {
	var blockSizeLimit = int64(len(buf))
	for i := int64(0); i < stopOffset-startOffset; i += blockSizeLimit {
		if err := v.UnavailableError(); err != nil {
			return err
		}
		read := stopOffset - startOffset - i
		if read > blockSizeLimit {
			read = blockSizeLimit
		}
		n, readErr := v.DataBackend.ReadAt(buf[:read], startOffset+i)
		if readErr == nil || readErr == io.EOF {
			resp := &volume_server_pb.VolumeIncrementalCopyResponse{}
			resp.FileContent = buf[:int64(n)]
			sendErr := stream.Send(resp)
			if sendErr != nil {
				return sendErr
			}
		} else {
			return readErr
		}
	}
	return nil
}
