package mount

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) saveDataAsChunk(fullPath util.FullPath) filer.SaveDataAsChunkFunctionType {

	// Backstop: FUSE entry points sanitize names before they reach
	// inodeToPath, but async flush paths (e.g. writebackCache, handles whose
	// RememberPath was set from an older code path) may still carry bytes
	// that predate sanitization. Proto3 string fields require valid UTF-8,
	// so scrub the path once here before every AssignVolume call. Use '_'
	// (URL-safe) because the path also flows through HTTP URLs downstream.
	assignPath := string(fullPath)
	if !utf8.ValidString(assignPath) {
		assignPath = strings.ToValidUTF8(assignPath, "_")
	}

	return func(reader io.Reader, filename string, offset int64, tsNs int64, _ uint64) (chunk *filer_pb.FileChunk, err error) {
		uploader, err := operation.NewUploader()
		if err != nil {
			return
		}

		uploadOption := &operation.UploadOption{
			Filename:          filename,
			Cipher:            wfs.option.Cipher,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
		}
		genFileUrlFn := func(host, fileId string) string {
			fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
			if wfs.option.VolumeServerAccess == "filerProxy" {
				fileUrl = fmt.Sprintf("http://%s/?proxyChunkId=%s", wfs.getCurrentFiler(), fileId)
			}
			return fileUrl
		}

		fileId, uploadResult, err, data := uploader.UploadWithRetry(
			wfs,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: wfs.option.Replication,
				Collection:  wfs.option.Collection,
				TtlSec:      wfs.option.TtlSec,
				DiskType:    string(wfs.option.DiskType),
				DataCenter:  wfs.option.DataCenter,
				Path:        assignPath,
			},
			uploadOption, genFileUrlFn, reader,
		)

		if err != nil {
			glog.V(0).Infof("upload data %v: %v", filename, err)
			return nil, fmt.Errorf("upload data: %w", err)
		}
		if uploadResult.Error != "" {
			glog.V(0).Infof("upload failure %v: %v", filename, err)
			return nil, fmt.Errorf("upload result: %v", uploadResult.Error)
		}

		// When peer sharing is enabled we need EVERY chunk in the
		// local cache so we can actually serve it back to peers on
		// FetchChunk — otherwise the directory would advertise us as
		// a holder and the fetcher would get NOT_FOUND from our
		// chunk cache. When peer sharing is off we preserve the
		// original behavior of caching only the first chunk (small
		// files) to avoid blowing the cache on large uploads. Both
		// paths gate on chunkCache != nil: -cacheCapacityMB=0 disables
		// the cache entirely, in which case SetChunk would panic.
		shouldCache := wfs.chunkCache != nil && (offset == 0 || wfs.peerAnnouncer != nil)
		if shouldCache {
			wfs.chunkCache.SetChunk(fileId, data)
		}
		// Announce every uploaded chunk so the tier-2 directory fills
		// in as the file is written. Without this, the per-fetch
		// announce path only bootstraps after someone else has already
		// pulled a chunk via peer — which can't happen if nobody has
		// told the directory who holds the chunk. Skip the announce
		// when we couldn't cache (no point advertising bytes we can't
		// actually serve back).
		if wfs.peerAnnouncer != nil && shouldCache {
			wfs.peerAnnouncer.EnqueueAnnounce(fileId)
		}

		chunk = uploadResult.ToPbFileChunk(fileId, offset, tsNs)
		return chunk, nil
	}
}
