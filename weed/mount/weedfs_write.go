package mount

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"

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
	// so scrub the full path once here before every AssignVolume call.
	assignPath := fullPath.Sanitized()

	return func(reader io.Reader, filename string, offset int64, tsNs int64, _ uint64) (chunk *filer_pb.FileChunk, err error) {
		uploader, err := operation.NewUploader()
		if err != nil {
			return
		}

		// Send the chunk's MD5 as Content-MD5 so the volume server verifies
		// it on ingest and echoes it back into FileChunk.ETag, giving
		// mount-written files a meaningful filer.ETag. Skipped under cipher:
		// the server only sees ciphertext, so a plaintext digest could never
		// match.
		//
		// The dirty-page flush paths already hand us a *util.BytesReader whose
		// backing slice is the whole chunk, so hash it in place — no second
		// read, copy, or allocation (UploadWithRetry unwraps it the same way).
		// Only the rarer callers passing a plain reader (e.g. manifest chunks)
		// need an actual read, and io.ReadAll there cannot truncate the way a
		// size-hinted read would if the hint ever under-counted.
		var md5Base64 string
		if !wfs.option.Cipher {
			if br, ok := reader.(*util.BytesReader); ok {
				md5Base64 = contentMD5Base64(br.Bytes)
			} else {
				data, readErr := io.ReadAll(reader)
				if readErr != nil {
					glog.V(0).Infof("read chunk data %v: %v", filename, readErr)
					return nil, fmt.Errorf("read chunk data: %w", readErr)
				}
				md5Base64 = contentMD5Base64(data)
				reader = util.NewBytesReader(data)
			}
		}

		uploadOption := &operation.UploadOption{
			Filename:          filename,
			Cipher:            wfs.option.Cipher,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
			Md5:               md5Base64,
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

// contentMD5Base64 returns a chunk's MD5 in the exact encoding the volume
// server expects in the Content-MD5 header: the standard-base64 form of the
// raw 16-byte digest (see CreateNeedleFromRequest / ParseUpload). The server
// recomputes the digest over the received plaintext and rejects the upload on
// mismatch, then echoes the value back as the chunk ETag. Using any other
// encoding (hex, url-base64) would make every mount upload fail verification,
// so the encoding is a contract worth pinning.
func contentMD5Base64(data []byte) string {
	digest := md5.Sum(data)
	return base64.StdEncoding.EncodeToString(digest[:])
}
