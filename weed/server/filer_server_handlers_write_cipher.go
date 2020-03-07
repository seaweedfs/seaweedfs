package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

// handling single chunk POST or PUT upload
func (fs *FilerServer) encrypt(ctx context.Context, w http.ResponseWriter, r *http.Request,
	replication string, collection string, dataCenter string) (filerResult *FilerPostResult, err error) {

	fileId, urlLocation, auth, err := fs.assignNewFileInfo(w, r, replication, collection, dataCenter)

	if err != nil || fileId == "" || urlLocation == "" {
		return nil, fmt.Errorf("fail to allocate volume for %s, collection:%s, datacenter:%s", r.URL.Path, collection, dataCenter)
	}

	glog.V(4).Infof("write %s to %v", r.URL.Path, urlLocation)

	// Note: gzip(cipher(data)), cipher data first, then gzip

	sizeLimit := int64(fs.option.MaxMB) * 1024 * 1024

	pu, err := needle.ParseUpload(r, sizeLimit)
	data := pu.Data
	uncompressedData := pu.Data
	cipherKey := util.GenCipherKey()
	if pu.IsGzipped {
		uncompressedData = pu.UncompressedData
		data, err = util.Encrypt(pu.UncompressedData, cipherKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt input: %v", err)
		}
	}
	if pu.MimeType == "" {
		pu.MimeType = http.DetectContentType(uncompressedData)
	}

	uploadResult, uploadError := operation.Upload(urlLocation, pu.FileName, true, bytes.NewReader(data), pu.IsGzipped, "", pu.PairMap, auth)
	if uploadError != nil {
		return nil, fmt.Errorf("upload to volume server: %v", uploadError)
	}

	// Save to chunk manifest structure
	fileChunks := []*filer_pb.FileChunk{
		{
			FileId:    fileId,
			Offset:    0,
			Size:      uint64(uploadResult.Size),
			Mtime:     time.Now().UnixNano(),
			ETag:      uploadResult.ETag,
			CipherKey: uploadResult.CipherKey,
		},
	}

	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if pu.FileName != "" {
			path += pu.FileName
		}
	}

	entry := &filer2.Entry{
		FullPath: filer2.FullPath(path),
		Attr: filer2.Attr{
			Mtime:       time.Now(),
			Crtime:      time.Now(),
			Mode:        0660,
			Uid:         OS_UID,
			Gid:         OS_GID,
			Replication: replication,
			Collection:  collection,
			TtlSec:      int32(util.ParseInt(r.URL.Query().Get("ttl"), 0)),
			Mime:        pu.MimeType,
		},
		Chunks: fileChunks,
	}

	filerResult = &FilerPostResult{
		Name: pu.FileName,
		Size: int64(pu.OriginalDataSize),
	}

	if dbErr := fs.filer.CreateEntry(ctx, entry, false); dbErr != nil {
		fs.filer.DeleteChunks(entry.Chunks)
		err = dbErr
		filerResult.Error = dbErr.Error()
		return
	}

	return
}
