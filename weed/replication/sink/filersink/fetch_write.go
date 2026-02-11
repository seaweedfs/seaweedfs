package filersink

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/schollz/progressbar/v3"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func (fs *FilerSink) replicateChunks(ctx context.Context, sourceChunks []*filer_pb.FileChunk, path string, sourceMtime int64) (replicatedChunks []*filer_pb.FileChunk, err error) {
	if len(sourceChunks) == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// a simple progress bar. Not ideal. Fix me.
	var bar *progressbar.ProgressBar
	if len(sourceChunks) > 1 {
		name := filepath.Base(path)
		bar = progressbar.NewOptions64(int64(len(sourceChunks)),
			progressbar.OptionClearOnFinish(),
			progressbar.OptionOnCompletion(func() {
				fmt.Fprint(os.Stderr, "\n")
			}),
			progressbar.OptionFullWidth(),
			progressbar.OptionSetDescription(name),
		)
	}

	replicatedChunks = make([]*filer_pb.FileChunk, len(sourceChunks))
	var errLock sync.Mutex
	setError := func(e error) {
		if e == nil {
			return
		}
		errLock.Lock()
		if err == nil {
			err = e
		}
		errLock.Unlock()
	}
	hasError := func() bool {
		errLock.Lock()
		defer errLock.Unlock()
		return err != nil
	}

	var wg sync.WaitGroup
	for chunkIndex, sourceChunk := range sourceChunks {
		if hasError() {
			break
		}

		if sourceChunk.IsChunkManifest {
			replicatedChunk, replicateErr := fs.replicateOneManifestChunk(ctx, sourceChunk, path, sourceMtime)
			if replicateErr != nil {
				setError(replicateErr)
				break
			}
			replicatedChunks[chunkIndex] = replicatedChunk
			if bar != nil {
				bar.Add(1)
			}
			continue
		}

		wg.Add(1)
		index, source := chunkIndex, sourceChunk
		fs.executor.Execute(func() {
			defer wg.Done()
			var replicatedChunk *filer_pb.FileChunk
			retryErr := util.Retry("replicate chunks", func() error {
				chunk, e := fs.replicateOneChunk(source, path, sourceMtime)
				if e != nil {
					return e
				}
				replicatedChunk = chunk
				return nil
			})
			if retryErr != nil {
				setError(retryErr)
				return
			}

			replicatedChunks[index] = replicatedChunk
			if bar != nil {
				bar.Add(1)
			}
		})
	}
	wg.Wait()

	return
}

func (fs *FilerSink) replicateOneChunk(sourceChunk *filer_pb.FileChunk, path string, sourceMtime int64) (*filer_pb.FileChunk, error) {

	fileId, err := fs.fetchAndWrite(sourceChunk, path, sourceMtime)
	if err != nil {
		return nil, fmt.Errorf("copy %s: %v", sourceChunk.GetFileIdString(), err)
	}

	return &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       sourceChunk.Offset,
		Size:         sourceChunk.Size,
		ModifiedTsNs: sourceChunk.ModifiedTsNs,
		ETag:         sourceChunk.ETag,
		SourceFileId: sourceChunk.GetFileIdString(),
		CipherKey:    sourceChunk.CipherKey,
		IsCompressed: sourceChunk.IsCompressed,
		SseType:      sourceChunk.SseType,
		SseMetadata:  sourceChunk.SseMetadata,
	}, nil
}

func (fs *FilerSink) replicateOneManifestChunk(ctx context.Context, sourceChunk *filer_pb.FileChunk, path string, sourceMtime int64) (*filer_pb.FileChunk, error) {
	resolvedChunks, err := filer.ResolveOneChunkManifest(ctx, fs.filerSource.LookupFileId, sourceChunk)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest %s: %w", sourceChunk.GetFileIdString(), err)
	}

	replicatedResolvedChunks, err := fs.replicateChunks(ctx, resolvedChunks, path, sourceMtime)
	if err != nil {
		return nil, fmt.Errorf("replicate manifest data chunks %s: %w", sourceChunk.GetFileIdString(), err)
	}

	manifestDataChunks := make([]*filer_pb.FileChunk, len(replicatedResolvedChunks))
	for i, chunk := range replicatedResolvedChunks {
		copied := *chunk
		manifestDataChunks[i] = &copied
	}
	filer_pb.BeforeEntrySerialization(manifestDataChunks)
	manifestData, err := proto.Marshal(&filer_pb.FileChunkManifest{
		Chunks: manifestDataChunks,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal manifest %s: %w", sourceChunk.GetFileIdString(), err)
	}

	manifestFileId, err := fs.uploadManifestChunk(path, sourceMtime, sourceChunk.GetFileIdString(), manifestData)
	if err != nil {
		return nil, err
	}

	return &filer_pb.FileChunk{
		FileId:          manifestFileId,
		Offset:          sourceChunk.Offset,
		Size:            sourceChunk.Size,
		ModifiedTsNs:    sourceChunk.ModifiedTsNs,
		ETag:            sourceChunk.ETag,
		SourceFileId:    sourceChunk.GetFileIdString(),
		IsChunkManifest: true,
	}, nil
}

func (fs *FilerSink) uploadManifestChunk(path string, sourceMtime int64, sourceFileId string, manifestData []byte) (fileId string, err error) {
	uploader, err := operation.NewUploader()
	if err != nil {
		glog.V(0).Infof("upload manifest data %v: %v", sourceFileId, err)
		return "", fmt.Errorf("upload manifest data: %w", err)
	}

	retryName := fmt.Sprintf("replicate manifest chunk %s", sourceFileId)
	err = util.RetryUntil(retryName, func() error {
		currentFileId, uploadResult, uploadErr, _ := uploader.UploadWithRetry(
			fs,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: fs.replication,
				Collection:  fs.collection,
				TtlSec:      fs.ttlSec,
				DataCenter:  fs.dataCenter,
				DiskType:    fs.diskType,
				Path:        path,
			},
			&operation.UploadOption{
				Filename:          "",
				Cipher:            false,
				IsInputCompressed: false,
				MimeType:          "application/octet-stream",
				PairMap:           nil,
				RetryForever:      false,
			},
			func(host, fileId string) string {
				return fs.buildUploadUrl(host, fileId)
			},
			bytes.NewReader(manifestData),
		)
		if uploadErr != nil {
			return fmt.Errorf("upload manifest data: %w", uploadErr)
		}
		if uploadResult.Error != "" {
			return fmt.Errorf("upload manifest result: %v", uploadResult.Error)
		}

		fileId = currentFileId
		return nil
	}, func(uploadErr error) (shouldContinue bool) {
		if fs.hasSourceNewerVersion(path, sourceMtime) {
			glog.V(1).Infof("skip retrying stale source manifest %s for %s: %v", sourceFileId, path, uploadErr)
			return false
		}
		glog.V(0).Infof("upload source manifest %v: %v", sourceFileId, uploadErr)
		return true
	})
	if err != nil {
		return "", err
	}

	return fileId, nil
}

func (fs *FilerSink) fetchAndWrite(sourceChunk *filer_pb.FileChunk, path string, sourceMtime int64) (fileId string, err error) {
	uploader, err := operation.NewUploader()
	if err != nil {
		glog.V(0).Infof("upload source data %v: %v", sourceChunk.GetFileIdString(), err)
		return "", fmt.Errorf("upload data: %w", err)
	}

	retryName := fmt.Sprintf("replicate chunk %s", sourceChunk.GetFileIdString())
	err = util.RetryUntil(retryName, func() error {
		filename, header, resp, readErr := fs.filerSource.ReadPart(sourceChunk.GetFileIdString())
		if readErr != nil {
			return fmt.Errorf("read part %s: %w", sourceChunk.GetFileIdString(), readErr)
		}
		defer util_http.CloseResponse(resp)

		currentFileId, uploadResult, uploadErr, _ := uploader.UploadWithRetry(
			fs,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: fs.replication,
				Collection:  fs.collection,
				TtlSec:      fs.ttlSec,
				DataCenter:  fs.dataCenter,
				DiskType:    fs.diskType,
				Path:        path,
			},
			&operation.UploadOption{
				Filename:          filename,
				Cipher:            false,
				IsInputCompressed: "gzip" == header.Get("Content-Encoding"),
				MimeType:          header.Get("Content-Type"),
				PairMap:           nil,
				RetryForever:      false,
			},
			func(host, fileId string) string {
				fileUrl := fs.buildUploadUrl(host, fileId)
				glog.V(4).Infof("replicating %s to %s header:%+v", filename, fileUrl, header)
				return fileUrl
			},
			resp.Body,
		)
		if uploadErr != nil {
			return fmt.Errorf("upload data: %w", uploadErr)
		}
		if uploadResult.Error != "" {
			return fmt.Errorf("upload result: %v", uploadResult.Error)
		}

		fileId = currentFileId
		return nil
	}, func(uploadErr error) (shouldContinue bool) {
		if fs.hasSourceNewerVersion(path, sourceMtime) {
			glog.V(1).Infof("skip retrying stale source %s for %s: %v", sourceChunk.GetFileIdString(), path, uploadErr)
			return false
		}
		glog.V(0).Infof("upload source data %v: %v", sourceChunk.GetFileIdString(), uploadErr)
		return true
	})
	if err != nil {
		return "", err
	}

	return fileId, nil
}

func (fs *FilerSink) buildUploadUrl(host, fileId string) string {
	if fs.writeChunkByFiler {
		return fmt.Sprintf("http://%s/?proxyChunkId=%s", fs.address, fileId)
	}
	return fmt.Sprintf("http://%s/%s", host, fileId)
}

func (fs *FilerSink) hasSourceNewerVersion(targetPath string, sourceMtime int64) bool {
	if sourceMtime <= 0 || fs.filerSource == nil {
		return false
	}

	sourcePath, ok := fs.targetPathToSourcePath(targetPath)
	if !ok {
		return false
	}

	sourceEntry, err := filer_pb.GetEntry(context.Background(), fs.filerSource, sourcePath)
	if err != nil {
		glog.V(1).Infof("lookup source entry %s: %v", sourcePath, err)
		return false
	}
	if sourceEntry == nil {
		glog.V(1).Infof("source entry %s no longer exists", sourcePath)
		return true
	}

	return sourceEntry.Attributes != nil && sourceEntry.Attributes.Mtime > sourceMtime
}

func (fs *FilerSink) targetPathToSourcePath(targetPath string) (util.FullPath, bool) {
	if fs.filerSource == nil {
		return "", false
	}

	normalizePath := func(p string) string {
		p = strings.TrimSuffix(p, "/")
		if p == "" {
			return "/"
		}
		return p
	}

	sourceRoot := normalizePath(fs.filerSource.Dir)
	targetRoot := normalizePath(fs.dir)
	targetPath = normalizePath(targetPath)

	var relative string
	switch {
	case targetRoot == "/":
		relative = strings.TrimPrefix(targetPath, "/")
	case targetPath == targetRoot:
		relative = ""
	case strings.HasPrefix(targetPath, targetRoot+"/"):
		relative = targetPath[len(targetRoot)+1:]
	default:
		return "", false
	}

	if relative == "" {
		return util.FullPath(sourceRoot), true
	}
	return util.FullPath(sourceRoot).Child(relative), true
}

var _ = filer_pb.FilerClient(&FilerSink{})

func (fs *FilerSink) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcClient(streamingMode, fs.signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.grpcAddress, false, fs.grpcDialOption)

}

func (fs *FilerSink) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (fs *FilerSink) GetDataCenter() string {
	return fs.dataCenter
}
