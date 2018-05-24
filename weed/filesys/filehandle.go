package filesys

import (
	"bazil.org/fuse/fs"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"context"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"bazil.org/fuse"
	"bytes"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"time"
)

type FileHandle struct {
	// cache file has been written to
	dirty bool

	cachePath string

	handle uint64

	wfs        *WFS
	dirPath    string
	name       string
	RequestId  fuse.RequestID // unique ID for request
	NodeId     fuse.NodeID    // file or directory the request is about
	Uid        uint32         // user ID of process making request
	Gid        uint32         // group ID of process making request
	attributes *filer_pb.FuseAttributes
	Chunks     []*filer_pb.FileChunk
}

var _ = fs.Handle(&FileHandle{})
var _ = fs.HandleReadAller(&FileHandle{})
// var _ = fs.HandleReader(&FileHandle{})
var _ = fs.HandleFlusher(&FileHandle{})
var _ = fs.HandleWriter(&FileHandle{})
var _ = fs.HandleReleaser(&FileHandle{})

func (fh *FileHandle) ReadAll(ctx context.Context) (content []byte, err error) {

	glog.V(3).Infof("%v/%v read all fh ", fh.dirPath, fh.name)

	if len(fh.Chunks) == 0 {
		glog.V(0).Infof("empty fh %v/%v", fh.dirPath, fh.name)
		return
	}

	err = fh.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		// FIXME: need to either use Read() or implement differently
		chunks, _ := filer2.CompactFileChunks(fh.Chunks)
		glog.V(1).Infof("read fh %v/%v %d/%d chunks", fh.dirPath, fh.name, len(chunks), len(fh.Chunks))
		for i, chunk := range chunks {
			glog.V(1).Infof("read fh %v/%v %d/%d chunk %s [%d,%d)", fh.dirPath, fh.name, i, len(chunks), chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
		request := &filer_pb.GetFileContentRequest{
			FileId: chunks[0].FileId,
		}

		glog.V(1).Infof("read fh content %d chunk %s [%d,%d): %v", len(chunks),
			chunks[0].FileId, chunks[0].Offset, chunks[0].Offset+int64(chunks[0].Size), request)
		resp, err := client.GetFileContent(ctx, request)
		if err != nil {
			return err
		}

		content = resp.Content

		return nil
	})

	return content, err
}

// Write to the file handle
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	// write the request to volume servers

	glog.V(3).Infof("%+v/%v write fh: %+v", fh.dirPath, fh.name, req)

	var fileId, host string

	if err := fh.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: "000",
			Collection:  "",
		}

		glog.V(1).Infof("assign volume: %v", request)
		resp, err := client.AssignVolume(ctx, request)
		if err != nil {
			return err
		}

		fileId, host = resp.FileId, resp.Url

		return nil
	}); err != nil {
		return fmt.Errorf("filer assign volume: %v", err)
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
	bufReader := bytes.NewReader(req.Data)
	uploadResult, err := operation.Upload(fileUrl, fh.name, bufReader, false, "application/octet-stream", nil, "")
	if err != nil {
		return fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		return fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	resp.Size = int(uploadResult.Size)

	fh.Chunks = append(fh.Chunks, &filer_pb.FileChunk{
		FileId: fileId,
		Offset: req.Offset,
		Size:   uint64(uploadResult.Size),
		Mtime:  time.Now().UnixNano(),
	})

	glog.V(1).Infof("uploaded %s/%s to: %v, [%d,%d)", fh.dirPath, fh.name, fileUrl, req.Offset, req.Offset+int64(resp.Size))

	fh.dirty = true

	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	glog.V(3).Infof("%+v/%v release fh", fh.dirPath, fh.name)

	return nil
}

// Flush - experimenting with uploading at flush, this slows operations down till it has been
// completely flushed
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// fflush works at fh level
	// send the data to the OS
	glog.V(3).Infof("%s/%s fh flush %v", fh.dirPath, fh.name, req)

	if !fh.dirty {
		return nil
	}

	if len(fh.Chunks) == 0 {
		glog.V(2).Infof("fh %s/%s flush skipping empty: %v", fh.dirPath, fh.name, req)
		return nil
	}

	err := fh.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory: fh.dirPath,
			Entry: &filer_pb.Entry{
				Name:       fh.name,
				Attributes: fh.attributes,
				Chunks:     fh.Chunks,
			},
		}

		glog.V(1).Infof("%s/%s set chunks: %v", fh.dirPath, fh.name, len(fh.Chunks))
		if _, err := client.UpdateEntry(ctx, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}

		return nil
	})

	if err == nil {
		fh.dirty = false
	}

	return err
}
