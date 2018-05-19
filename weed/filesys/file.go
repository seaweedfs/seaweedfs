package filesys

import (
	"context"
	"fmt"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"path/filepath"
	"os"
	"time"
	"bytes"
	"github.com/chrislusf/seaweedfs/weed/operation"
)

var _ = fs.Node(&File{})
// var _ = fs.NodeOpener(&File{})
var _ = fs.NodeFsyncer(&File{})
var _ = fs.Handle(&File{})
var _ = fs.HandleReadAller(&File{})
// var _ = fs.HandleReader(&File{})
var _ = fs.HandleFlusher(&File{})
var _ = fs.HandleWriter(&File{})
var _ = fs.HandleReleaser(&File{})

type File struct {
	Chunks []*filer_pb.FileChunk
	Name   string
	dir    *Dir
	wfs    *WFS
}

func (file *File) Attr(context context.Context, attr *fuse.Attr) error {
	fullPath := filepath.Join(file.dir.Path, file.Name)
	item := file.wfs.listDirectoryEntriesCache.Get(fullPath)
	var attributes *filer_pb.FuseAttributes
	if item != nil {
		attributes = item.Value().(*filer_pb.FuseAttributes)
		glog.V(1).Infof("read cached file %v attributes", file.Name)
	} else {
		err := file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.GetFileAttributesRequest{
				Name:      file.Name,
				ParentDir: file.dir.Path,
			}

			glog.V(1).Infof("read file size: %v", request)
			resp, err := client.GetFileAttributes(context, request)
			if err != nil {
				glog.V(0).Infof("read file attributes %v: %v", request, err)
				return err
			}

			attributes = resp.Attributes

			return nil
		})

		if err != nil {
			return err
		}
	}

	attr.Mode = os.FileMode(attributes.FileMode)
	attr.Size = attributes.FileSize
	attr.Mtime = time.Unix(attributes.Mtime, 0)
	attr.Gid = attributes.Gid
	attr.Uid = attributes.Uid

	return nil

}

func (file *File) ReadAll(ctx context.Context) (content []byte, err error) {

	// fmt.Printf("read all file %+v/%v\n", file.dir.Path, file.Name)

	if len(file.Chunks) == 0 {
		glog.V(0).Infof("empty file %v/%v", file.dir.Path, file.Name)
		return
	}

	err = file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		// FIXME: need to either use Read() or implement differently
		request := &filer_pb.GetFileContentRequest{
			FileId: file.Chunks[0].FileId,
		}

		glog.V(1).Infof("read file content: %v", request)
		resp, err := client.GetFileContent(ctx, request)
		if err != nil {
			return err
		}

		content = resp.Content

		return nil
	})

	return content, err
}

func (file *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// fsync works at OS level
	// write the file chunks to the filer
	fmt.Printf("flush file %+v\n", req)

	return nil
}

func (file *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// fflush works at file level
	// send the data to the OS
	glog.V(3).Infof("file flush %v", req)

	if len(file.Chunks) == 0 {
		glog.V(2).Infof("file flush skipping empty %v", req)
		return nil
	}

	err := file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AppendFileChunksRequest{
			Directory: file.dir.Path,
			Entry: &filer_pb.Entry{
				Name:   file.Name,
				Chunks: file.Chunks,
			},
		}

		glog.V(1).Infof("append chunks: %v", request)
		if _, err := client.AppendFileChunks(ctx, request); err != nil {
			return fmt.Errorf("create file: %v", err)
		}

		return nil
	})

	return err
}

func (file *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// write the request to volume servers
	// fmt.Printf("write file %+v\n", req)

	var fileId, host string

	if err := file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

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
	uploadResult, err := operation.Upload(fileUrl, file.Name, bufReader, false, "application/octet-stream", nil, "")
	if err != nil {
		return fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		return fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	glog.V(1).Infof("uploaded %s/%s to: %v", file.dir.Path, file.Name, fileUrl)

	file.Chunks = append(file.Chunks, &filer_pb.FileChunk{
		FileId: fileId,
		Offset: req.Offset,
		Size:   uint64(uploadResult.Size),
		Mtime:  time.Now().UnixNano(),
	})

	resp.Size = int(uploadResult.Size)

	return nil
}

func (file *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	// fmt.Printf("release file %+v\n", req)

	return nil
}
