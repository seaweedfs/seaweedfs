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
	"github.com/chrislusf/seaweedfs/weed/filer2"
)

var _ = fs.Node(&File{})
var _ = fs.NodeOpener(&File{})
var _ = fs.NodeFsyncer(&File{})
var _ = fs.Handle(&File{})
var _ = fs.HandleReadAller(&File{})
// var _ = fs.HandleReader(&File{})
var _ = fs.HandleFlusher(&File{})
var _ = fs.HandleWriter(&File{})
var _ = fs.HandleReleaser(&File{})
var _ = fs.NodeSetattrer(&File{})

type File struct {
	Chunks     []*filer_pb.FileChunk
	Name       string
	dir        *Dir
	wfs        *WFS
	isOpened   bool
	attributes *filer_pb.FuseAttributes
}

func (file *File) Attr(context context.Context, attr *fuse.Attr) error {

	if !file.isOpened || file.attributes == nil {
		fullPath := filepath.Join(file.dir.Path, file.Name)
		item := file.wfs.listDirectoryEntriesCache.Get(fullPath)
		if item != nil {
			file.attributes = item.Value().(*filer_pb.FuseAttributes)
			glog.V(1).Infof("read cached file %v attributes", file.Name)
		} else {
			err := file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

				request := &filer_pb.GetEntryAttributesRequest{
					Name:      file.Name,
					ParentDir: file.dir.Path,
				}

				glog.V(1).Infof("read file size: %v", request)
				resp, err := client.GetEntryAttributes(context, request)
				if err != nil {
					glog.V(0).Infof("read file attributes %v: %v", request, err)
					return err
				}

				file.attributes = resp.Attributes

				return nil
			})

			if err != nil {
				return err
			}
		}
	}

	attr.Mode = os.FileMode(file.attributes.FileMode)
	attr.Size = filer2.TotalSize(file.Chunks)
	attr.Mtime = time.Unix(file.attributes.Mtime, 0)
	attr.Gid = file.attributes.Gid
	attr.Uid = file.attributes.Uid

	return nil

}

func (file *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	fullPath := filepath.Join(file.dir.Path, file.Name)

	fmt.Printf("Open %v %+v\n", fullPath, req)
	file.isOpened = true

	return file, nil

}

func (file *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	fullPath := filepath.Join(file.dir.Path, file.Name)

	fmt.Printf("Setattr %v %+v\n", fullPath, req)
	if req.Valid.Size() {

		if req.Size == 0 {
			fmt.Printf("truncate %v \n", fullPath)
			file.Chunks = nil
		}
		file.attributes.FileSize = req.Size
	}
	if req.Valid.Mode() {
		file.attributes.FileMode = uint32(req.Mode)
	}

	if req.Valid.Uid() {
		file.attributes.Uid = req.Uid
	}

	if req.Valid.Gid() {
		file.attributes.Gid = req.Gid
	}

	if req.Valid.Mtime() {
		file.attributes.Mtime = req.Mtime.Unix()
	}

	return nil

}

func (file *File) ReadAll(ctx context.Context) (content []byte, err error) {

	fmt.Printf("read all file %+v/%v\n", file.dir.Path, file.Name)

	if len(file.Chunks) == 0 {
		glog.V(0).Infof("empty file %v/%v", file.dir.Path, file.Name)
		return
	}

	err = file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		// FIXME: need to either use Read() or implement differently
		chunks, _ := filer2.CompactFileChunks(file.Chunks)
		glog.V(1).Infof("read file %v/%v %d/%d chunks", file.dir.Path, file.Name, len(chunks), len(file.Chunks))
		request := &filer_pb.GetFileContentRequest{
			FileId: chunks[0].FileId,
		}

		glog.V(1).Infof("read file content %d chunk %s [%d,%d): %v", len(chunks),
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

		request := &filer_pb.UpdateEntryRequest{
			Directory: file.dir.Path,
			Entry: &filer_pb.Entry{
				Name:       file.Name,
				Attributes: file.attributes,
				Chunks:     file.Chunks,
			},
		}

		glog.V(1).Infof("append chunks: %v", request)
		if _, err := client.UpdateEntry(ctx, request); err != nil {
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

	resp.Size = int(uploadResult.Size)

	file.Chunks = append(file.Chunks, &filer_pb.FileChunk{
		FileId: fileId,
		Offset: req.Offset,
		Size:   uint64(uploadResult.Size),
		Mtime:  time.Now().UnixNano(),
	})

	glog.V(1).Infof("uploaded %s/%s to: %v, [%d,%d)", file.dir.Path, file.Name, fileUrl, req.Offset, req.Offset+int64(resp.Size))

	return nil
}

func (file *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	fmt.Printf("release file %+v\n", req)
	file.isOpened = false

	return nil
}
