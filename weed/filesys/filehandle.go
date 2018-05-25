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
	"strings"
	"sync"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type FileHandle struct {
	// cache file has been written to
	dirty bool

	cachePath string

	handle uint64

	f         *File
	RequestId fuse.RequestID // unique ID for request
	NodeId    fuse.NodeID    // file or directory the request is about
	Uid       uint32         // user ID of process making request
	Gid       uint32         // group ID of process making request
}

var _ = fs.Handle(&FileHandle{})
// var _ = fs.HandleReadAller(&FileHandle{})
var _ = fs.HandleReader(&FileHandle{})
var _ = fs.HandleFlusher(&FileHandle{})
var _ = fs.HandleWriter(&FileHandle{})
var _ = fs.HandleReleaser(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	glog.V(3).Infof("%v/%v read fh: [%d,%d)", fh.f.dir.Path, fh.f.Name, req.Offset, req.Offset+int64(req.Size))

	if len(fh.f.Chunks) == 0 {
		glog.V(0).Infof("empty fh %v/%v", fh.f.dir.Path, fh.f.Name)
		return fmt.Errorf("empty file %v/%v", fh.f.dir.Path, fh.f.Name)
	}

	buff := make([]byte, req.Size)

	chunkViews := filer2.ReadFromChunks(fh.f.Chunks, req.Offset, req.Size)

	var vids []string
	for _, chunkView := range chunkViews {
		vids = append(vids, volumeId(chunkView.FileId))
	}

	vid2Locations := make(map[string]*filer_pb.Locations)

	err := fh.f.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		glog.V(4).Infof("read fh lookup volume id locations: %v", vids)
		resp, err := client.LookupVolume(ctx, &filer_pb.LookupVolumeRequest{
			VolumeIds: vids,
		})
		if err != nil {
			return err
		}

		vid2Locations = resp.LocationsMap

		return nil
	})

	if err != nil {
		glog.V(3).Infof("%v/%v read fh lookup volume ids: %v", fh.f.dir.Path, fh.f.Name, err)
		return fmt.Errorf("failed to lookup volume ids %v: %v", vids, err)
	}

	var totalRead int64
	var wg sync.WaitGroup
	for _, chunkView := range chunkViews {
		wg.Add(1)
		go func(chunkView *filer2.ChunkView) {
			defer wg.Done()

			glog.V(3).Infof("read fh reading chunk: %+v", chunkView)

			locations := vid2Locations[volumeId(chunkView.FileId)]
			if locations == nil || len(locations.Locations) == 0 {
				glog.V(0).Infof("failed to locate %s", chunkView.FileId)
				err = fmt.Errorf("failed to locate %s", chunkView.FileId)
				return
			}

			var n int64
			n, err = util.ReadUrl(
				fmt.Sprintf("http://%s/%s", locations.Locations[0].Url, chunkView.FileId),
				chunkView.Offset,
				int(chunkView.Size),
				buff[chunkView.LogicOffset-req.Offset:chunkView.LogicOffset-req.Offset+int64(chunkView.Size)])

			if err != nil {

				glog.V(0).Infof("%v/%v read http://%s/%v %v bytes: %v", fh.f.dir.Path, fh.f.Name, locations.Locations[0].Url, chunkView.FileId, n, err)

				err = fmt.Errorf("failed to read http://%s/%s: %v",
					locations.Locations[0].Url, chunkView.FileId, err)
				return
			}

			glog.V(3).Infof("read fh read %d bytes: %+v", n, chunkView)
			totalRead += n

		}(chunkView)
	}
	wg.Wait()

	resp.Data = buff[:totalRead]

	return err
}

// Write to the file handle
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	// write the request to volume servers

	glog.V(3).Infof("%+v/%v write fh: %+v", fh.f.dir.Path, fh.f.Name, req)

	var fileId, host string

	if err := fh.f.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: "000",
			Collection:  "",
		}

		resp, err := client.AssignVolume(ctx, request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}

		fileId, host = resp.FileId, resp.Url

		return nil
	}); err != nil {
		return fmt.Errorf("filer assign volume: %v", err)
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
	bufReader := bytes.NewReader(req.Data)
	uploadResult, err := operation.Upload(fileUrl, fh.f.Name, bufReader, false, "application/octet-stream", nil, "")
	if err != nil {
		glog.V(0).Infof("upload data %v to %s: %v", req, fileUrl, err)
		return fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v to %s: %v", req, fileUrl, err)
		return fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	resp.Size = int(uploadResult.Size)

	fh.f.Chunks = append(fh.f.Chunks, &filer_pb.FileChunk{
		FileId: fileId,
		Offset: req.Offset,
		Size:   uint64(uploadResult.Size),
		Mtime:  time.Now().UnixNano(),
	})

	glog.V(1).Infof("uploaded %s/%s to: %v, [%d,%d)", fh.f.dir.Path, fh.f.Name, fileUrl, req.Offset, req.Offset+int64(resp.Size))

	fh.dirty = true

	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	glog.V(3).Infof("%+v/%v release fh", fh.f.dir.Path, fh.f.Name)

	return nil
}

// Flush - experimenting with uploading at flush, this slows operations down till it has been
// completely flushed
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// fflush works at fh level
	// send the data to the OS
	glog.V(3).Infof("%s/%s fh flush %v", fh.f.dir.Path, fh.f.Name, req)

	if !fh.dirty {
		return nil
	}

	if len(fh.f.Chunks) == 0 {
		glog.V(2).Infof("fh %s/%s flush skipping empty: %v", fh.f.dir.Path, fh.f.Name, req)
		return nil
	}

	err := fh.f.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory: fh.f.dir.Path,
			Entry: &filer_pb.Entry{
				Name:       fh.f.Name,
				Attributes: fh.f.attributes,
				Chunks:     fh.f.Chunks,
			},
		}

		glog.V(1).Infof("%s/%s set chunks: %v", fh.f.dir.Path, fh.f.Name, len(fh.f.Chunks))
		for i, chunk := range fh.f.Chunks {
			glog.V(1).Infof("%s/%s chunks %d: %v [%d,%d)", fh.f.dir.Path, fh.f.Name, i, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
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

func volumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}
