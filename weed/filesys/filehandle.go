package filesys

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"net/http"
	"strings"
	"sync"
)

type FileHandle struct {
	// cache file has been written to
	dirtyPages    *ContinuousDirtyPages
	contentType   string
	dirtyMetadata bool
	handle        uint64

	f         *File
	RequestId fuse.RequestID // unique ID for request
	NodeId    fuse.NodeID    // file or directory the request is about
	Uid       uint32         // user ID of process making request
	Gid       uint32         // group ID of process making request
}

func newFileHandle(file *File, uid, gid uint32) *FileHandle {
	return &FileHandle{
		f:          file,
		dirtyPages: newDirtyPages(file),
		Uid:        uid,
		Gid:        gid,
	}
}

func (fh *FileHandle) InitializeToFile(file *File, uid, gid uint32) *FileHandle {
	newHandle := &FileHandle{
		f:          file,
		dirtyPages: fh.dirtyPages.InitializeToFile(file),
		Uid:        uid,
		Gid:        gid,
	}
	return newHandle
}
var _ = fs.Handle(&FileHandle{})

// var _ = fs.HandleReadAller(&FileHandle{})
var _ = fs.HandleReader(&FileHandle{})
var _ = fs.HandleFlusher(&FileHandle{})
var _ = fs.HandleWriter(&FileHandle{})
var _ = fs.HandleReleaser(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	glog.V(4).Infof("%s read fh %d: [%d,%d)", fh.f.fullpath(), fh.handle, req.Offset, req.Offset+int64(req.Size))

	// this value should come from the filer instead of the old f
	if len(fh.f.entry.Chunks) == 0 {
		glog.V(1).Infof("empty fh %v/%v", fh.f.dir.Path, fh.f.Name)
		return nil
	}

	buff := make([]byte, req.Size)

	chunkViews := filer2.ViewFromChunks(fh.f.entry.Chunks, req.Offset, req.Size)

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
		glog.V(4).Infof("%v/%v read fh lookup volume ids: %v", fh.f.dir.Path, fh.f.Name, err)
		return fmt.Errorf("failed to lookup volume ids %v: %v", vids, err)
	}

	var totalRead int64
	var wg sync.WaitGroup
	for _, chunkView := range chunkViews {
		wg.Add(1)
		go func(chunkView *filer2.ChunkView) {
			defer wg.Done()

			glog.V(4).Infof("read fh reading chunk: %+v", chunkView)

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

			glog.V(4).Infof("read fh read %d bytes: %+v", n, chunkView)
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

	glog.V(4).Infof("%+v/%v write fh %d: [%d,%d)", fh.f.dir.Path, fh.f.Name, fh.handle, req.Offset, req.Offset+int64(len(req.Data)))

	chunks, err := fh.dirtyPages.AddPage(ctx, req.Offset, req.Data)
	if err != nil {
		glog.Errorf("%+v/%v write fh %d: [%d,%d): %v", fh.f.dir.Path, fh.f.Name, fh.handle, req.Offset, req.Offset+int64(len(req.Data)), err)
		return fmt.Errorf("write %s/%s at [%d,%d): %v", fh.f.dir.Path, fh.f.Name, req.Offset, req.Offset+int64(len(req.Data)), err)
	}

	resp.Size = len(req.Data)

	if req.Offset == 0 {
		fh.contentType = http.DetectContentType(req.Data)
		fh.dirtyMetadata = true
	}

	for _, chunk := range chunks {
		fh.f.entry.Chunks = append(fh.f.entry.Chunks, chunk)
		glog.V(1).Infof("uploaded %s/%s to %s [%d,%d)", fh.f.dir.Path, fh.f.Name, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		fh.dirtyMetadata = true
	}

	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	glog.V(4).Infof("%v release fh %d", fh.f.fullpath(), fh.handle)

	fh.f.wfs.ReleaseHandle(fh.f.fullpath(), fuse.HandleID(fh.handle))

	fh.f.isOpen = false

	return nil
}

// Flush - experimenting with uploading at flush, this slows operations down till it has been
// completely flushed
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// fflush works at fh level
	// send the data to the OS
	glog.V(4).Infof("%s fh %d flush %v", fh.f.fullpath(), fh.handle, req)

	chunk, err := fh.dirtyPages.FlushToStorage(ctx)
	if err != nil {
		glog.Errorf("flush %s/%s: %v", fh.f.dir.Path, fh.f.Name, err)
		return fmt.Errorf("flush %s/%s: %v", fh.f.dir.Path, fh.f.Name, err)
	}
	if chunk != nil {
		fh.f.entry.Chunks = append(fh.f.entry.Chunks, chunk)
	}

	if !fh.dirtyMetadata {
		return nil
	}

	return fh.f.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		if fh.f.entry.Attributes != nil {
			fh.f.entry.Attributes.Mime = fh.contentType
			fh.f.entry.Attributes.Uid = req.Uid
			fh.f.entry.Attributes.Gid = req.Gid
		}

		request := &filer_pb.CreateEntryRequest{
			Directory: fh.f.dir.Path,
			Entry:     fh.f.entry,
		}

		glog.V(1).Infof("%s/%s set chunks: %v", fh.f.dir.Path, fh.f.Name, len(fh.f.entry.Chunks))
		for i, chunk := range fh.f.entry.Chunks {
			glog.V(1).Infof("%s/%s chunks %d: %v [%d,%d)", fh.f.dir.Path, fh.f.Name, i, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
		if _, err := client.CreateEntry(ctx, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}

		return nil
	})
}

func volumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}
