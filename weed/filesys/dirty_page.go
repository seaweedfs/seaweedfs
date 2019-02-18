package filesys

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
)

type ContinuousDirtyPages struct {
	hasData bool
	Offset  int64
	Size    int64
	Data    []byte
	f       *File
	lock    sync.Mutex
}

func newDirtyPages(file *File) *ContinuousDirtyPages {
	return &ContinuousDirtyPages{
		Data: nil,
		f:    file,
	}
}

func (pages *ContinuousDirtyPages) releaseResource() {
	if pages.Data != nil {
		pages.f.wfs.bufPool.Put(pages.Data)
		pages.Data = nil
		atomic.AddInt32(&counter, -1)
		glog.V(3).Infof("%s/%s releasing resource %d", pages.f.dir.Path, pages.f.Name, counter)
	}
}

var counter = int32(0)

func (pages *ContinuousDirtyPages) AddPage(ctx context.Context, offset int64, data []byte) (chunks []*filer_pb.FileChunk, err error) {

	pages.lock.Lock()
	defer pages.lock.Unlock()

	var chunk *filer_pb.FileChunk

	if len(data) > int(pages.f.wfs.option.ChunkSizeLimit) {
		// this is more than what buffer can hold.
		return pages.flushAndSave(ctx, offset, data)
	}

	if pages.Data == nil {
		pages.Data = pages.f.wfs.bufPool.Get().([]byte)
		atomic.AddInt32(&counter, 1)
		glog.V(3).Infof("%s/%s acquire resource %d", pages.f.dir.Path, pages.f.Name, counter)
	}

	if offset < pages.Offset || offset >= pages.Offset+int64(len(pages.Data)) ||
		pages.Offset+int64(len(pages.Data)) < offset+int64(len(data)) {
		// if the data is out of range,
		// or buffer is full if adding new data,
		// flush current buffer and add new data

		// println("offset", offset, "size", len(data), "existing offset", pages.Offset, "size", pages.Size)

		if chunk, err = pages.saveExistingPagesToStorage(ctx); err == nil {
			if chunk != nil {
				glog.V(4).Infof("%s/%s add save [%d,%d)", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size))
				chunks = append(chunks, chunk)
			}
		} else {
			glog.V(0).Infof("%s/%s add save [%d,%d): %v", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size), err)
			return
		}
		pages.Offset = offset
		copy(pages.Data, data)
		pages.Size = int64(len(data))
		return
	}

	if offset != pages.Offset+pages.Size {
		// when this happens, debug shows the data overlapping with existing data is empty
		// the data is not just append
		if offset == pages.Offset && int(pages.Size) < len(data) {
			// glog.V(2).Infof("pages[%d,%d) pages.Data len=%v, data len=%d, pages.Size=%d", pages.Offset, pages.Offset+pages.Size, len(pages.Data), len(data), pages.Size)
			copy(pages.Data[pages.Size:], data[pages.Size:])
		} else {
			if pages.Size != 0 {
				glog.V(1).Infof("%s/%s add page: pages [%d, %d) write [%d, %d)", pages.f.dir.Path, pages.f.Name, pages.Offset, pages.Offset+pages.Size, offset, offset+int64(len(data)))
			}
			return pages.flushAndSave(ctx, offset, data)
		}
	} else {
		copy(pages.Data[offset-pages.Offset:], data)
	}

	pages.Size = max(pages.Size, offset+int64(len(data))-pages.Offset)

	return
}

func (pages *ContinuousDirtyPages) flushAndSave(ctx context.Context, offset int64, data []byte) (chunks []*filer_pb.FileChunk, err error) {

	var chunk *filer_pb.FileChunk

	// flush existing
	if chunk, err = pages.saveExistingPagesToStorage(ctx); err == nil {
		if chunk != nil {
			glog.V(4).Infof("%s/%s flush existing [%d,%d)", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size))
			chunks = append(chunks, chunk)
		}
	} else {
		glog.V(0).Infof("%s/%s failed to flush1 [%d,%d): %v", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size), err)
		return
	}
	pages.Size = 0
	pages.Offset = 0

	// flush the new page
	if chunk, err = pages.saveToStorage(ctx, data, offset); err == nil {
		if chunk != nil {
			glog.V(4).Infof("%s/%s flush big request [%d,%d)", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size))
			chunks = append(chunks, chunk)
		}
	} else {
		glog.V(0).Infof("%s/%s failed to flush2 [%d,%d): %v", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size), err)
		return
	}

	return
}

func (pages *ContinuousDirtyPages) FlushToStorage(ctx context.Context) (chunk *filer_pb.FileChunk, err error) {

	pages.lock.Lock()
	defer pages.lock.Unlock()

	if pages.Size == 0 {
		return nil, nil
	}

	if chunk, err = pages.saveExistingPagesToStorage(ctx); err == nil {
		pages.Size = 0
		pages.Offset = 0
		if chunk != nil {
			glog.V(4).Infof("%s/%s flush [%d,%d)", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
	}
	return
}

func (pages *ContinuousDirtyPages) saveExistingPagesToStorage(ctx context.Context) (*filer_pb.FileChunk, error) {

	if pages.Size == 0 {
		return nil, nil
	}

	return pages.saveToStorage(ctx, pages.Data[:pages.Size], pages.Offset)
}

func (pages *ContinuousDirtyPages) saveToStorage(ctx context.Context, buf []byte, offset int64) (*filer_pb.FileChunk, error) {

	var fileId, host string
	var auth security.EncodedJwt

	if err := pages.f.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: pages.f.wfs.option.Replication,
			Collection:  pages.f.wfs.option.Collection,
			TtlSec:      pages.f.wfs.option.TtlSec,
			DataCenter:  pages.f.wfs.option.DataCenter,
		}

		resp, err := client.AssignVolume(ctx, request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}

		fileId, host, auth = resp.FileId, resp.Url, security.EncodedJwt(resp.Auth)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("filerGrpcAddress assign volume: %v", err)
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
	bufReader := bytes.NewReader(buf)
	uploadResult, err := operation.Upload(fileUrl, pages.f.Name, bufReader, false, "application/octet-stream", nil, auth)
	if err != nil {
		glog.V(0).Infof("upload data %v to %s: %v", pages.f.Name, fileUrl, err)
		return nil, fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v to %s: %v", pages.f.Name, fileUrl, err)
		return nil, fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	return &filer_pb.FileChunk{
		FileId: fileId,
		Offset: offset,
		Size:   uint64(len(buf)),
		Mtime:  time.Now().UnixNano(),
		ETag:   uploadResult.ETag,
	}, nil

}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
