package filesys

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type ContinuousDirtyPages struct {
	hasData bool
	Offset  int64
	Size    int64
	Data    []byte
	f       *File
}

func newDirtyPages(file *File) *ContinuousDirtyPages {
	return &ContinuousDirtyPages{
		Data: make([]byte, file.wfs.chunkSizeLimit),
		f:    file,
	}
}

func (pages *ContinuousDirtyPages) AddPage(ctx context.Context, offset int64, data []byte) (chunks []*filer_pb.FileChunk, err error) {

	var chunk *filer_pb.FileChunk

	if len(data) > len(pages.Data) {
		// this is more than what buffer can hold.

		// flush existing
		if chunk, err = pages.saveExistingPagesToStorage(ctx); err == nil {
			if chunk != nil {
				glog.V(4).Infof("%s/%s flush existing [%d,%d)", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size))
			}
			chunks = append(chunks, chunk)
		} else {
			glog.V(0).Infof("%s/%s failed to flush1 [%d,%d): %v", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size), err)
			return
		}
		pages.Size = 0

		// flush the big page
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
		pages.Size = int64(len(data))
		copy(pages.Data, data)
		return
	}

	copy(pages.Data[offset-pages.Offset:], data)
	pages.Size = max(pages.Size, offset+int64(len(data))-pages.Offset)

	return
}

func (pages *ContinuousDirtyPages) FlushToStorage(ctx context.Context) (chunk *filer_pb.FileChunk, err error) {

	if pages.Size == 0 {
		return nil, nil
	}

	if chunk, err = pages.saveExistingPagesToStorage(ctx); err == nil {
		pages.Size = 0
		if chunk != nil {
			glog.V(4).Infof("%s/%s flush [%d,%d)", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
	}
	return
}

func (pages *ContinuousDirtyPages) saveExistingPagesToStorage(ctx context.Context) (*filer_pb.FileChunk, error) {
	return pages.saveToStorage(ctx, pages.Data[:pages.Size], pages.Offset)
}

func (pages *ContinuousDirtyPages) saveToStorage(ctx context.Context, buf []byte, offset int64) (*filer_pb.FileChunk, error) {

	if pages.Size == 0 {
		return nil, nil
	}

	var fileId, host string

	if err := pages.f.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: pages.f.wfs.replication,
			Collection:  pages.f.wfs.collection,
			TtlSec:      pages.f.wfs.ttlSec,
			DataCenter:  pages.f.wfs.dataCenter,
		}

		resp, err := client.AssignVolume(ctx, request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}

		fileId, host = resp.FileId, resp.Url

		return nil
	}); err != nil {
		return nil, fmt.Errorf("filerGrpcAddress assign volume: %v", err)
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
	bufReader := bytes.NewReader(pages.Data[:pages.Size])
	uploadResult, err := operation.Upload(fileUrl, pages.f.Name, bufReader, false, "application/octet-stream", nil, "")
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
	}, nil

}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
