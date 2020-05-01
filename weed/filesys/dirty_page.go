package filesys

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
)

type ContinuousDirtyPages struct {
	intervals   *ContinuousIntervals
	f           *File
	lock        sync.Mutex
	collection  string
	replication string
}

func newDirtyPages(file *File) *ContinuousDirtyPages {
	return &ContinuousDirtyPages{
		intervals: &ContinuousIntervals{},
		f:         file,
	}
}

func (pages *ContinuousDirtyPages) releaseResource() {
}

var counter = int32(0)

func (pages *ContinuousDirtyPages) AddPage(offset int64, data []byte) (chunks []*filer_pb.FileChunk, err error) {

	pages.lock.Lock()
	defer pages.lock.Unlock()

	glog.V(3).Infof("%s AddPage [%d,%d)", pages.f.fullpath(), offset, offset+int64(len(data)))

	if len(data) > int(pages.f.wfs.option.ChunkSizeLimit) {
		// this is more than what buffer can hold.
		return pages.flushAndSave(offset, data)
	}

	pages.intervals.AddInterval(data, offset)

	var chunk *filer_pb.FileChunk
	var hasSavedData bool

	if pages.intervals.TotalSize() > pages.f.wfs.option.ChunkSizeLimit {
		chunk, hasSavedData, err = pages.saveExistingLargestPageToStorage()
		if hasSavedData {
			chunks = append(chunks, chunk)
		}
	}

	return
}

func (pages *ContinuousDirtyPages) flushAndSave(offset int64, data []byte) (chunks []*filer_pb.FileChunk, err error) {

	var chunk *filer_pb.FileChunk
	var newChunks []*filer_pb.FileChunk

	// flush existing
	if newChunks, err = pages.saveExistingPagesToStorage(); err == nil {
		if newChunks != nil {
			chunks = append(chunks, newChunks...)
		}
	} else {
		return
	}

	// flush the new page
	if chunk, err = pages.saveToStorage(bytes.NewReader(data), offset, int64(len(data))); err == nil {
		if chunk != nil {
			glog.V(4).Infof("%s/%s flush big request [%d,%d) to %s", pages.f.dir.FullPath(), pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size), chunk.FileId)
			chunks = append(chunks, chunk)
		}
	} else {
		glog.V(0).Infof("%s/%s failed to flush2 [%d,%d): %v", pages.f.dir.FullPath(), pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size), err)
		return
	}

	return
}

func (pages *ContinuousDirtyPages) FlushToStorage() (chunks []*filer_pb.FileChunk, err error) {

	pages.lock.Lock()
	defer pages.lock.Unlock()

	return pages.saveExistingPagesToStorage()
}

func (pages *ContinuousDirtyPages) saveExistingPagesToStorage() (chunks []*filer_pb.FileChunk, err error) {

	var hasSavedData bool
	var chunk *filer_pb.FileChunk

	for {

		chunk, hasSavedData, err = pages.saveExistingLargestPageToStorage()
		if !hasSavedData {
			return chunks, err
		}

		if err == nil {
			chunks = append(chunks, chunk)
		} else {
			return
		}
	}

}

func (pages *ContinuousDirtyPages) saveExistingLargestPageToStorage() (chunk *filer_pb.FileChunk, hasSavedData bool, err error) {

	maxList := pages.intervals.RemoveLargestIntervalLinkedList()
	if maxList == nil {
		return nil, false, nil
	}

	for {
		chunk, err = pages.saveToStorage(maxList.ToReader(), maxList.Offset(), maxList.Size())
		if err == nil {
			hasSavedData = true
			glog.V(3).Infof("%s saveToStorage [%d,%d) %s", pages.f.fullpath(), maxList.Offset(), maxList.Offset()+maxList.Size(), chunk.FileId)
			return
		} else {
			glog.V(0).Infof("%s saveToStorage [%d,%d): %v", pages.f.fullpath(), maxList.Offset(), maxList.Offset()+maxList.Size(), err)
			time.Sleep(5 * time.Second)
		}
	}

}

func (pages *ContinuousDirtyPages) saveToStorage(reader io.Reader, offset int64, size int64) (*filer_pb.FileChunk, error) {

	var fileId, host string
	var auth security.EncodedJwt

	dir, _ := pages.f.fullpath().DirAndName()

	if err := pages.f.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: pages.f.wfs.option.Replication,
			Collection:  pages.f.wfs.option.Collection,
			TtlSec:      pages.f.wfs.option.TtlSec,
			DataCenter:  pages.f.wfs.option.DataCenter,
			ParentPath:  dir,
		}

		resp, err := client.AssignVolume(context.Background(), request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("assign volume failure %v: %v", request, resp.Error)
		}

		fileId, host, auth = resp.FileId, resp.Url, security.EncodedJwt(resp.Auth)
		host = pages.f.wfs.AdjustedUrl(host)
		pages.collection, pages.replication = resp.Collection, resp.Replication

		return nil
	}); err != nil {
		return nil, fmt.Errorf("filerGrpcAddress assign volume: %v", err)
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
	uploadResult, err, data := operation.Upload(fileUrl, pages.f.Name, pages.f.wfs.option.Cipher, reader, false, "", nil, auth)
	if err != nil {
		glog.V(0).Infof("upload data %v to %s: %v", pages.f.Name, fileUrl, err)
		return nil, fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v to %s: %v", pages.f.Name, fileUrl, err)
		return nil, fmt.Errorf("upload result: %v", uploadResult.Error)
	}
	pages.f.wfs.chunkCache.SetChunk(fileId, data)

	return uploadResult.ToPbFileChunk(fileId, offset), nil

}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func (pages *ContinuousDirtyPages) ReadDirtyData(data []byte, startOffset int64) (offset int64, size int) {

	pages.lock.Lock()
	defer pages.lock.Unlock()

	return pages.intervals.ReadData(data, startOffset)

}
