package filesys

import (
	"sync"
	"sort"
	"fmt"
	"bytes"
	"io"
	"time"
	"context"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

type DirtyPage struct {
	Offset int64
	Data   []byte
}

type ContinuousDirtyPages struct {
	sync.Mutex

	pages []*DirtyPage
	f     *File
}

func (pages *ContinuousDirtyPages) AddPage(ctx context.Context, offset int64, data []byte) (chunk *filer_pb.FileChunk, err error) {
	pages.Lock()
	defer pages.Unlock()

	isPerfectAppend := len(pages.pages) == 0
	if len(pages.pages) > 0 {
		lastPage := pages.pages[len(pages.pages)-1]
		if lastPage.Offset+int64(len(lastPage.Data)) == offset {
			// write continuous pages
			glog.V(3).Infof("%s/%s append [%d,%d)", pages.f.dir.Path, pages.f.Name, offset, offset+int64(len(data)))
			isPerfectAppend = true
		}
	}

	isPerfectReplace := false
	for _, page := range pages.pages {
		if page.Offset == offset && len(page.Data) == len(data) {
			// perfect replace
			glog.V(3).Infof("%s/%s replace [%d,%d)", pages.f.dir.Path, pages.f.Name, offset, offset+int64(len(data)))
			page.Data = data
			isPerfectReplace = true
		}
	}

	if isPerfectReplace {
		return nil, nil
	}

	if isPerfectAppend {
		pages.pages = append(pages.pages, &DirtyPage{
			Offset: offset,
			Data:   data,
		})

		if pages.f.wfs.chunkSizeLimit > 0 && pages.totalSize() >= pages.f.wfs.chunkSizeLimit {
			chunk, err = pages.saveToStorage(ctx)
			pages.pages = nil
			glog.V(3).Infof("%s/%s add split [%d,%d)", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
		return
	}

	chunk, err = pages.saveToStorage(ctx)

	glog.V(3).Infof("%s/%s saved [%d,%d)", pages.f.dir.Path, pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size))

	pages.pages = []*DirtyPage{&DirtyPage{
		Offset: offset,
		Data:   data,
	}}

	return
}

func (pages *ContinuousDirtyPages) FlushToStorage(ctx context.Context) (chunk *filer_pb.FileChunk, err error) {

	pages.Lock()
	defer pages.Unlock()

	if chunk, err = pages.saveToStorage(ctx); err == nil {
		pages.pages = nil
	}
	return
}

func (pages *ContinuousDirtyPages) totalSize() (total int64) {
	for _, page := range pages.pages {
		total += int64(len(page.Data))
	}
	return
}

func (pages *ContinuousDirtyPages) saveToStorage(ctx context.Context) (*filer_pb.FileChunk, error) {

	if len(pages.pages) == 0 {
		return nil, nil
	}

	sort.Slice(pages.pages, func(i, j int) bool {
		return pages.pages[i].Offset < pages.pages[j].Offset
	})

	var fileId, host string

	if err := pages.f.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: pages.f.wfs.replication,
			Collection:  pages.f.wfs.collection,
		}

		resp, err := client.AssignVolume(ctx, request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}

		fileId, host = resp.FileId, resp.Url

		return nil
	}); err != nil {
		return nil, fmt.Errorf("filer assign volume: %v", err)
	}

	var readers []io.Reader
	for _, page := range pages.pages {
		readers = append(readers, bytes.NewReader(page.Data))
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
	bufReader := io.MultiReader(readers...)
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
		Offset: pages.pages[0].Offset,
		Size:   uint64(pages.totalSize()),
		Mtime:  time.Now().UnixNano(),
	}, nil

}
