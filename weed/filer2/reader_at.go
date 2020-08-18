package filer2

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util/chunk_cache"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

type ChunkReadAt struct {
	masterClient *wdclient.MasterClient
	chunkViews   []*ChunkView
	lookupFileId func(fileId string) (targetUrl string, err error)
	readerLock   sync.Mutex
	fileSize     int64

	chunkCache chunk_cache.ChunkCache
}

// var _ = io.ReaderAt(&ChunkReadAt{})

type LookupFileIdFunctionType func(fileId string) (targetUrl string, err error)

func LookupFn(filerClient filer_pb.FilerClient) LookupFileIdFunctionType {
	return func(fileId string) (targetUrl string, err error) {
		err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
			vid := VolumeId(fileId)
			resp, err := client.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
				VolumeIds: []string{vid},
			})
			if err != nil {
				return err
			}

			locations := resp.LocationsMap[vid]
			if locations == nil || len(locations.Locations) == 0 {
				glog.V(0).Infof("failed to locate %s", fileId)
				return fmt.Errorf("failed to locate %s", fileId)
			}

			volumeServerAddress := filerClient.AdjustedUrl(locations.Locations[0].Url)

			targetUrl = fmt.Sprintf("http://%s/%s", volumeServerAddress, fileId)

			return nil
		})
		return
	}
}

func NewChunkReaderAtFromClient(filerClient filer_pb.FilerClient, chunkViews []*ChunkView, chunkCache chunk_cache.ChunkCache, fileSize int64) *ChunkReadAt {

	return &ChunkReadAt{
		chunkViews:   chunkViews,
		lookupFileId: LookupFn(filerClient),
		chunkCache:   chunkCache,
		fileSize:     fileSize,
	}
}

func (c *ChunkReadAt) ReadAt(p []byte, offset int64) (n int, err error) {

	c.readerLock.Lock()
	defer c.readerLock.Unlock()

	glog.V(4).Infof("ReadAt [%d,%d) of total file size %d bytes %d chunk views", offset, offset+int64(len(p)), c.fileSize, len(c.chunkViews))
	return c.doReadAt(p[n:], offset+int64(n))
}

func (c *ChunkReadAt) doReadAt(p []byte, offset int64) (n int, err error) {

	var buffer []byte
	startOffset, remaining := offset, int64(len(p))
	for i, chunk := range c.chunkViews {
		if remaining <= 0 {
			break
		}
		if startOffset < chunk.LogicOffset {
			gap := int(chunk.LogicOffset - startOffset)
			glog.V(4).Infof("zero [%d,%d)", startOffset, startOffset+int64(gap))
			n += int(min(int64(gap), remaining))
			startOffset, remaining = chunk.LogicOffset, remaining-int64(gap)
			if remaining <= 0 {
				break
			}
		}
		// fmt.Printf(">>> doReadAt [%d,%d), chunk[%d,%d)\n", offset, offset+int64(len(p)), chunk.LogicOffset, chunk.LogicOffset+int64(chunk.Size))
		chunkStart, chunkStop := max(chunk.LogicOffset, startOffset), min(chunk.LogicOffset+int64(chunk.Size), startOffset+remaining)
		if chunkStart >= chunkStop {
			continue
		}
		glog.V(4).Infof("read [%d,%d), %d/%d chunk %s [%d,%d)", chunkStart, chunkStop, i, len(c.chunkViews), chunk.FileId, chunk.LogicOffset-chunk.Offset, chunk.LogicOffset-chunk.Offset+int64(chunk.Size))
		buffer, err = c.readFromWholeChunkData(chunk)
		if err != nil {
			glog.Errorf("fetching chunk %+v: %v\n", chunk, err)
			return
		}
		bufferOffset := chunkStart - chunk.LogicOffset + chunk.Offset
		copied := copy(p[startOffset-offset:chunkStop-chunkStart+startOffset-offset], buffer[bufferOffset:bufferOffset+chunkStop-chunkStart])
		n += copied
		startOffset, remaining = startOffset+int64(copied), remaining-int64(copied)
	}

	glog.V(4).Infof("doReadAt [%d,%d), n:%v, err:%v", offset, offset+int64(len(p)), n, err)

	if remaining > 0 && c.fileSize > startOffset {
		delta := int(min(remaining, c.fileSize - startOffset))
		glog.V(4).Infof("zero2 [%d,%d)", n, n+delta)
		n += delta
	}

	if offset+int64(n) >= c.fileSize {
		err = io.EOF
	}
	// fmt.Printf("~~~ filled %d, err: %v\n\n", n, err)

	return

}

func (c *ChunkReadAt) readFromWholeChunkData(chunkView *ChunkView) (chunkData []byte, err error) {

	glog.V(4).Infof("readFromWholeChunkData %s offset %d [%d,%d) size at least %d", chunkView.FileId, chunkView.Offset, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size), chunkView.ChunkSize)

	chunkData = c.chunkCache.GetChunk(chunkView.FileId, chunkView.ChunkSize)
	if chunkData != nil {
		glog.V(5).Infof("cache hit %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset-chunkView.Offset, chunkView.LogicOffset-chunkView.Offset+int64(len(chunkData)))
	} else {
		glog.V(4).Infof("doFetchFullChunkData %s", chunkView.FileId)
		chunkData, err = c.doFetchFullChunkData(chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped)
		if err != nil {
			return
		}
		c.chunkCache.SetChunk(chunkView.FileId, chunkData)
	}

	return
}

func (c *ChunkReadAt) doFetchFullChunkData(fileId string, cipherKey []byte, isGzipped bool) ([]byte, error) {

	return fetchChunk(c.lookupFileId, fileId, cipherKey, isGzipped)

}
