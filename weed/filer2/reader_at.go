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
	buffer       []byte
	bufferFileId string
	lookupFileId func(fileId string) (targetUrl string, err error)
	readerLock   sync.Mutex
	fileSize     int64

	chunkCache *chunk_cache.ChunkCache
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

func NewChunkReaderAtFromClient(filerClient filer_pb.FilerClient, chunkViews []*ChunkView, chunkCache *chunk_cache.ChunkCache, fileSize int64) *ChunkReadAt {

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

	for n < len(p) && err == nil {
		readCount, readErr := c.doReadAt(p[n:], offset+int64(n))
		n += readCount
		err = readErr
	}
	return
}

func (c *ChunkReadAt) doReadAt(p []byte, offset int64) (n int, err error) {

	var startOffset = offset
	for _, chunk := range c.chunkViews {
		if startOffset < min(chunk.LogicOffset, int64(len(p))+offset) {
			gap := int(min(chunk.LogicOffset, int64(len(p))+offset) - startOffset)
			glog.V(4).Infof("zero [%d,%d)", n, n+gap)
			n += gap
			startOffset = chunk.LogicOffset
		}
		// fmt.Printf(">>> doReadAt [%d,%d), chunk[%d,%d)\n", offset, offset+int64(len(p)), chunk.LogicOffset, chunk.LogicOffset+int64(chunk.Size))
		chunkStart, chunkStop := max(chunk.LogicOffset, offset), min(chunk.LogicOffset+int64(chunk.Size), offset+int64(len(p)))
		if chunkStart >= chunkStop {
			continue
		}
		glog.V(4).Infof("read [%d,%d), chunk %s [%d,%d)\n", chunkStart, chunkStop, chunk.FileId, chunk.LogicOffset-chunk.Offset, chunk.LogicOffset-chunk.Offset+int64(chunk.Size))
		c.buffer, err = c.fetchWholeChunkData(chunk)
		if err != nil {
			glog.Errorf("fetching chunk %+v: %v\n", chunk, err)
			return
		}
		c.bufferFileId = chunk.FileId
		bufferOffset := chunkStart - chunk.LogicOffset + chunk.Offset
		copied := copy(p[chunkStart-offset:chunkStop-offset], c.buffer[bufferOffset:bufferOffset+chunkStop-chunkStart])
		n += copied
		startOffset += int64(copied)
	}

	// fmt.Printf("> doReadAt [%d,%d), buffer %s:%d, found:%v, err:%v\n", offset, offset+int64(len(p)), c.bufferFileId, int64(len(c.buffer)), found, err)

	if startOffset < min(c.fileSize, int64(len(p))+offset) {
		gap := int(min(c.fileSize, int64(len(p))+offset) - startOffset)
		glog.V(4).Infof("zero2 [%d,%d)", n, n+gap)
		n += gap
	}
	if offset+int64(n) >= c.fileSize {
		err = io.EOF
	}
	// fmt.Printf("~~~ filled %d, err: %v\n\n", n, err)

	return

}

func (c *ChunkReadAt) fetchWholeChunkData(chunkView *ChunkView) (chunkData []byte, err error) {

	glog.V(4).Infof("fetchWholeChunkData %s offset %d [%d,%d)\n", chunkView.FileId, chunkView.Offset, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	chunkData = c.chunkCache.GetChunk(chunkView.FileId, chunkView.ChunkSize)
	if chunkData != nil {
		glog.V(5).Infof("cache hit %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset-chunkView.Offset, chunkView.LogicOffset-chunkView.Offset+int64(len(chunkData)))
	} else {
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
