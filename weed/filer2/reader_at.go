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
	bufferOffset int64
	lookupFileId func(fileId string) (targetUrl string, err error)
	readerLock   sync.Mutex

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

func NewChunkReaderAtFromClient(filerClient filer_pb.FilerClient, chunkViews []*ChunkView, chunkCache *chunk_cache.ChunkCache) *ChunkReadAt {

	return &ChunkReadAt{
		chunkViews:   chunkViews,
		lookupFileId: LookupFn(filerClient),
		bufferOffset: -1,
		chunkCache:   chunkCache,
	}
}

func (c *ChunkReadAt) ReadAt(p []byte, offset int64) (n int, err error) {

	c.readerLock.Lock()
	defer c.readerLock.Unlock()

	for n < len(p) && err == nil {
		readCount, readErr := c.doReadAt(p[n:], offset+int64(n))
		n += readCount
		err = readErr
		if readCount == 0 {
			return n, io.EOF
		}
	}
	return
}

func (c *ChunkReadAt) doReadAt(p []byte, offset int64) (n int, err error) {

	var found bool
	for _, chunk := range c.chunkViews {
		if chunk.LogicOffset <= offset && offset < chunk.LogicOffset+int64(chunk.Size) {
			found = true
			if c.bufferOffset != chunk.LogicOffset {
				c.buffer, err = c.fetchChunkData(chunk)
				if err != nil {
					glog.Errorf("fetching chunk %+v: %v\n", chunk, err)
				}
				c.bufferOffset = chunk.LogicOffset
			}
			break
		}
	}
	if !found {
		return 0, io.EOF
	}

	if err == nil {
		n = copy(p, c.buffer[offset-c.bufferOffset:])
	}

	// fmt.Printf("> doReadAt [%d,%d), buffer:[%d,%d)\n", offset, offset+int64(n), c.bufferOffset, c.bufferOffset+int64(len(c.buffer)))

	return

}

func (c *ChunkReadAt) fetchChunkData(chunkView *ChunkView) (data []byte, err error) {

	glog.V(4).Infof("fetchChunkData %s [%d,%d)\n", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	hasDataInCache := false
	chunkData := c.chunkCache.GetChunk(chunkView.FileId, chunkView.ChunkSize)
	if chunkData != nil {
		glog.V(3).Infof("cache hit %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))
		hasDataInCache = true
	} else {
		chunkData, err = c.doFetchFullChunkData(chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped)
		if err != nil {
			return nil, err
		}
	}

	if int64(len(chunkData)) < chunkView.Offset+int64(chunkView.Size) {
		glog.Errorf("unexpected larger cached:%v chunk %s [%d,%d) than %d", hasDataInCache, chunkView.FileId, chunkView.Offset, chunkView.Offset+int64(chunkView.Size), len(chunkData))
		return nil, fmt.Errorf("unexpected larger cached:%v chunk %s [%d,%d) than %d", hasDataInCache, chunkView.FileId, chunkView.Offset, chunkView.Offset+int64(chunkView.Size), len(chunkData))
	}

	data = chunkData[chunkView.Offset : chunkView.Offset+int64(chunkView.Size)]

	if !hasDataInCache {
		c.chunkCache.SetChunk(chunkView.FileId, chunkData)
	}

	return data, nil
}

func (c *ChunkReadAt) doFetchFullChunkData(fileId string, cipherKey []byte, isGzipped bool) ([]byte, error) {

	return fetchChunk(c.lookupFileId, fileId, cipherKey, isGzipped)

}
