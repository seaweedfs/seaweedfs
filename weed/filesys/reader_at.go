package filesys

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/pb_cache"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

type ChunkReadAt struct {
	masterClient *wdclient.MasterClient
	chunkViews   []*filer2.ChunkView
	buffer       []byte
	bufferOffset int64
	lookupFileId func(fileId string) (targetUrl string, err error)
	readerLock   sync.Mutex

	chunkCache *pb_cache.ChunkCache
}

// var _ = io.ReaderAt(&ChunkReadAt{})

func NewChunkReaderAtFromClient(filerClient filer_pb.FilerClient, chunkViews []*filer2.ChunkView, chunkCache *pb_cache.ChunkCache) *ChunkReadAt {

	return &ChunkReadAt{
		chunkViews: chunkViews,
		lookupFileId: func(fileId string) (targetUrl string, err error) {
			err = filerClient.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
				vid := filer2.VolumeId(fileId)
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
		},
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
			return n, nil
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
				c.bufferOffset = chunk.LogicOffset
			}
			break
		}
	}
	if !found {
		return 0, io.EOF
	}

	n = copy(p, c.buffer[offset-c.bufferOffset:])

	// fmt.Printf("> doReadAt [%d,%d), buffer:[%d,%d)\n", offset, offset+int64(n), c.bufferOffset, c.bufferOffset+int64(len(c.buffer)))

	return

}

func (c *ChunkReadAt) fetchChunkData(chunkView *filer2.ChunkView) ([]byte, error) {

	// fmt.Printf("fetching %s [%d,%d)\n", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	chunkData := c.chunkCache.GetChunk(chunkView.FileId)
	if chunkData != nil {
		glog.V(3).Infof("cache hit %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))
		return chunkData, nil
	}

	urlString, err := c.lookupFileId(chunkView.FileId)
	if err != nil {
		glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
		return nil, err
	}
	var buffer bytes.Buffer
	err = util.ReadUrlAsStream(urlString, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk, chunkView.Offset, int(chunkView.Size), func(data []byte) {
		buffer.Write(data)
	})
	if err != nil {
		glog.V(1).Infof("read %s failed, err: %v", chunkView.FileId, err)
		return nil, err
	}

	glog.V(3).Infof("read %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	chunkData = buffer.Bytes()
	c.chunkCache.SetChunk(chunkView.FileId, chunkData)

	return chunkData, nil
}
