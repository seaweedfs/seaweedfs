package filer2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

type ChunkReadAt struct {
	masterClient *wdclient.MasterClient
	chunkViews   []*ChunkView
	buffer       []byte
	bufferOffset int64
	lookupFileId func(fileId string) (targetUrl string, err error)
	readerLock   sync.Mutex
}

// var _ = io.ReaderAt(&ChunkReadAt{})

func NewChunkReaderAtFromClient(filerClient filer_pb.FilerClient, chunkViews   []*ChunkView) *ChunkReadAt {

	return &ChunkReadAt{
		chunkViews: chunkViews,
		lookupFileId: func(fileId string) (targetUrl string, err error) {
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
		},
		bufferOffset: -1,
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
				c.fetchChunkToBuffer(chunk)
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

func (c *ChunkReadAt) fetchChunkToBuffer(chunkView *ChunkView) error {

	// fmt.Printf("fetching %s [%d,%d)\n", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	urlString, err := c.lookupFileId(chunkView.FileId)
	if err != nil {
		glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
		return err
	}
	var buffer bytes.Buffer
	err = util.ReadUrlAsStream(urlString, chunkView.CipherKey, chunkView.isGzipped, chunkView.IsFullChunk, chunkView.Offset, int(chunkView.Size), func(data []byte) {
		buffer.Write(data)
	})
	if err != nil {
		glog.V(1).Infof("read %s failed, err: %v", chunkView.FileId, err)
		return err
	}
	c.buffer = buffer.Bytes()
	c.bufferOffset = chunkView.LogicOffset

	glog.V(3).Infof("read %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	return nil
}
