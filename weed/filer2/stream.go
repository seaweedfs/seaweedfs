package filer2

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

func StreamContent(masterClient *wdclient.MasterClient, w io.Writer, chunks []*filer_pb.FileChunk, offset int64, size int) error {

	chunkViews := ViewFromChunks(chunks, offset, size)

	fileId2Url := make(map[string]string)

	for _, chunkView := range chunkViews {

		urlString, err := masterClient.LookupFileId(chunkView.FileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return err
		}
		fileId2Url[chunkView.FileId] = urlString
	}

	for _, chunkView := range chunkViews {

		urlString := fileId2Url[chunkView.FileId]
		err := util.ReadUrlAsStream(urlString, chunkView.CipherKey, chunkView.isGzipped, chunkView.IsFullChunk, chunkView.Offset, int(chunkView.Size), func(data []byte) {
			w.Write(data)
		})
		if err != nil {
			glog.V(1).Infof("read %s failed, err: %v", chunkView.FileId, err)
			return err
		}
	}

	return nil

}

type ChunkStreamReader struct {
	masterClient *wdclient.MasterClient
	chunkViews   []*ChunkView
	logicOffset  int64
	buffer       bytes.Buffer
	bufferOffset int64
	chunkIndex   int
}

var _ = io.ReadSeeker(&ChunkStreamReader{})

func NewChunkStreamReader(masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	chunkViews := ViewFromChunks(chunks, 0, math.MaxInt32)

	return &ChunkStreamReader{
		masterClient: masterClient,
		chunkViews:   chunkViews,
		bufferOffset: -1,
	}
}

func (c *ChunkStreamReader) Read(p []byte) (n int, err error) {
	if c.buffer.Len() == 0 {
		if c.chunkIndex >= len(c.chunkViews) {
			return 0, io.EOF
		}
		chunkView := c.chunkViews[c.chunkIndex]
		c.fetchChunkToBuffer(chunkView)
		c.chunkIndex++
	}
	return c.buffer.Read(p)
}

func (c *ChunkStreamReader) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("ChunkStreamReader: seek not supported")
}

func (c *ChunkStreamReader) fetchChunkToBuffer(chunkView *ChunkView) error {
	urlString, err := c.masterClient.LookupFileId(chunkView.FileId)
	if err != nil {
		glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
		return err
	}
	c.buffer.Reset()
	err = util.ReadUrlAsStream(urlString, chunkView.CipherKey, chunkView.isGzipped, chunkView.IsFullChunk, chunkView.Offset, int(chunkView.Size), func(data []byte) {
		c.buffer.Write(data)
	})
	if err != nil {
		glog.V(1).Infof("read %s failed, err: %v", chunkView.FileId, err)
		return err
	}
	return nil
}
