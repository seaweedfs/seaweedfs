package filer2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

func StreamContent(masterClient *wdclient.MasterClient, w io.Writer, chunks []*filer_pb.FileChunk, offset int64, size int64) error {

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
	buffer       []byte
	bufferOffset int64
	bufferPos    int
	chunkIndex   int
	lookupFileId func(fileId string) (targetUrl string, err error)
}

var _ = io.ReadSeeker(&ChunkStreamReader{})

func NewChunkStreamReaderFromFiler(masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	chunkViews := ViewFromChunks(chunks, 0, math.MaxInt64)

	return &ChunkStreamReader{
		chunkViews: chunkViews,
		lookupFileId: func(fileId string) (targetUrl string, err error) {
			return masterClient.LookupFileId(fileId)
		},
	}
}

func NewChunkStreamReaderFromClient(filerClient filer_pb.FilerClient, chunkViews []*ChunkView) *ChunkStreamReader {

	return &ChunkStreamReader{
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
	}
}

func (c *ChunkStreamReader) Read(p []byte) (n int, err error) {
	if c.isBufferEmpty() {
		if c.chunkIndex >= len(c.chunkViews) {
			return 0, io.EOF
		}
		chunkView := c.chunkViews[c.chunkIndex]
		println("fetch1")
		c.fetchChunkToBuffer(chunkView)
		c.chunkIndex++
	}
	n = copy(p, c.buffer[c.bufferPos:])
	c.bufferPos += n
	return
}

func (c *ChunkStreamReader) isBufferEmpty() bool {
	return len(c.buffer) <= c.bufferPos
}

func (c *ChunkStreamReader) Seek(offset int64, whence int) (int64, error) {

	var totalSize int64
	for _, chunk := range c.chunkViews {
		totalSize += int64(chunk.Size)
	}

	var err error
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += c.bufferOffset + int64(c.bufferPos)
	case io.SeekEnd:
		offset = totalSize + offset
	}
	if offset > totalSize {
		err = io.ErrUnexpectedEOF
	}

	for i, chunk := range c.chunkViews {
		if chunk.LogicOffset <= offset && offset < chunk.LogicOffset+int64(chunk.Size) {
			if c.isBufferEmpty() || c.bufferOffset != chunk.LogicOffset {
				c.fetchChunkToBuffer(chunk)
				c.chunkIndex = i + 1
				break
			}
		}
	}
	c.bufferPos = int(offset - c.bufferOffset)

	return offset, err

}

func (c *ChunkStreamReader) fetchChunkToBuffer(chunkView *ChunkView) error {
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
	c.bufferPos = 0
	c.bufferOffset = chunkView.LogicOffset

	// glog.V(0).Infof("read %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	return nil
}

func VolumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}

