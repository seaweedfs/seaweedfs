package filer

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

func StreamContent(masterClient wdclient.HasLookupFileIdFunction, w io.Writer, chunks []*filer_pb.FileChunk, offset int64, size int64) error {

	// fmt.Printf("start to stream content for chunks: %+v\n", chunks)
	chunkViews := ViewFromChunks(masterClient.GetLookupFileIdFunction(), chunks, offset, size)

	fileId2Url := make(map[string][]string)

	for _, chunkView := range chunkViews {

		urlStrings, err := masterClient.GetLookupFileIdFunction()(chunkView.FileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return err
		}
		fileId2Url[chunkView.FileId] = urlStrings
	}

	for _, chunkView := range chunkViews {

		urlStrings := fileId2Url[chunkView.FileId]

		data, err := retriedFetchChunkData(urlStrings, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.Offset, int(chunkView.Size))
		if err != nil {
			glog.Errorf("read chunk: %v", err)
			return fmt.Errorf("read chunk: %v", err)
		}
		_, err = w.Write(data)
		if err != nil {
			glog.Errorf("write chunk: %v", err)
			return fmt.Errorf("write chunk: %v", err)
		}
	}

	return nil

}

// ----------------  ReadAllReader ----------------------------------

func ReadAll(masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) ([]byte, error) {

	buffer := bytes.Buffer{}

	lookupFileIdFn := func(fileId string) (targetUrls []string, err error) {
		return masterClient.LookupFileId(fileId)
	}

	chunkViews := ViewFromChunks(lookupFileIdFn, chunks, 0, math.MaxInt64)

	for _, chunkView := range chunkViews {
		urlStrings, err := lookupFileIdFn(chunkView.FileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return nil, err
		}

		data, err := retriedFetchChunkData(urlStrings, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.Offset, int(chunkView.Size))
		if err != nil {
			return nil, err
		}
		buffer.Write(data)
	}
	return buffer.Bytes(), nil
}

// ----------------  ChunkStreamReader ----------------------------------
type ChunkStreamReader struct {
	chunkViews   []*ChunkView
	logicOffset  int64
	buffer       []byte
	bufferOffset int64
	bufferPos    int
	chunkIndex   int
	lookupFileId wdclient.LookupFileIdFunctionType
}

var _ = io.ReadSeeker(&ChunkStreamReader{})

func NewChunkStreamReaderFromFiler(masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	lookupFileIdFn := func(fileId string) (targetUrl []string, err error) {
		return masterClient.LookupFileId(fileId)
	}

	chunkViews := ViewFromChunks(lookupFileIdFn, chunks, 0, math.MaxInt64)

	return &ChunkStreamReader{
		chunkViews:   chunkViews,
		lookupFileId: lookupFileIdFn,
	}
}

func NewChunkStreamReader(filerClient filer_pb.FilerClient, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	lookupFileIdFn := LookupFn(filerClient)

	chunkViews := ViewFromChunks(lookupFileIdFn, chunks, 0, math.MaxInt64)

	return &ChunkStreamReader{
		chunkViews:   chunkViews,
		lookupFileId: lookupFileIdFn,
	}
}

func (c *ChunkStreamReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		if c.isBufferEmpty() {
			if c.chunkIndex >= len(c.chunkViews) {
				return n, io.EOF
			}
			chunkView := c.chunkViews[c.chunkIndex]
			c.fetchChunkToBuffer(chunkView)
			c.chunkIndex++
		}
		t := copy(p[n:], c.buffer[c.bufferPos:])
		c.bufferPos += t
		n += t
	}
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
	urlStrings, err := c.lookupFileId(chunkView.FileId)
	if err != nil {
		glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
		return err
	}
	var buffer bytes.Buffer
	var shouldRetry bool
	for _, urlString := range urlStrings {
		shouldRetry, err = util.ReadUrlAsStream(urlString+"?readDeleted=true", chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.Offset, int(chunkView.Size), func(data []byte) {
			buffer.Write(data)
		})
		if !shouldRetry {
			break
		}
		if err != nil {
			glog.V(1).Infof("read %s failed, err: %v", chunkView.FileId, err)
			buffer.Reset()
		} else {
			break
		}
	}
	if err != nil {
		return err
	}
	c.buffer = buffer.Bytes()
	c.bufferPos = 0
	c.bufferOffset = chunkView.LogicOffset

	// glog.V(0).Infof("read %s [%d,%d)", chunkView.FileId, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size))

	return nil
}

func (c *ChunkStreamReader) Close() {
	// TODO try to release and reuse buffer
}

func VolumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}
