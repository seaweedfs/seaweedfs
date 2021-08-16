package filer

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

func HasData(entry *filer_pb.Entry) bool {

	if len(entry.Content) > 0 {
		return true
	}

	return len(entry.Chunks) > 0
}

func IsSameData(a, b *filer_pb.Entry) bool {

	if len(a.Content) > 0 || len(b.Content) > 0 {
		return bytes.Equal(a.Content, b.Content)
	}

	return isSameChunks(a.Chunks, b.Chunks)
}

func isSameChunks(a, b []*filer_pb.FileChunk) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if !proto.Equal(x, y) {
			return false
		}
	}
	return true
}

func NewFileReader(filerClient filer_pb.FilerClient, entry *filer_pb.Entry) io.Reader {
	if len(entry.Content) > 0 {
		return bytes.NewReader(entry.Content)
	}
	return NewChunkStreamReader(filerClient, entry.Chunks)
}

func StreamContent(masterClient wdclient.HasLookupFileIdFunction, writer io.Writer, chunks []*filer_pb.FileChunk, offset int64, size int64) error {

	glog.V(9).Infof("start to stream content for chunks: %+v\n", chunks)
	chunkViews := ViewFromChunks(masterClient.GetLookupFileIdFunction(), chunks, offset, size)

	fileId2Url := make(map[string][]string)

	for _, chunkView := range chunkViews {

		urlStrings, err := masterClient.GetLookupFileIdFunction()(chunkView.FileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return err
		} else if len(urlStrings) == 0 {
			glog.Errorf("operation LookupFileId %s failed, err: urls not found", chunkView.FileId)
			return fmt.Errorf("operation LookupFileId %s failed, err: urls not found", chunkView.FileId)
		}
		fileId2Url[chunkView.FileId] = urlStrings
	}

	for _, chunkView := range chunkViews {

		urlStrings := fileId2Url[chunkView.FileId]
		start := time.Now()
		err := retriedStreamFetchChunkData(writer, urlStrings, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.Offset, int(chunkView.Size))
		stats.FilerRequestHistogram.WithLabelValues("chunkDownload").Observe(time.Since(start).Seconds())
		if err != nil {
			stats.FilerRequestCounter.WithLabelValues("chunkDownloadError").Inc()
			return fmt.Errorf("read chunk: %v", err)
		}
		stats.FilerRequestCounter.WithLabelValues("chunkDownload").Inc()
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
	chunkViews         []*ChunkView
	totalSize          int64
	logicOffset        int64
	buffer             []byte
	bufferOffset       int64
	bufferPos          int
	nextChunkViewIndex int
	lookupFileId       wdclient.LookupFileIdFunctionType
}

var _ = io.ReadSeeker(&ChunkStreamReader{})
var _ = io.ReaderAt(&ChunkStreamReader{})

func doNewChunkStreamReader(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	chunkViews := ViewFromChunks(lookupFileIdFn, chunks, 0, math.MaxInt64)
	sort.Slice(chunkViews, func(i, j int) bool {
		return chunkViews[i].LogicOffset < chunkViews[j].LogicOffset
	})

	var totalSize int64
	for _, chunk := range chunkViews {
		totalSize += int64(chunk.Size)
	}

	return &ChunkStreamReader{
		chunkViews:   chunkViews,
		lookupFileId: lookupFileIdFn,
		totalSize:    totalSize,
	}
}

func NewChunkStreamReaderFromFiler(masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	lookupFileIdFn := func(fileId string) (targetUrl []string, err error) {
		return masterClient.LookupFileId(fileId)
	}

	return doNewChunkStreamReader(lookupFileIdFn, chunks)
}

func NewChunkStreamReader(filerClient filer_pb.FilerClient, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	lookupFileIdFn := LookupFn(filerClient)

	return doNewChunkStreamReader(lookupFileIdFn, chunks)
}

func (c *ChunkStreamReader) ReadAt(p []byte, off int64) (n int, err error) {
	if err = c.prepareBufferFor(off); err != nil {
		return
	}
	c.logicOffset = off
	return c.Read(p)
}

func (c *ChunkStreamReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		if c.isBufferEmpty() {
			if c.nextChunkViewIndex >= len(c.chunkViews) {
				return n, io.EOF
			}
			chunkView := c.chunkViews[c.nextChunkViewIndex]
			if err = c.fetchChunkToBuffer(chunkView); err != nil {
				return
			}
			c.nextChunkViewIndex++
		}
		t := copy(p[n:], c.buffer[c.bufferPos:])
		c.bufferPos += t
		n += t
		c.logicOffset += int64(t)
	}
	return
}

func (c *ChunkStreamReader) isBufferEmpty() bool {
	return len(c.buffer) <= c.bufferPos
}

func (c *ChunkStreamReader) Seek(offset int64, whence int) (int64, error) {

	var err error
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += c.logicOffset
	case io.SeekEnd:
		offset = c.totalSize + offset
	}
	if offset > c.totalSize {
		err = io.ErrUnexpectedEOF
	} else {
		c.logicOffset = offset
	}

	return offset, err

}

func (c *ChunkStreamReader) prepareBufferFor(offset int64) (err error) {
	// stay in the same chunk
	if !c.isBufferEmpty() {
		if c.bufferOffset <= offset && offset < c.bufferOffset+int64(len(c.buffer)) {
			c.bufferPos = int(offset - c.bufferOffset)
			return nil
		}
	}

	// need to seek to a different chunk
	currentChunkIndex := sort.Search(len(c.chunkViews), func(i int) bool {
		return c.chunkViews[i].LogicOffset <= offset
	})
	if currentChunkIndex == len(c.chunkViews) {
		return io.EOF
	}

	// positioning within the new chunk
	chunk := c.chunkViews[currentChunkIndex]
	if chunk.LogicOffset <= offset && offset < chunk.LogicOffset+int64(chunk.Size) {
		if c.isBufferEmpty() || c.bufferOffset != chunk.LogicOffset {
			if err = c.fetchChunkToBuffer(chunk); err != nil {
				return
			}
			c.nextChunkViewIndex = currentChunkIndex + 1
		}
		c.bufferPos = int(offset - c.bufferOffset)
	}
	return
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
		shouldRetry, err = util.ReadUrlAsStream(urlString, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.Offset, int(chunkView.Size), func(data []byte) {
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
