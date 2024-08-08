package filer

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var getLookupFileIdBackoffSchedule = []time.Duration{
	150 * time.Millisecond,
	600 * time.Millisecond,
	1800 * time.Millisecond,
}

func HasData(entry *filer_pb.Entry) bool {

	if len(entry.Content) > 0 {
		return true
	}

	return len(entry.GetChunks()) > 0
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
	slices.SortFunc(a, func(i, j *filer_pb.FileChunk) int {
		return strings.Compare(i.ETag, j.ETag)
	})
	slices.SortFunc(b, func(i, j *filer_pb.FileChunk) int {
		return strings.Compare(i.ETag, j.ETag)
	})
	for i := 0; i < len(a); i++ {
		if a[i].ETag != b[i].ETag {
			return false
		}
	}
	return true
}

func NewFileReader(filerClient filer_pb.FilerClient, entry *filer_pb.Entry) io.Reader {
	if len(entry.Content) > 0 {
		return bytes.NewReader(entry.Content)
	}
	return NewChunkStreamReader(filerClient, entry.GetChunks())
}

type DoStreamContent func(writer io.Writer) error

func PrepareStreamContent(masterClient wdclient.HasLookupFileIdFunction, jwtFunc VolumeServerJwtFunction, chunks []*filer_pb.FileChunk, offset int64, size int64) (DoStreamContent, error) {
	return PrepareStreamContentWithThrottler(masterClient, jwtFunc, chunks, offset, size, 0)
}

type VolumeServerJwtFunction func(fileId string) string

func noJwtFunc(string) string {
	return ""
}

func PrepareStreamContentWithThrottler(masterClient wdclient.HasLookupFileIdFunction, jwtFunc VolumeServerJwtFunction, chunks []*filer_pb.FileChunk, offset int64, size int64, downloadMaxBytesPs int64) (DoStreamContent, error) {
	glog.V(4).Infof("prepare to stream content for chunks: %d", len(chunks))
	chunkViews := ViewFromChunks(masterClient.GetLookupFileIdFunction(), chunks, offset, size)

	fileId2Url := make(map[string][]string)

	for x := chunkViews.Front(); x != nil; x = x.Next {
		chunkView := x.Value
		var urlStrings []string
		var err error
		for _, backoff := range getLookupFileIdBackoffSchedule {
			urlStrings, err = masterClient.GetLookupFileIdFunction()(chunkView.FileId)
			if err == nil && len(urlStrings) > 0 {
				break
			}
			glog.V(4).Infof("waiting for chunk: %s", chunkView.FileId)
			time.Sleep(backoff)
		}
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return nil, err
		} else if len(urlStrings) == 0 {
			errUrlNotFound := fmt.Errorf("operation LookupFileId %s failed, err: urls not found", chunkView.FileId)
			glog.Error(errUrlNotFound)
			return nil, errUrlNotFound
		}
		fileId2Url[chunkView.FileId] = urlStrings
	}

	return func(writer io.Writer) error {
		downloadThrottler := util.NewWriteThrottler(downloadMaxBytesPs)
		remaining := size
		for x := chunkViews.Front(); x != nil; x = x.Next {
			chunkView := x.Value
			if offset < chunkView.ViewOffset {
				gap := chunkView.ViewOffset - offset
				remaining -= gap
				glog.V(4).Infof("zero [%d,%d)", offset, chunkView.ViewOffset)
				err := writeZero(writer, gap)
				if err != nil {
					return fmt.Errorf("write zero [%d,%d)", offset, chunkView.ViewOffset)
				}
				offset = chunkView.ViewOffset
			}
			urlStrings := fileId2Url[chunkView.FileId]
			start := time.Now()
			jwt := jwtFunc(chunkView.FileId)
			err := retriedStreamFetchChunkData(writer, urlStrings, jwt, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.OffsetInChunk, int(chunkView.ViewSize))
			offset += int64(chunkView.ViewSize)
			remaining -= int64(chunkView.ViewSize)
			stats.FilerRequestHistogram.WithLabelValues("chunkDownload").Observe(time.Since(start).Seconds())
			if err != nil {
				stats.FilerHandlerCounter.WithLabelValues("chunkDownloadError").Inc()
				return fmt.Errorf("read chunk: %v", err)
			}
			stats.FilerHandlerCounter.WithLabelValues("chunkDownload").Inc()
			downloadThrottler.MaybeSlowdown(int64(chunkView.ViewSize))
		}
		if remaining > 0 {
			glog.V(4).Infof("zero [%d,%d)", offset, offset+remaining)
			err := writeZero(writer, remaining)
			if err != nil {
				return fmt.Errorf("write zero [%d,%d)", offset, offset+remaining)
			}
		}

		return nil
	}, nil
}

func StreamContent(masterClient wdclient.HasLookupFileIdFunction, writer io.Writer, chunks []*filer_pb.FileChunk, offset int64, size int64) error {
	streamFn, err := PrepareStreamContent(masterClient, noJwtFunc, chunks, offset, size)
	if err != nil {
		return err
	}
	return streamFn(writer)
}

// ----------------  ReadAllReader ----------------------------------

func writeZero(w io.Writer, size int64) (err error) {
	zeroPadding := make([]byte, 1024)
	var written int
	for size > 0 {
		if size > 1024 {
			written, err = w.Write(zeroPadding)
		} else {
			written, err = w.Write(zeroPadding[:size])
		}
		size -= int64(written)
		if err != nil {
			return
		}
	}
	return
}

func ReadAll(buffer []byte, masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) error {

	lookupFileIdFn := func(fileId string) (targetUrls []string, err error) {
		return masterClient.LookupFileId(fileId)
	}

	chunkViews := ViewFromChunks(lookupFileIdFn, chunks, 0, int64(len(buffer)))

	idx := 0

	for x := chunkViews.Front(); x != nil; x = x.Next {
		chunkView := x.Value
		urlStrings, err := lookupFileIdFn(chunkView.FileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return err
		}

		n, err := util_http.RetriedFetchChunkData(buffer[idx:idx+int(chunkView.ViewSize)], urlStrings, chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.OffsetInChunk)
		if err != nil {
			return err
		}
		idx += n
	}
	return nil
}

// ----------------  ChunkStreamReader ----------------------------------
type ChunkStreamReader struct {
	head         *Interval[*ChunkView]
	chunkView    *Interval[*ChunkView]
	totalSize    int64
	logicOffset  int64
	buffer       []byte
	bufferOffset int64
	bufferLock   sync.Mutex
	chunk        string
	lookupFileId wdclient.LookupFileIdFunctionType
}

var _ = io.ReadSeeker(&ChunkStreamReader{})
var _ = io.ReaderAt(&ChunkStreamReader{})

func doNewChunkStreamReader(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk) *ChunkStreamReader {

	chunkViews := ViewFromChunks(lookupFileIdFn, chunks, 0, math.MaxInt64)

	var totalSize int64
	for x := chunkViews.Front(); x != nil; x = x.Next {
		chunk := x.Value
		totalSize += int64(chunk.ViewSize)
	}

	return &ChunkStreamReader{
		head:         chunkViews.Front(),
		chunkView:    chunkViews.Front(),
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
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	if err = c.prepareBufferFor(off); err != nil {
		return
	}
	c.logicOffset = off
	return c.doRead(p)
}

func (c *ChunkStreamReader) Read(p []byte) (n int, err error) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	return c.doRead(p)
}

func (c *ChunkStreamReader) doRead(p []byte) (n int, err error) {
	// fmt.Printf("do read [%d,%d) at %s[%d,%d)\n", c.logicOffset, c.logicOffset+int64(len(p)), c.chunk, c.bufferOffset, c.bufferOffset+int64(len(c.buffer)))
	for n < len(p) {
		// println("read", c.logicOffset)
		if err = c.prepareBufferFor(c.logicOffset); err != nil {
			return
		}
		t := copy(p[n:], c.buffer[c.logicOffset-c.bufferOffset:])
		n += t
		c.logicOffset += int64(t)
	}
	return
}

func (c *ChunkStreamReader) isBufferEmpty() bool {
	return len(c.buffer) <= int(c.logicOffset-c.bufferOffset)
}

func (c *ChunkStreamReader) Seek(offset int64, whence int) (int64, error) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()

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

func insideChunk(offset int64, chunk *ChunkView) bool {
	return chunk.ViewOffset <= offset && offset < chunk.ViewOffset+int64(chunk.ViewSize)
}

func (c *ChunkStreamReader) prepareBufferFor(offset int64) (err error) {
	// stay in the same chunk
	if c.bufferOffset <= offset && offset < c.bufferOffset+int64(len(c.buffer)) {
		return nil
	}
	// glog.V(2).Infof("c.chunkView: %v buffer:[%d,%d) offset:%d totalSize:%d", c.chunkView, c.bufferOffset, c.bufferOffset+int64(len(c.buffer)), offset, c.totalSize)

	// find a possible chunk view
	p := c.chunkView
	for p != nil {
		chunk := p.Value
		// glog.V(2).Infof("prepareBufferFor check chunk:[%d,%d)", chunk.ViewOffset, chunk.ViewOffset+int64(chunk.ViewSize))
		if insideChunk(offset, chunk) {
			if c.isBufferEmpty() || c.bufferOffset != chunk.ViewOffset {
				c.chunkView = p
				return c.fetchChunkToBuffer(chunk)
			}
		}
		if offset < c.bufferOffset {
			p = p.Prev
		} else {
			p = p.Next
		}
	}

	return io.EOF
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
		shouldRetry, err = util_http.ReadUrlAsStream(urlString+"?readDeleted=true", chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(), chunkView.OffsetInChunk, int(chunkView.ViewSize), func(data []byte) {
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
	c.bufferOffset = chunkView.ViewOffset
	c.chunk = chunkView.FileId

	// glog.V(0).Infof("fetched %s [%d,%d)", chunkView.FileId, chunkView.ViewOffset, chunkView.ViewOffset+int64(chunkView.ViewSize))

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
