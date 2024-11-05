package filer

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type ChunkReadAt struct {
	masterClient  *wdclient.MasterClient
	chunkViews    *IntervalList[*ChunkView]
	fileSize      int64
	readerCache   *ReaderCache
	readerPattern *ReaderPattern
	lastChunkFid  string
}

var _ = io.ReaderAt(&ChunkReadAt{})
var _ = io.Closer(&ChunkReadAt{})

func LookupFn(filerClient filer_pb.FilerClient) wdclient.LookupFileIdFunctionType {

	vidCache := make(map[string]*filer_pb.Locations)
	var vicCacheLock sync.RWMutex
	return func(fileId string) (targetUrls []string, err error) {
		vid := VolumeId(fileId)
		vicCacheLock.RLock()
		locations, found := vidCache[vid]
		vicCacheLock.RUnlock()

		if !found {
			util.Retry("lookup volume "+vid, func() error {
				err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
					resp, err := client.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
						VolumeIds: []string{vid},
					})
					if err != nil {
						return err
					}

					locations = resp.LocationsMap[vid]
					if locations == nil || len(locations.Locations) == 0 {
						glog.V(0).Infof("failed to locate %s", fileId)
						return fmt.Errorf("failed to locate %s", fileId)
					}
					vicCacheLock.Lock()
					vidCache[vid] = locations
					vicCacheLock.Unlock()

					return nil
				})
				return err
			})
		}

		if err != nil {
			return nil, err
		}

		fcDataCenter := filerClient.GetDataCenter()
		var sameDcTargetUrls, otherTargetUrls []string
		for _, loc := range locations.Locations {
			volumeServerAddress := filerClient.AdjustedUrl(loc)
			targetUrl := fmt.Sprintf("http://%s/%s", volumeServerAddress, fileId)
			if fcDataCenter == "" || fcDataCenter != loc.DataCenter {
				otherTargetUrls = append(otherTargetUrls, targetUrl)
			} else {
				sameDcTargetUrls = append(sameDcTargetUrls, targetUrl)
			}
		}
		rand.Shuffle(len(sameDcTargetUrls), func(i, j int) {
			sameDcTargetUrls[i], sameDcTargetUrls[j] = sameDcTargetUrls[j], sameDcTargetUrls[i]
		})
		rand.Shuffle(len(otherTargetUrls), func(i, j int) {
			otherTargetUrls[i], otherTargetUrls[j] = otherTargetUrls[j], otherTargetUrls[i]
		})
		// Prefer same data center
		targetUrls = append(sameDcTargetUrls, otherTargetUrls...)
		return
	}
}

func NewChunkReaderAtFromClient(readerCache *ReaderCache, chunkViews *IntervalList[*ChunkView], fileSize int64) *ChunkReadAt {

	return &ChunkReadAt{
		chunkViews:    chunkViews,
		fileSize:      fileSize,
		readerCache:   readerCache,
		readerPattern: NewReaderPattern(),
	}
}

func (c *ChunkReadAt) Size() int64 {
	return c.fileSize
}

func (c *ChunkReadAt) Close() error {
	c.readerCache.destroy()
	return nil
}

func (c *ChunkReadAt) ReadAt(p []byte, offset int64) (n int, err error) {

	c.readerPattern.MonitorReadAt(offset, len(p))

	c.chunkViews.Lock.RLock()
	defer c.chunkViews.Lock.RUnlock()

	// glog.V(4).Infof("ReadAt [%d,%d) of total file size %d bytes %d chunk views", offset, offset+int64(len(p)), c.fileSize, len(c.chunkViews))
	n, _, err = c.doReadAt(p, offset)
	return
}

func (c *ChunkReadAt) ReadAtWithTime(p []byte, offset int64) (n int, ts int64, err error) {

	c.readerPattern.MonitorReadAt(offset, len(p))

	c.chunkViews.Lock.RLock()
	defer c.chunkViews.Lock.RUnlock()

	// glog.V(4).Infof("ReadAt [%d,%d) of total file size %d bytes %d chunk views", offset, offset+int64(len(p)), c.fileSize, len(c.chunkViews))
	return c.doReadAt(p, offset)
}

func (c *ChunkReadAt) doReadAt(p []byte, offset int64) (n int, ts int64, err error) {

	startOffset, remaining := offset, int64(len(p))
	var nextChunks *Interval[*ChunkView]
	for x := c.chunkViews.Front(); x != nil; x = x.Next {
		chunk := x.Value
		if remaining <= 0 {
			break
		}
		if x.Next != nil {
			nextChunks = x.Next
		}
		if startOffset < chunk.ViewOffset {
			gap := chunk.ViewOffset - startOffset
			glog.V(4).Infof("zero [%d,%d)", startOffset, chunk.ViewOffset)
			n += zero(p, startOffset-offset, gap)
			startOffset, remaining = chunk.ViewOffset, remaining-gap
			if remaining <= 0 {
				break
			}
		}
		// fmt.Printf(">>> doReadAt [%d,%d), chunk[%d,%d)\n", offset, offset+int64(len(p)), chunk.ViewOffset, chunk.ViewOffset+int64(chunk.ViewSize))
		chunkStart, chunkStop := max(chunk.ViewOffset, startOffset), min(chunk.ViewOffset+int64(chunk.ViewSize), startOffset+remaining)
		if chunkStart >= chunkStop {
			continue
		}
		// glog.V(4).Infof("read [%d,%d), %d/%d chunk %s [%d,%d)", chunkStart, chunkStop, i, len(c.chunkViews), chunk.FileId, chunk.ViewOffset-chunk.Offset, chunk.ViewOffset-chunk.Offset+int64(chunk.ViewSize))
		bufferOffset := chunkStart - chunk.ViewOffset + chunk.OffsetInChunk
		ts = chunk.ModifiedTsNs
		copied, err := c.readChunkSliceAt(p[startOffset-offset:chunkStop-chunkStart+startOffset-offset], chunk, nextChunks, uint64(bufferOffset))
		if err != nil {
			glog.Errorf("fetching chunk %+v: %v\n", chunk, err)
			return copied, ts, err
		}

		n += copied
		startOffset, remaining = startOffset+int64(copied), remaining-int64(copied)
	}

	// glog.V(4).Infof("doReadAt [%d,%d), n:%v, err:%v", offset, offset+int64(len(p)), n, err)

	// zero the remaining bytes if a gap exists at the end of the last chunk (or a fully sparse file)
	if err == nil && remaining > 0 {
		var delta int64
		if c.fileSize >= startOffset {
			delta = min(remaining, c.fileSize-startOffset)
			startOffset -= offset
		}
		if delta > 0 {
			glog.V(4).Infof("zero2 [%d,%d) of file size %d bytes", startOffset, startOffset+delta, c.fileSize)
			n += zero(p, startOffset, delta)
		}
	}

	if err == nil && offset+int64(len(p)) >= c.fileSize {
		err = io.EOF
	}
	// fmt.Printf("~~~ filled %d, err: %v\n\n", n, err)

	return

}

func (c *ChunkReadAt) readChunkSliceAt(buffer []byte, chunkView *ChunkView, nextChunkViews *Interval[*ChunkView], offset uint64) (n int, err error) {

	if c.readerPattern.IsRandomMode() {
		n, err := c.readerCache.chunkCache.ReadChunkAt(buffer, chunkView.FileId, offset)
		if n > 0 {
			return n, err
		}
		return fetchChunkRange(buffer, c.readerCache.lookupFileIdFn, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int64(offset))
	}

	shouldCache := (uint64(chunkView.ViewOffset) + chunkView.ChunkSize) <= c.readerCache.chunkCache.GetMaxFilePartSizeInCache()
	n, err = c.readerCache.ReadChunkAt(buffer, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int64(offset), int(chunkView.ChunkSize), shouldCache)
	if c.lastChunkFid != chunkView.FileId {
		if chunkView.OffsetInChunk == 0 { // start of a new chunk
			if c.lastChunkFid != "" {
				c.readerCache.UnCache(c.lastChunkFid)
			}
			if nextChunkViews != nil {
				c.readerCache.MaybeCache(nextChunkViews) // just read the next chunk if at the very beginning
			}
		}
	}
	c.lastChunkFid = chunkView.FileId
	return
}

func zero(buffer []byte, start, length int64) int {
	if length <= 0 {
		return 0
	}
	end := min(start+length, int64(len(buffer)))
	start = max(start, 0)

	// zero the bytes
	for o := start; o < end; o++ {
		buffer[o] = 0
	}
	return int(end - start)
}
