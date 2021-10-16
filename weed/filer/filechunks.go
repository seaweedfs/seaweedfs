package filer

import (
	"bytes"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"math"
)

func TotalSize(chunks []*filer_pb.FileChunk) (size uint64) {
	for _, c := range chunks {
		t := uint64(c.Offset + int64(c.Size))
		if size < t {
			size = t
		}
	}
	return
}

func FileSize(entry *filer_pb.Entry) (size uint64) {
	return maxUint64(TotalSize(entry.Chunks), entry.Attributes.FileSize)
}

func ETag(entry *filer_pb.Entry) (etag string) {
	if entry.Attributes == nil || entry.Attributes.Md5 == nil {
		return ETagChunks(entry.Chunks)
	}
	return fmt.Sprintf("%x", entry.Attributes.Md5)
}

func ETagEntry(entry *Entry) (etag string) {
	if entry.Attr.Md5 == nil {
		return ETagChunks(entry.Chunks)
	}
	return fmt.Sprintf("%x", entry.Attr.Md5)
}

func ETagChunks(chunks []*filer_pb.FileChunk) (etag string) {
	if len(chunks) == 1 {
		return fmt.Sprintf("%x", util.Base64Md5ToBytes(chunks[0].ETag))
	}
	md5_digests := [][]byte{}
	for _, c := range chunks {
		md5_digests = append(md5_digests, util.Base64Md5ToBytes(c.ETag))
	}
	return fmt.Sprintf("%x-%d", util.Md5(bytes.Join(md5_digests, nil)), len(chunks))
}

func CompactFileChunks(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk) (compacted, garbage []*filer_pb.FileChunk) {

	visibles, _ := NonOverlappingVisibleIntervals(lookupFileIdFn, chunks, 0, math.MaxInt64)

	fileIds := make(map[string]bool)
	for _, interval := range visibles {
		fileIds[interval.fileId] = true
	}
	for _, chunk := range chunks {
		if _, found := fileIds[chunk.GetFileIdString()]; found {
			compacted = append(compacted, chunk)
		} else {
			garbage = append(garbage, chunk)
		}
	}

	return
}

func MinusChunks(lookupFileIdFn wdclient.LookupFileIdFunctionType, as, bs []*filer_pb.FileChunk) (delta []*filer_pb.FileChunk, err error) {

	aData, aMeta, aErr := ResolveChunkManifest(lookupFileIdFn, as, 0, math.MaxInt64)
	if aErr != nil {
		return nil, aErr
	}
	bData, bMeta, bErr := ResolveChunkManifest(lookupFileIdFn, bs, 0, math.MaxInt64)
	if bErr != nil {
		return nil, bErr
	}

	delta = append(delta, DoMinusChunks(aData, bData)...)
	delta = append(delta, DoMinusChunks(aMeta, bMeta)...)
	return
}

func DoMinusChunks(as, bs []*filer_pb.FileChunk) (delta []*filer_pb.FileChunk) {

	fileIds := make(map[string]bool)
	for _, interval := range bs {
		fileIds[interval.GetFileIdString()] = true
	}
	for _, chunk := range as {
		if _, found := fileIds[chunk.GetFileIdString()]; !found {
			delta = append(delta, chunk)
		}
	}

	return
}

type ChunkView struct {
	FileId      string
	Offset      int64
	Size        uint64
	LogicOffset int64 // actual offset in the file, for the data specified via [offset, offset+size) in current chunk
	ChunkSize   uint64
	CipherKey   []byte
	IsGzipped   bool
}

func (cv *ChunkView) IsFullChunk() bool {
	return cv.Size == cv.ChunkSize
}

func ViewFromChunks(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, offset int64, size int64) (views []*ChunkView) {

	visibles, _ := NonOverlappingVisibleIntervals(lookupFileIdFn, chunks, offset, offset+size)

	return ViewFromVisibleIntervals(visibles, offset, size)

}

func ViewFromVisibleIntervals(visibles []VisibleInterval, offset int64, size int64) (views []*ChunkView) {

	stop := offset + size
	if size == math.MaxInt64 {
		stop = math.MaxInt64
	}
	if stop < offset {
		stop = math.MaxInt64
	}

	for _, chunk := range visibles {

		chunkStart, chunkStop := max(offset, chunk.start), min(stop, chunk.stop)

		if chunkStart < chunkStop {
			views = append(views, &ChunkView{
				FileId:      chunk.fileId,
				Offset:      chunkStart - chunk.start + chunk.chunkOffset,
				Size:        uint64(chunkStop - chunkStart),
				LogicOffset: chunkStart,
				ChunkSize:   chunk.chunkSize,
				CipherKey:   chunk.cipherKey,
				IsGzipped:   chunk.isGzipped,
			})
		}
	}

	return views

}

func logPrintf(name string, visibles []VisibleInterval) {

	/*
		glog.V(0).Infof("%s len %d", name, len(visibles))
		for _, v := range visibles {
			glog.V(0).Infof("%s:  [%d,%d) %s %d", name, v.start, v.stop, v.fileId, v.chunkOffset)
		}
	*/
}

// NonOverlappingVisibleIntervals translates the file chunk into VisibleInterval in memory
// If the file chunk content is a chunk manifest
func NonOverlappingVisibleIntervals(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, startOffset int64, stopOffset int64) (visibles []VisibleInterval, err error) {

	chunks, _, err = ResolveChunkManifest(lookupFileIdFn, chunks, startOffset, stopOffset)

	visibles = readResolvedChunks(chunks)

	return

}

// find non-overlapping visible intervals
// visible interval map to one file chunk

type VisibleInterval struct {
	start        int64
	stop         int64
	modifiedTime int64
	fileId       string
	chunkOffset  int64
	chunkSize    uint64
	cipherKey    []byte
	isGzipped    bool
}

func min(x, y int64) int64 {
	if x <= y {
		return x
	}
	return y
}
func max(x, y int64) int64 {
	if x <= y {
		return y
	}
	return x
}
