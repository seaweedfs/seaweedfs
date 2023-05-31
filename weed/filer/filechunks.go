package filer

import (
	"bytes"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	if entry == nil || entry.Attributes == nil {
		return 0
	}
	fileSize := entry.Attributes.FileSize
	if entry.RemoteEntry != nil {
		if entry.RemoteEntry.RemoteMtime > entry.Attributes.Mtime {
			fileSize = maxUint64(fileSize, uint64(entry.RemoteEntry.RemoteSize))
		}
	}
	return maxUint64(TotalSize(entry.GetChunks()), fileSize)
}

func ETag(entry *filer_pb.Entry) (etag string) {
	if entry.Attributes == nil || entry.Attributes.Md5 == nil {
		return ETagChunks(entry.GetChunks())
	}
	return fmt.Sprintf("%x", entry.Attributes.Md5)
}

func ETagEntry(entry *Entry) (etag string) {
	if entry.IsInRemoteOnly() {
		return entry.Remote.RemoteETag
	}
	if entry.Attr.Md5 == nil {
		return ETagChunks(entry.GetChunks())
	}
	return fmt.Sprintf("%x", entry.Attr.Md5)
}

func ETagChunks(chunks []*filer_pb.FileChunk) (etag string) {
	if len(chunks) == 1 {
		return fmt.Sprintf("%x", util.Base64Md5ToBytes(chunks[0].ETag))
	}
	var md5Digests [][]byte
	for _, c := range chunks {
		md5Digests = append(md5Digests, util.Base64Md5ToBytes(c.ETag))
	}
	return fmt.Sprintf("%x-%d", util.Md5(bytes.Join(md5Digests, nil)), len(chunks))
}

func CompactFileChunks(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk) (compacted, garbage []*filer_pb.FileChunk) {

	visibles, _ := NonOverlappingVisibleIntervals(lookupFileIdFn, chunks, 0, math.MaxInt64)

	compacted, garbage = SeparateGarbageChunks(visibles, chunks)

	return
}

func SeparateGarbageChunks(visibles *IntervalList[*VisibleInterval], chunks []*filer_pb.FileChunk) (compacted []*filer_pb.FileChunk, garbage []*filer_pb.FileChunk) {
	fileIds := make(map[string]bool)
	for x := visibles.Front(); x != nil; x = x.Next {
		interval := x.Value
		fileIds[interval.fileId] = true
	}
	for _, chunk := range chunks {
		if _, found := fileIds[chunk.GetFileIdString()]; found {
			compacted = append(compacted, chunk)
		} else {
			garbage = append(garbage, chunk)
		}
	}
	return compacted, garbage
}

func FindGarbageChunks(visibles *IntervalList[*VisibleInterval], start int64, stop int64) (garbageFileIds map[string]struct{}) {
	garbageFileIds = make(map[string]struct{})
	for x := visibles.Front(); x != nil; x = x.Next {
		interval := x.Value
		offset := interval.start - interval.offsetInChunk
		if start <= offset && offset+int64(interval.chunkSize) <= stop {
			garbageFileIds[interval.fileId] = struct{}{}
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

func DoMinusChunksBySourceFileId(as, bs []*filer_pb.FileChunk) (delta []*filer_pb.FileChunk) {

	fileIds := make(map[string]bool)
	for _, interval := range bs {
		fileIds[interval.GetFileIdString()] = true
		fileIds[interval.GetSourceFileId()] = true
	}
	for _, chunk := range as {
		_, sourceFileIdFound := fileIds[chunk.GetSourceFileId()]
		_, fileIdFound := fileIds[chunk.GetFileId()]
		if !sourceFileIdFound && !fileIdFound {
			delta = append(delta, chunk)
		}
	}

	return
}

type ChunkView struct {
	FileId        string
	OffsetInChunk int64 // offset within the chunk
	ViewSize      uint64
	ViewOffset    int64 // actual offset in the file, for the data specified via [offset, offset+size) in current chunk
	ChunkSize     uint64
	CipherKey     []byte
	IsGzipped     bool
	ModifiedTsNs  int64
}

func (cv *ChunkView) SetStartStop(start, stop int64) {
	cv.OffsetInChunk += start - cv.ViewOffset
	cv.ViewOffset = start
	cv.ViewSize = uint64(stop - start)
}
func (cv *ChunkView) Clone() IntervalValue {
	return &ChunkView{
		FileId:        cv.FileId,
		OffsetInChunk: cv.OffsetInChunk,
		ViewSize:      cv.ViewSize,
		ViewOffset:    cv.ViewOffset,
		ChunkSize:     cv.ChunkSize,
		CipherKey:     cv.CipherKey,
		IsGzipped:     cv.IsGzipped,
		ModifiedTsNs:  cv.ModifiedTsNs,
	}
}

func (cv *ChunkView) IsFullChunk() bool {
	return cv.ViewSize == cv.ChunkSize
}

func ViewFromChunks(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, offset int64, size int64) (chunkViews *IntervalList[*ChunkView]) {

	visibles, _ := NonOverlappingVisibleIntervals(lookupFileIdFn, chunks, offset, offset+size)

	return ViewFromVisibleIntervals(visibles, offset, size)

}

func ViewFromVisibleIntervals(visibles *IntervalList[*VisibleInterval], offset int64, size int64) (chunkViews *IntervalList[*ChunkView]) {

	stop := offset + size
	if size == math.MaxInt64 {
		stop = math.MaxInt64
	}
	if stop < offset {
		stop = math.MaxInt64
	}

	chunkViews = NewIntervalList[*ChunkView]()
	for x := visibles.Front(); x != nil; x = x.Next {
		chunk := x.Value

		chunkStart, chunkStop := max(offset, chunk.start), min(stop, chunk.stop)

		if chunkStart < chunkStop {
			chunkView := &ChunkView{
				FileId:        chunk.fileId,
				OffsetInChunk: chunkStart - chunk.start + chunk.offsetInChunk,
				ViewSize:      uint64(chunkStop - chunkStart),
				ViewOffset:    chunkStart,
				ChunkSize:     chunk.chunkSize,
				CipherKey:     chunk.cipherKey,
				IsGzipped:     chunk.isGzipped,
				ModifiedTsNs:  chunk.modifiedTsNs,
			}
			chunkViews.AppendInterval(&Interval[*ChunkView]{
				StartOffset: chunkStart,
				StopOffset:  chunkStop,
				TsNs:        chunk.modifiedTsNs,
				Value:       chunkView,
				Prev:        nil,
				Next:        nil,
			})
		}
	}

	return chunkViews

}

func MergeIntoVisibles(visibles *IntervalList[*VisibleInterval], start int64, stop int64, chunk *filer_pb.FileChunk) {

	newV := &VisibleInterval{
		start:         start,
		stop:          stop,
		fileId:        chunk.GetFileIdString(),
		modifiedTsNs:  chunk.ModifiedTsNs,
		offsetInChunk: start - chunk.Offset, // the starting position in the chunk
		chunkSize:     chunk.Size,           // size of the chunk
		cipherKey:     chunk.CipherKey,
		isGzipped:     chunk.IsCompressed,
	}

	visibles.InsertInterval(start, stop, chunk.ModifiedTsNs, newV)
}

func MergeIntoChunkViews(chunkViews *IntervalList[*ChunkView], start int64, stop int64, chunk *filer_pb.FileChunk) {

	chunkView := &ChunkView{
		FileId:        chunk.GetFileIdString(),
		OffsetInChunk: start - chunk.Offset,
		ViewSize:      uint64(stop - start),
		ViewOffset:    start,
		ChunkSize:     chunk.Size,
		CipherKey:     chunk.CipherKey,
		IsGzipped:     chunk.IsCompressed,
		ModifiedTsNs:  chunk.ModifiedTsNs,
	}

	chunkViews.InsertInterval(start, stop, chunk.ModifiedTsNs, chunkView)
}

// NonOverlappingVisibleIntervals translates the file chunk into VisibleInterval in memory
// If the file chunk content is a chunk manifest
func NonOverlappingVisibleIntervals(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, startOffset int64, stopOffset int64) (visibles *IntervalList[*VisibleInterval], err error) {

	chunks, _, err = ResolveChunkManifest(lookupFileIdFn, chunks, startOffset, stopOffset)
	if err != nil {
		return
	}

	visibles2 := readResolvedChunks(chunks, 0, math.MaxInt64)

	return visibles2, err
}

// find non-overlapping visible intervals
// visible interval map to one file chunk

type VisibleInterval struct {
	start         int64
	stop          int64
	modifiedTsNs  int64
	fileId        string
	offsetInChunk int64
	chunkSize     uint64
	cipherKey     []byte
	isGzipped     bool
}

func (v *VisibleInterval) SetStartStop(start, stop int64) {
	v.offsetInChunk += start - v.start
	v.start, v.stop = start, stop
}
func (v *VisibleInterval) Clone() IntervalValue {
	return &VisibleInterval{
		start:         v.start,
		stop:          v.stop,
		modifiedTsNs:  v.modifiedTsNs,
		fileId:        v.fileId,
		offsetInChunk: v.offsetInChunk,
		chunkSize:     v.chunkSize,
		cipherKey:     v.cipherKey,
		isGzipped:     v.isGzipped,
	}
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
