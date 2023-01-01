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

func SeparateGarbageChunks(visibles *IntervalList[VisibleInterval], chunks []*filer_pb.FileChunk) (compacted []*filer_pb.FileChunk, garbage []*filer_pb.FileChunk) {
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
	Size          uint64
	LogicOffset   int64 // actual offset in the file, for the data specified via [offset, offset+size) in current chunk
	ChunkSize     uint64
	CipherKey     []byte
	IsGzipped     bool
	ModifiedTsNs  int64
}

func (cv *ChunkView) IsFullChunk() bool {
	return cv.Size == cv.ChunkSize
}

func ViewFromChunks(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, offset int64, size int64) (chunkViews *IntervalList[*ChunkView]) {

	visibles, _ := NonOverlappingVisibleIntervals(lookupFileIdFn, chunks, offset, offset+size)

	return ViewFromVisibleIntervals(visibles, offset, size)

}

func ViewFromVisibleIntervals(visibles *IntervalList[VisibleInterval], offset int64, size int64) (chunkViews *IntervalList[*ChunkView]) {

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
				Size:          uint64(chunkStop - chunkStart),
				LogicOffset:   chunkStart,
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

func logPrintf(name string, visibles []VisibleInterval) {

	/*
		glog.V(0).Infof("%s len %d", name, len(visibles))
		for _, v := range visibles {
			glog.V(0).Infof("%s:  [%d,%d) %s %d", name, v.start, v.stop, v.fileId, v.chunkOffset)
		}
	*/
}

func MergeIntoVisibles(visibles []VisibleInterval, chunk *filer_pb.FileChunk) (newVisibles []VisibleInterval) {

	newV := newVisibleInterval(chunk.Offset, chunk.Offset+int64(chunk.Size), chunk.GetFileIdString(), chunk.ModifiedTsNs, 0, chunk.Size, chunk.CipherKey, chunk.IsCompressed)

	length := len(visibles)
	if length == 0 {
		return append(visibles, newV)
	}
	last := visibles[length-1]
	if last.stop <= chunk.Offset {
		return append(visibles, newV)
	}

	logPrintf("  before", visibles)
	// glog.V(0).Infof("newVisibles %d adding chunk [%d,%d) %s size:%d", len(newVisibles), chunk.Offset, chunk.Offset+int64(chunk.Size), chunk.GetFileIdString(), chunk.Size)
	chunkStop := chunk.Offset + int64(chunk.Size)
	for _, v := range visibles {
		if v.start < chunk.Offset && chunk.Offset < v.stop {
			t := newVisibleInterval(v.start, chunk.Offset, v.fileId, v.modifiedTsNs, v.offsetInChunk, v.chunkSize, v.cipherKey, v.isGzipped)
			newVisibles = append(newVisibles, t)
			// glog.V(0).Infof("visible %d [%d,%d) =1> [%d,%d)", i, v.start, v.stop, t.start, t.stop)
		}
		if v.start < chunkStop && chunkStop < v.stop {
			t := newVisibleInterval(chunkStop, v.stop, v.fileId, v.modifiedTsNs, v.offsetInChunk+(chunkStop-v.start), v.chunkSize, v.cipherKey, v.isGzipped)
			newVisibles = append(newVisibles, t)
			// glog.V(0).Infof("visible %d [%d,%d) =2> [%d,%d)", i, v.start, v.stop, t.start, t.stop)
		}
		if chunkStop <= v.start || v.stop <= chunk.Offset {
			newVisibles = append(newVisibles, v)
			// glog.V(0).Infof("visible %d [%d,%d) =3> [%d,%d)", i, v.start, v.stop, v.start, v.stop)
		}
	}
	newVisibles = append(newVisibles, newV)

	logPrintf("  append", newVisibles)

	for i := len(newVisibles) - 1; i >= 0; i-- {
		if i > 0 && newV.start < newVisibles[i-1].start {
			newVisibles[i] = newVisibles[i-1]
		} else {
			newVisibles[i] = newV
			break
		}
	}
	logPrintf("  sorted", newVisibles)

	return newVisibles
}

// NonOverlappingVisibleIntervals translates the file chunk into VisibleInterval in memory
// If the file chunk content is a chunk manifest
func NonOverlappingVisibleIntervals(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, startOffset int64, stopOffset int64) (visibles *IntervalList[VisibleInterval], err error) {

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

func newVisibleInterval(start, stop int64, fileId string, modifiedTime int64, offsetInChunk int64, chunkSize uint64, cipherKey []byte, isGzipped bool) VisibleInterval {
	return VisibleInterval{
		start:         start,
		stop:          stop,
		fileId:        fileId,
		modifiedTsNs:  modifiedTime,
		offsetInChunk: offsetInChunk, // the starting position in the chunk
		chunkSize:     chunkSize,     // size of the chunk
		cipherKey:     cipherKey,
		isGzipped:     isGzipped,
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
