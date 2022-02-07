package filer

import (
	"bytes"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"math"
	"sort"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
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

func DoMinusChunksBySourceFileId(as, bs []*filer_pb.FileChunk) (delta []*filer_pb.FileChunk) {

	fileIds := make(map[string]bool)
	for _, interval := range bs {
		fileIds[interval.GetFileIdString()] = true
	}
	for _, chunk := range as {
		if _, found := fileIds[chunk.GetSourceFileId()]; !found {
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

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(VisibleInterval)
	},
}

func MergeIntoVisibles(visibles []VisibleInterval, chunk *filer_pb.FileChunk) (newVisibles []VisibleInterval) {

	newV := newVisibleInterval(chunk.Offset, chunk.Offset+int64(chunk.Size), chunk.GetFileIdString(), chunk.Mtime, 0, chunk.Size, chunk.CipherKey, chunk.IsCompressed)

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
			t := newVisibleInterval(v.start, chunk.Offset, v.fileId, v.modifiedTime, v.chunkOffset, v.chunkSize, v.cipherKey, v.isGzipped)
			newVisibles = append(newVisibles, t)
			// glog.V(0).Infof("visible %d [%d,%d) =1> [%d,%d)", i, v.start, v.stop, t.start, t.stop)
		}
		if v.start < chunkStop && chunkStop < v.stop {
			t := newVisibleInterval(chunkStop, v.stop, v.fileId, v.modifiedTime, v.chunkOffset+(chunkStop-v.start), v.chunkSize, v.cipherKey, v.isGzipped)
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
func NonOverlappingVisibleIntervals(lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, startOffset int64, stopOffset int64) (visibles []VisibleInterval, err error) {

	chunks, _, err = ResolveChunkManifest(lookupFileIdFn, chunks, startOffset, stopOffset)

	visibles2 := readResolvedChunks(chunks)

	if true {
		return visibles2, err
	}

	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].Mtime == chunks[j].Mtime {
			filer_pb.EnsureFid(chunks[i])
			filer_pb.EnsureFid(chunks[j])
			if chunks[i].Fid == nil || chunks[j].Fid == nil {
				return true
			}
			return chunks[i].Fid.FileKey < chunks[j].Fid.FileKey
		}
		return chunks[i].Mtime < chunks[j].Mtime // keep this to make tests run
	})

	for _, chunk := range chunks {

		// glog.V(0).Infof("merge [%d,%d)", chunk.Offset, chunk.Offset+int64(chunk.Size))
		visibles = MergeIntoVisibles(visibles, chunk)

		logPrintf("add", visibles)

	}

	if len(visibles) != len(visibles2) {
		fmt.Printf("different visibles size %d : %d\n", len(visibles), len(visibles2))
	} else {
		for i := 0; i < len(visibles); i++ {
			checkDifference(visibles[i], visibles2[i])
		}
	}

	return
}

func checkDifference(x, y VisibleInterval) {
	if x.start != y.start ||
		x.stop != y.stop ||
		x.fileId != y.fileId ||
		x.modifiedTime != y.modifiedTime {
		fmt.Printf("different visible %+v : %+v\n", x, y)
	}
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

func newVisibleInterval(start, stop int64, fileId string, modifiedTime int64, chunkOffset int64, chunkSize uint64, cipherKey []byte, isGzipped bool) VisibleInterval {
	return VisibleInterval{
		start:        start,
		stop:         stop,
		fileId:       fileId,
		modifiedTime: modifiedTime,
		chunkOffset:  chunkOffset, // the starting position in the chunk
		chunkSize:    chunkSize,
		cipherKey:    cipherKey,
		isGzipped:    isGzipped,
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
