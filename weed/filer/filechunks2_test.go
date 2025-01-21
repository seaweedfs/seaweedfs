package filer

import (
	"github.com/stretchr/testify/assert"
	"log"
	"slices"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestDoMinusChunks(t *testing.T) {
	// https://github.com/seaweedfs/seaweedfs/issues/3328

	// clusterA and clusterB using filer.sync to sync file: hello.txt
	// clusterA append a new line and then clusterB also append a new line
	// clusterA append a new line again
	chunksInA := []*filer_pb.FileChunk{
		{Offset: 0, Size: 3, FileId: "11", ModifiedTsNs: 100},
		{Offset: 3, Size: 3, FileId: "22", SourceFileId: "2", ModifiedTsNs: 200},
		{Offset: 6, Size: 3, FileId: "33", ModifiedTsNs: 300},
	}
	chunksInB := []*filer_pb.FileChunk{
		{Offset: 0, Size: 3, FileId: "1", SourceFileId: "11", ModifiedTsNs: 100},
		{Offset: 3, Size: 3, FileId: "2", ModifiedTsNs: 200},
		{Offset: 6, Size: 3, FileId: "3", SourceFileId: "33", ModifiedTsNs: 300},
	}

	// clusterB using command "echo 'content' > hello.txt" to overwrite file
	// clusterA will receive two evenNotification, need to empty the whole file content first and add new content
	// the first one is oldEntry is chunksInB and newEntry is empty fileChunks
	firstOldEntry := chunksInB
	var firstNewEntry []*filer_pb.FileChunk

	// clusterA received the first one event, gonna empty the whole chunk, according the code in filer_sink 194
	// we can get the deleted chunks and newChunks
	firstDeletedChunks := DoMinusChunks(firstOldEntry, firstNewEntry)
	log.Println("first deleted chunks:", firstDeletedChunks)
	//firstNewEntry := DoMinusChunks(firstNewEntry, firstOldEntry)

	// clusterA need to delete all chunks in firstDeletedChunks
	emptiedChunksInA := DoMinusChunksBySourceFileId(chunksInA, firstDeletedChunks)
	// chunksInA supposed to be empty by minus the deletedChunks but it just delete the chunk which sync from clusterB
	log.Println("clusterA synced empty chunks event result:", emptiedChunksInA)
	// clusterB emptied it's chunks and clusterA must sync the change and empty chunks too
	assert.Equalf(t, firstNewEntry, emptiedChunksInA, "empty")
}

func TestCompactFileChunksRealCase(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{FileId: "2,512f31f2c0700a", Offset: 0, Size: 25 - 0, ModifiedTsNs: 5320497},
		{FileId: "6,512f2c2e24e9e8", Offset: 868352, Size: 917585 - 868352, ModifiedTsNs: 5320492},
		{FileId: "7,514468dd5954ca", Offset: 884736, Size: 901120 - 884736, ModifiedTsNs: 5325928},
		{FileId: "5,5144463173fe77", Offset: 917504, Size: 2297856 - 917504, ModifiedTsNs: 5325894},
		{FileId: "4,51444c7ab54e2d", Offset: 2301952, Size: 2367488 - 2301952, ModifiedTsNs: 5325900},
		{FileId: "4,514450e643ad22", Offset: 2371584, Size: 2420736 - 2371584, ModifiedTsNs: 5325904},
		{FileId: "6,514456a5e9e4d7", Offset: 2449408, Size: 2490368 - 2449408, ModifiedTsNs: 5325910},
		{FileId: "3,51444f8d53eebe", Offset: 2494464, Size: 2555904 - 2494464, ModifiedTsNs: 5325903},
		{FileId: "4,5144578b097c7e", Offset: 2560000, Size: 2596864 - 2560000, ModifiedTsNs: 5325911},
		{FileId: "3,51445500b6b4ac", Offset: 2637824, Size: 2678784 - 2637824, ModifiedTsNs: 5325909},
		{FileId: "1,51446285e52a61", Offset: 2695168, Size: 2715648 - 2695168, ModifiedTsNs: 5325922},
	}

	printChunks("before", chunks)

	compacted, garbage := CompactFileChunks(nil, chunks)

	printChunks("compacted", compacted)
	printChunks("garbage", garbage)

}

func printChunks(name string, chunks []*filer_pb.FileChunk) {
	slices.SortFunc(chunks, func(a, b *filer_pb.FileChunk) int {
		if a.Offset == b.Offset {
			return int(a.ModifiedTsNs - b.ModifiedTsNs)
		}
		return int(a.Offset - b.Offset)
	})
	for _, chunk := range chunks {
		glog.V(0).Infof("%s chunk %s [%10d,%10d)", name, chunk.GetFileIdString(), chunk.Offset, chunk.Offset+int64(chunk.Size))
	}
}
