package filer

import (
	"golang.org/x/exp/slices"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func TestCompactFileChunksRealCase(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{FileId: "2,512f31f2c0700a", Offset: 0, Size: 25 - 0, Mtime: 5320497},
		{FileId: "6,512f2c2e24e9e8", Offset: 868352, Size: 917585 - 868352, Mtime: 5320492},
		{FileId: "7,514468dd5954ca", Offset: 884736, Size: 901120 - 884736, Mtime: 5325928},
		{FileId: "5,5144463173fe77", Offset: 917504, Size: 2297856 - 917504, Mtime: 5325894},
		{FileId: "4,51444c7ab54e2d", Offset: 2301952, Size: 2367488 - 2301952, Mtime: 5325900},
		{FileId: "4,514450e643ad22", Offset: 2371584, Size: 2420736 - 2371584, Mtime: 5325904},
		{FileId: "6,514456a5e9e4d7", Offset: 2449408, Size: 2490368 - 2449408, Mtime: 5325910},
		{FileId: "3,51444f8d53eebe", Offset: 2494464, Size: 2555904 - 2494464, Mtime: 5325903},
		{FileId: "4,5144578b097c7e", Offset: 2560000, Size: 2596864 - 2560000, Mtime: 5325911},
		{FileId: "3,51445500b6b4ac", Offset: 2637824, Size: 2678784 - 2637824, Mtime: 5325909},
		{FileId: "1,51446285e52a61", Offset: 2695168, Size: 2715648 - 2695168, Mtime: 5325922},
	}

	printChunks("before", chunks)

	compacted, garbage := CompactFileChunks(nil, chunks)

	printChunks("compacted", compacted)
	printChunks("garbage", garbage)

}

func printChunks(name string, chunks []*filer_pb.FileChunk) {
	slices.SortFunc(chunks, func(a, b *filer_pb.FileChunk) bool {
		if a.Offset == b.Offset {
			return a.Mtime < b.Mtime
		}
		return a.Offset < b.Offset
	})
	for _, chunk := range chunks {
		glog.V(0).Infof("%s chunk %s [%10d,%10d)", name, chunk.GetFileIdString(), chunk.Offset, chunk.Offset+int64(chunk.Size))
	}
}
