package filer2

import "github.com/chrislusf/seaweedfs/weed/pb/filer_pb"

type Chunks []*filer_pb.FileChunk

func (chunks Chunks) TotalSize() (size uint64) {
	for _, c := range chunks {
		t := uint64(c.Offset + int64(c.Size))
		if size < t {
			size = t
		}
	}
	return
}

func (chunks Chunks) Len() int {
	return len(chunks)
}
func (chunks Chunks) Swap(i, j int) {
	chunks[i], chunks[j] = chunks[j], chunks[i]
}
func (chunks Chunks) Less(i, j int) bool {
	return chunks[i].Offset < chunks[j].Offset
}
