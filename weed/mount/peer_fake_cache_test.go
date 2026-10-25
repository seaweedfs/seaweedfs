package mount

// fakeChunkCache satisfies chunk_cache.ChunkCache with a simple in-memory
// map keyed by fid. Just enough to exercise the peer-serve handler paths.
type fakeChunkCache struct {
	chunks map[string][]byte
}

func newFakeChunkCache() *fakeChunkCache {
	return &fakeChunkCache{chunks: map[string][]byte{}}
}

func (f *fakeChunkCache) Put(fid string, data []byte) {
	buf := make([]byte, len(data))
	copy(buf, data)
	f.chunks[fid] = buf
}

func (f *fakeChunkCache) ReadChunkAt(data []byte, fileId string, offset uint64) (int, error) {
	b, ok := f.chunks[fileId]
	if !ok {
		return 0, nil
	}
	if int(offset) >= len(b) {
		return 0, nil
	}
	return copy(data, b[offset:]), nil
}

func (f *fakeChunkCache) SetChunk(fileId string, data []byte) {
	f.Put(fileId, data)
}

func (f *fakeChunkCache) IsInCache(fileId string, lockNeeded bool) bool {
	_, ok := f.chunks[fileId]
	return ok
}

func (f *fakeChunkCache) GetMaxFilePartSizeInCache() uint64 {
	return 8 * 1024 * 1024
}
