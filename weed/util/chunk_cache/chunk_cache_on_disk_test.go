package chunk_cache

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func TestOnDisk(t *testing.T) {

	tmpDir, _ := ioutil.TempDir("", "c")
	defer os.RemoveAll(tmpDir)

	totalDiskSizeInKB := int64(32)

	cache := NewTieredChunkCache(2, tmpDir, totalDiskSizeInKB, 1024)

	writeCount := 5
	type test_data struct {
		data   []byte
		fileId string
		size   uint64
	}
	testData := make([]*test_data, writeCount)
	for i := 0; i < writeCount; i++ {
		buff := make([]byte, 1024)
		rand.Read(buff)
		testData[i] = &test_data{
			data:   buff,
			fileId: fmt.Sprintf("1,%daabbccdd", i+1),
			size:   uint64(len(buff)),
		}
		cache.SetChunk(testData[i].fileId, testData[i].data)

		// read back right after write
		data := cache.GetChunk(testData[i].fileId, testData[i].size)
		if bytes.Compare(data, testData[i].data) != 0 {
			t.Errorf("failed to write to and read from cache: %d", i)
		}
	}

	for i := 0; i < 2; i++ {
		data := cache.GetChunk(testData[i].fileId, testData[i].size)
		if bytes.Compare(data, testData[i].data) == 0 {
			t.Errorf("old cache should have been purged: %d", i)
		}
	}

	for i := 2; i < writeCount; i++ {
		data := cache.GetChunk(testData[i].fileId, testData[i].size)
		if bytes.Compare(data, testData[i].data) != 0 {
			t.Errorf("failed to write to and read from cache: %d", i)
		}
	}

	cache.Shutdown()

	cache = NewTieredChunkCache(2, tmpDir, totalDiskSizeInKB, 1024)

	for i := 0; i < 2; i++ {
		data := cache.GetChunk(testData[i].fileId, testData[i].size)
		if bytes.Compare(data, testData[i].data) == 0 {
			t.Errorf("old cache should have been purged: %d", i)
		}
	}

	for i := 2; i < writeCount; i++ {
		data := cache.GetChunk(testData[i].fileId, testData[i].size)
		if bytes.Compare(data, testData[i].data) != 0 {
			t.Errorf("failed to write to and read from cache: %d", i)
		}
	}

	cache.Shutdown()

}
