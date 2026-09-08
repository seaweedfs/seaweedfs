package filer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestChunkGroupReaderCacheMemory(t *testing.T) {
	for _, tt := range []struct {
		name      string
		chunkSize int
		chunks    int
		mode      string
	}{
		{"default chunks", 2 << 20, 40, "sequential"},
		{"pooled buffers", 3 << 20, 20, "sequential"},
		{"oversized chunk", 65 << 20, 1, "sequential"},
		{"concurrent readers", 2 << 20, 40, "concurrent"},
		{"prefetch", 2 << 20, 40, "prefetch"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			group, err := NewChunkGroup(func(context.Context, string) ([]string, error) {
				return []string{"unused"}, nil
			}, newMockChunkCacheForReaderCache(), nil, 128, nil)
			if err != nil {
				t.Fatal(err)
			}
			rc := group.readerCache
			defer rc.destroy()
			started := make(chan struct{}, tt.chunks)
			gate := make(chan struct{})
			if tt.mode == "sequential" {
				close(gate)
			}
			rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
				started <- struct{}{}
				<-gate
				buffer[0] = 42
				return len(buffer), nil
			}
			var readers sync.WaitGroup
			var views *Interval[*ChunkView]
			for i := 0; i < tt.chunks; i++ {
				read := func() {
					buffer := make([]byte, 1)
					n, err := rc.ReadChunkAt(context.Background(), buffer, fmt.Sprint(i), nil, false, 0, tt.chunkSize, false)
					if err != nil || n != 1 || buffer[0] != 42 {
						t.Errorf("read %d: n=%d, data=%v, err=%v", i, n, buffer, err)
					}
				}
				switch tt.mode {
				case "sequential":
					read()
				case "concurrent":
					readers.Add(1)
					go func() { defer readers.Done(); read() }()
				case "prefetch":
					views = &Interval[*ChunkView]{Value: &ChunkView{FileId: fmt.Sprint(i), ChunkSize: uint64(tt.chunkSize)}, Next: views}
				}
			}
			if tt.mode == "prefetch" {
				rc.MaybeCache(views, tt.chunks)
			}
			if tt.mode != "sequential" {
				for i := 0; i < tt.chunks; i++ {
					<-started
				}
				close(gate)
				readers.Wait()
			}

			deadline := time.Now().Add(time.Second)
			for {
				rc.Lock()
				retained := 0
				completed := true
				for _, downloader := range rc.downloaders {
					select {
					case <-downloader.done:
					default:
						completed = false
					}
					downloader.Lock()
					retained += cap(downloader.data)
					downloader.Unlock()
				}
				rc.Unlock()
				if completed && retained <= 64<<20 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("reader cache retained %d MiB, want at most 64 MiB", retained>>20)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}
