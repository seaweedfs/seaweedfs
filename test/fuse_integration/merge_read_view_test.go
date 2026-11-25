//go:build linux

package fuse_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	mergePageSize = 8192
	mergePages    = 32
)

// mergeHTTP bounds every HTTP call the test makes to the cluster.
var mergeHTTP = &http.Client{Timeout: 30 * time.Second}

// mergeVacuumHTTP outlasts a master vacuum, which waits up to 30 s per volume
// for writes assigned to it to drain before compacting it.
var mergeVacuumHTTP = &http.Client{Timeout: 3 * time.Minute}

// TestReadOpenHandleAfterChunkMerge covers a file a flush merged while a
// descriptor kept it open. The merge rewrites the file into new chunks and the
// filer deletes the chunks it replaced, so after a vacuum only the new chunks
// exist. Reads through the open handle must find them at once, whether or not
// the handle has a write it has not flushed yet.
func TestReadOpenHandleAfterChunkMerge(t *testing.T) {
	config := DefaultTestConfig()
	// The client chunk cache could still hold a replaced chunk; the reads
	// below have to reach the volume server.
	config.CacheSizeMB = 0
	config.MountOptions = append(config.MountOptions, "-cacheCapacityMB=0")
	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()
	require.NoError(t, framework.Setup(config))

	t.Run("CleanHandle", func(t *testing.T) {
		testReadOpenHandleAfterChunkMerge(t, framework, "merged_clean.dat", false)
	})
	t.Run("HandleWithUnflushedWrite", func(t *testing.T) {
		testReadOpenHandleAfterChunkMerge(t, framework, "merged_dirty.dat", true)
	})
}

func testReadOpenHandleAfterChunkMerge(t *testing.T, framework *FuseTestFramework, name string, unflushedWrite bool) {
	path := filepath.Join(framework.GetMountPoint(), name)
	held, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer held.Close()

	version := make([]int, mergePages)
	whole := make([]byte, 0, mergePages*mergePageSize)
	for p := 0; p < mergePages; p++ {
		whole = append(whole, mergePagePattern(p, 0)...)
	}
	_, err = held.WriteAt(whole, 0)
	require.NoError(t, err)
	require.NoError(t, held.Sync())

	// Overlapping 16-page windows: each leaves part of every older window
	// visible, so the stored chunks soon add up to more than twice the file
	// size and the next flush merges them.
	var replaced []mergeChunk
	for i, start := range []int{0, 3, 6, 9, 12, 15} {
		before := mergeEntryChunks(t, framework, name)
		window := make([]byte, 0, 16*mergePageSize)
		for p := start; p < start+16; p++ {
			window = append(window, mergePagePattern(p, i+1)...)
			version[p] = i + 1
		}
		_, err = held.WriteAt(window, int64(start*mergePageSize))
		require.NoError(t, err)
		require.NoError(t, held.Sync())
		if after := mergeEntryChunks(t, framework, name); len(before) > 1 && len(after) == 1 {
			replaced = before
			break
		}
	}
	require.NotEmpty(t, replaced, "the overlapping writes never made a flush merge the chunks")

	// The filer deletes the replaced chunks after the merge commits; vacuum
	// once they are deleted so that only the merged chunk is left on disk.
	master := framework.GetMasterAddr()
	require.Eventually(t, func() bool {
		for _, c := range replaced {
			if mergeChunkStatus(master, c, false) != http.StatusNotFound {
				return false
			}
		}
		return true
	}, 30*time.Second, 200*time.Millisecond, "the replaced chunks were never deleted")
	require.Eventually(t, func() bool {
		resp, err := mergeVacuumHTTP.Get(fmt.Sprintf("http://%s/vol/vacuum?garbageThreshold=0.0001", master))
		if err != nil {
			return false
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		for _, c := range replaced {
			if mergeChunkStatus(master, c, true) != http.StatusNotFound {
				return false
			}
		}
		return true
	}, 4*time.Minute, time.Second, "vacuum left the replaced chunks on disk")

	if unflushedWrite {
		_, err = held.WriteAt(mergePagePattern(mergePages-1, 99), int64((mergePages-1)*mergePageSize))
		require.NoError(t, err)
		version[mergePages-1] = 99
	}

	// A second descriptor shares the handle the first one keeps open. Drop its
	// pages from the kernel cache so every read goes to the mount.
	reader, err := os.Open(path)
	require.NoError(t, err)
	defer reader.Close()
	require.NoError(t, unix.Fadvise(int(reader.Fd()), 0, 0, unix.FADV_DONTNEED))
	started := time.Now()
	buf := make([]byte, mergePageSize)
	for p := 0; p < mergePages; p++ {
		n, err := reader.ReadAt(buf, int64(p*mergePageSize))
		require.NoError(t, err, "read page %d through the open handle after the merge", p)
		require.Equal(t, mergePageSize, n, "page %d", p)
		require.Equal(t, mergePagePattern(p, version[p]), buf, "page %d", p)
	}
	assert.Less(t, time.Since(started), 5*time.Second, "reads through the open handle waited on chunks the merge replaced")
}

func mergePagePattern(page, version int) []byte {
	b := make([]byte, mergePageSize)
	for i := 0; i < len(b); i += 8 {
		binary.LittleEndian.PutUint32(b[i:], uint32(page))
		binary.LittleEndian.PutUint32(b[i+4:], uint32(version))
	}
	return b
}

type mergeChunk struct {
	FileId string `json:"file_id"`
	Fid    *struct {
		VolumeId uint32 `json:"volume_id"`
		FileKey  uint64 `json:"file_key"`
		Cookie   uint32 `json:"cookie"`
	} `json:"fid"`
}

func (c mergeChunk) fileId() string {
	if c.FileId != "" || c.Fid == nil {
		return c.FileId
	}
	return fmt.Sprintf("%d,%x%08x", c.Fid.VolumeId, c.Fid.FileKey, c.Fid.Cookie)
}

// mergeEntryChunks returns the chunks the filer stores for the file.
func mergeEntryChunks(t *testing.T, framework *FuseTestFramework, name string) []mergeChunk {
	t.Helper()
	resp, err := mergeHTTP.Get(fmt.Sprintf("http://%s/%s?metadata=true", framework.GetFilerAddr(), url.PathEscape(name)))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "filer metadata of %s", name)
	var entry struct {
		Chunks []mergeChunk `json:"chunks"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&entry))
	return entry.Chunks
}

// mergeChunkStatus HEADs a chunk on the volume server holding it, and returns
// 0 when it cannot ask. With readDeleted, a deleted needle still answers until
// a vacuum removes it.
func mergeChunkStatus(master string, c mergeChunk, readDeleted bool) int {
	fid := c.fileId()
	volumeId := strings.SplitN(fid, ",", 2)[0]
	resp, err := mergeHTTP.Get(fmt.Sprintf("http://%s/dir/lookup?volumeId=%s", master, volumeId))
	if err != nil {
		return 0
	}
	var lookup struct {
		Locations []struct {
			Url string `json:"url"`
		} `json:"locations"`
	}
	err = json.NewDecoder(resp.Body).Decode(&lookup)
	resp.Body.Close()
	if err != nil || len(lookup.Locations) == 0 {
		return 0
	}
	target := fmt.Sprintf("http://%s/%s", lookup.Locations[0].Url, fid)
	if readDeleted {
		target += "?readDeleted=true"
	}
	head, err := mergeHTTP.Head(target)
	if err != nil {
		return 0
	}
	head.Body.Close()
	return head.StatusCode
}
