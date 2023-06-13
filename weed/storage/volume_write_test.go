package storage

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSearchVolumesWithDeletedNeedles(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	count := 20

	for i := 1; i < count; i++ {
		n := newRandomNeedle(uint64(i))
		_, _, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}

	for i := 1; i < 15; i++ {
		n := newEmptyNeedle(uint64(i))
		err := v.nm.Put(n.Id, types.Offset{}, types.TombstoneFileSize)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	ts1 := time.Now().UnixNano()

	for i := 15; i < count; i++ {
		n := newEmptyNeedle(uint64(i))
		_, err := v.doDeleteRequest(n)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	offset, isLast, err := v.BinarySearchByAppendAtNs(uint64(ts1))
	if err != nil {
		t.Fatalf("lookup by ts: %v", err)
	}
	fmt.Printf("offset: %v, isLast: %v\n", offset.ToActualOffset(), isLast)

}

func isFileExist(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}

func assertFileExist(t *testing.T, expected bool, path string) {
	exist, err := isFileExist(path)
	if err != nil {
		t.Fatalf("isFileExist: %v", err)
	}
	assert.Equal(t, expected, exist)
}

func TestDestroyEmptyVolumeWithOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy empty volume with onlyEmpty
	assertFileExist(t, true, path)
	err = v.Destroy(true)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestDestroyEmptyVolumeWithoutOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy empty volume without onlyEmpty
	assertFileExist(t, true, path)
	err = v.Destroy(false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestDestroyNonemptyVolumeWithOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should return "volume not empty" error and do not delete file when Destroy non-empty volume
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(true)
	assert.EqualError(t, err, "volume not empty")
	assertFileExist(t, true, path)

	// should keep working after "volume not empty"
	_, _, _, err = v.writeNeedle2(newRandomNeedle(2), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}

	assert.Equal(t, uint64(2), v.FileCount())
}

func TestDestroyNonemptyVolumeWithoutOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy non-empty volume without onlyEmpty
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}
