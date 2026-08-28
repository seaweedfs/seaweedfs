package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The walk reads in batches, so it has to keep going past the first one and
// stop at the end without repeating or dropping an entry.
func TestEachDirEntrySeesEveryEntry(t *testing.T) {
	dir := t.TempDir()
	want := make(map[string]bool, dirScanBatch*2+3)
	for i := 0; i < dirScanBatch*2+3; i++ {
		name := fmt.Sprintf("%d.dat", i)
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0644); err != nil {
			t.Fatal(err)
		}
		want[name] = true
	}

	seen := make(map[string]bool, len(want))
	if err := eachDirEntry(dir, func(entry os.DirEntry) bool {
		if seen[entry.Name()] {
			t.Errorf("entry %s visited twice", entry.Name())
		}
		seen[entry.Name()] = true
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(want) {
		t.Fatalf("walked %d entries, want %d", len(seen), len(want))
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("entry %s was never visited", name)
		}
	}
}

func TestEachDirEntryStopsWhenAsked(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < dirScanBatch+10; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.dat", i)), nil, 0644); err != nil {
			t.Fatal(err)
		}
	}

	visited := 0
	if err := eachDirEntry(dir, func(entry os.DirEntry) bool {
		visited++
		return visited < 5
	}); err != nil {
		t.Fatal(err)
	}
	if visited != 5 {
		t.Errorf("walk visited %d entries after being asked to stop at 5", visited)
	}
}

func TestEachDirEntryReportsAMissingDirectory(t *testing.T) {
	if err := eachDirEntry(filepath.Join(t.TempDir(), "absent"), func(os.DirEntry) bool {
		t.Error("visited an entry of a directory that does not exist")
		return true
	}); err == nil {
		t.Error("walking a missing directory reported no error")
	}
}
