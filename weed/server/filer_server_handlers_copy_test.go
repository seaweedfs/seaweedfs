package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCopyEntryRefreshesDestinationTimestamps(t *testing.T) {
	fs := &FilerServer{}

	oldTime := time.Unix(123, 0)
	srcEntry := &filer.Entry{
		FullPath: util.FullPath("/src.txt"),
		Attr: filer.Attr{
			Mtime:  oldTime,
			Crtime: oldTime,
		},
		Content: []byte("hello"),
	}

	before := time.Now().Add(-time.Second)
	copied, err := fs.copyEntry(context.Background(), srcEntry, util.FullPath("/dst.txt"), nil)
	after := time.Now().Add(time.Second)
	if err != nil {
		t.Fatalf("copyEntry: %v", err)
	}

	if copied.Crtime.Before(before) || copied.Crtime.After(after) {
		t.Fatalf("copied Crtime = %v, want between %v and %v", copied.Crtime, before, after)
	}
	if copied.Mtime.Before(before) || copied.Mtime.After(after) {
		t.Fatalf("copied Mtime = %v, want between %v and %v", copied.Mtime, before, after)
	}
	if copied.Crtime.Equal(oldTime) || copied.Mtime.Equal(oldTime) {
		t.Fatalf("destination timestamps should differ from source timestamps: src=%v copied=(%v,%v)", oldTime, copied.Crtime, copied.Mtime)
	}
}
