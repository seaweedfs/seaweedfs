package mount

import "testing"

func TestDiskSizes(t *testing.T) {
	wfs := &WFS{option: &Option{}}
	wfs.stats.TotalSize = 3000
	wfs.stats.UsedSize = 2000
	wfs.stats.LogicalTotalSize = 1500
	wfs.stats.LogicalUsedSize = 1000

	total, used := wfs.diskSizes()
	if total != 3000 || used != 2000 {
		t.Errorf("raw sizes: got %d/%d, want 3000/2000", total, used)
	}

	wfs.option.LogicalDiskUsage = true
	total, used = wfs.diskSizes()
	if total != 1500 || used != 1000 {
		t.Errorf("logical sizes: got %d/%d, want 1500/1000", total, used)
	}

	// a filer that does not report logical sizes must not read as an empty
	// filesystem
	wfs.stats.LogicalTotalSize = 0
	wfs.stats.LogicalUsedSize = 0
	total, used = wfs.diskSizes()
	if total != 3000 || used != 2000 {
		t.Errorf("sizes from a filer without logical support: got %d/%d, want 3000/2000", total, used)
	}
}
