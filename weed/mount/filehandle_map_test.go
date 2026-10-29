package mount

import (
	"testing"
)

// A handle whose last open closed with a deferred flush stays in the map while
// the drain runs so rename/unlink can find it. A new open must NOT attach to it:
// the fh id it would mint is dropped by RemoveFileHandle, leaving kernel reads
// to fail with ENOENT — which the kernel reports to read() as EIO.
func TestAcquireFileHandleRefusesDrainingHandle(t *testing.T) {
	wfs := &WFS{option: &Option{}}
	fhMap := NewFileHandleToInode()
	inode := uint64(12345)

	fh, _, existed, draining := fhMap.AcquireFileHandle(wfs, inode, nil, 0, 0)
	if existed || draining || fh == nil {
		t.Fatalf("first acquire: fh=%v existed=%v draining=%v", fh, existed, draining)
	}
	fh.asyncFlushPending = true

	// Last release with a deferred flush keeps the draining handle in the map.
	released := fhMap.ReleaseByHandle(fh.fh)
	if released != fh {
		t.Fatalf("ReleaseByHandle returned %v, want the draining handle", released)
	}
	if _, found := fhMap.FindFileHandle(inode); !found {
		t.Fatal("draining handle should stay in the map for rename/unlink")
	}

	// A new open must not resurrect the draining handle.
	fh2, openFh, existed, draining := fhMap.AcquireFileHandle(wfs, inode, nil, 0, 0)
	if !draining || fh2 != nil {
		t.Fatalf("acquire during drain: fh=%v openFh=%v existed=%v draining=%v, want draining", fh2, openFh, existed, draining)
	}

	// After the drain removes the handle, the next acquire creates a fresh one.
	fhMap.RemoveFileHandle(fh.fh, inode)
	fh3, _, existed, draining := fhMap.AcquireFileHandle(wfs, inode, nil, 0, 0)
	if draining || existed || fh3 == nil || fh3 == fh {
		t.Fatalf("acquire after drain: fh=%v existed=%v draining=%v, want fresh handle", fh3, existed, draining)
	}
}
