package command

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSelectVolumeFile(t *testing.T) {
	for _, tc := range []struct {
		name, ext  string
		volumeId   int64
		want       volumeFile
		wantOk     bool
		wantErr    bool
		collection string
	}{
		{name: "fix_7.dat", ext: ".dat", want: volumeFile{"fix_7", "fix", 7}, wantOk: true},
		{name: "7.dat", ext: ".dat", want: volumeFile{"7", "", 7}, wantOk: true},
		{name: "fix_7.idx", ext: ".dat"},
		{name: "fix_7.dat", ext: ".dat", volumeId: 8},
		{name: "fix_7.dat", ext: ".dat", collection: "other"},
		{name: "fix_4294967296.dat", ext: ".dat", volumeId: 7},
		{name: "x_99999999999.dat", ext: ".dat", wantErr: true},
		{name: "x_abc.dat", ext: ".dat", wantErr: true},
	} {
		vf, ok, err := selectVolumeFile(tc.name, tc.ext, tc.collection, tc.volumeId)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, want error %v", tc.name, err, tc.wantErr)
			continue
		}
		if ok != tc.wantOk || (ok && vf != tc.want) {
			t.Errorf("%s: got %+v ok=%v, want %+v ok=%v", tc.name, vf, ok, tc.want, tc.wantOk)
		}
	}
}

func TestRunFixRecoverNeedsAPath(t *testing.T) {
	captureStdout(t, func() {
		if runFixRecover(cmdFixRecover, nil) {
			t.Error("fix.recover without a path was accepted")
		}
	})
}

func TestRunFixRecoverRebuildsIndex(t *testing.T) {
	dir := t.TempDir()
	base, offsets := writeFixTestVolume(t, dir, 6)
	patchDat(t, base+".dat", offsets[2], bytes.Repeat([]byte{0xA5}, types.NeedleHeaderSize))
	withVerifyFlags(t, false)

	captureStdout(t, func() {
		if !runFixRecover(cmdFixRecover, []string{dir}) {
			t.Error("fix.recover failed on a damaged volume")
		}
	})
	if got := indexedKeys(t, base+".idx"); !slices.Equal(got, []uint64{1, 2, 4, 5, 6}) {
		t.Fatalf("recovered keys %v, want [1 2 4 5 6]", got)
	}
	if CommandExitStatus() != 1 {
		t.Fatalf("exit status %d, want 1 after damage", CommandExitStatus())
	}
}

func TestRunFixRecoverVolumeIdFilter(t *testing.T) {
	dir := t.TempDir()
	writeFixTestVolume(t, dir, 2)
	if err := os.Rename(filepath.Join(dir, "fix_7.dat"), filepath.Join(dir, "fix_8.dat")); err != nil {
		t.Fatal(err)
	}
	writeFixTestVolume(t, dir, 3)
	withVerifyFlags(t, false)
	saved := *fixRecoverVolumeId
	*fixRecoverVolumeId = 7
	t.Cleanup(func() { *fixRecoverVolumeId = saved })

	captureStdout(t, func() {
		if !runFixRecover(cmdFixRecover, []string{dir}) {
			t.Error("fix.recover failed")
		}
	})
	if got := indexedKeys(t, filepath.Join(dir, "fix_7.idx")); len(got) != 3 {
		t.Fatalf("volume 7 keys %v, want 3", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "fix_8.idx")); !os.IsNotExist(err) {
		t.Fatal("volume 8 was touched despite -volumeId=7")
	}
}

// Old scripts that pass the recovery flags to weed fix must fail loudly
// instead of silently running a plain fix.
func TestFixNoLongerAcceptsRecoverFlags(t *testing.T) {
	for _, name := range []string{"verify", "dryRun", "truncateTail", "maxNeedleSizeMB"} {
		if cmdFix.Flag.Lookup(name) != nil {
			t.Errorf("weed fix still defines -%s", name)
		}
		if name != "verify" && cmdFixRecover.Flag.Lookup(name) == nil {
			t.Errorf("weed fix.recover lacks -%s", name)
		}
	}
	if cmdFix.Flag.Lookup("verifyCrc") == nil {
		t.Error("weed fix lost -verifyCrc")
	}
}
