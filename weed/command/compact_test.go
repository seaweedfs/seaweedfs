package command

import (
	"bytes"
	"os"
	"testing"
)

func TestCompactDataMethodBadHeaderLeavesNoOutput(t *testing.T) {
	dir := t.TempDir()
	base, offsets := writeFixTestVolume(t, dir, 5)
	doFixOneVolume(dir, "fix_7", "fix", 7, true)
	overwriteAt(t, base+".dat", offsets[2], bytes.Repeat([]byte{0xa5}, 16))

	savedDir, savedCollection, savedId, savedMethod, savedStatus := *compactVolumePath, *compactVolumeCollection, *compactVolumeId, *compactMethod, commandExitStatus
	defer func() {
		*compactVolumePath, *compactVolumeCollection, *compactVolumeId, *compactMethod, commandExitStatus = savedDir, savedCollection, savedId, savedMethod, savedStatus
	}()
	*compactVolumePath, *compactVolumeCollection, *compactVolumeId, *compactMethod, commandExitStatus = dir, "fix", 7, "data", 0

	runCompact(cmdCompact, nil)

	if CommandExitStatus() == 0 {
		t.Error("expected a non-zero exit status")
	}
	for _, ext := range []string{".cpd", ".cpx"} {
		if _, err := os.Stat(base + ext); !os.IsNotExist(err) {
			t.Errorf("%s left behind: %v", ext, err)
		}
	}
}
