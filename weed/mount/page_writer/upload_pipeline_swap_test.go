package page_writer

import (
	"os"
	"path/filepath"
	"testing"
)

// When the swap file cannot be created, SaveDataAt must fail the write
// instead of keeping a nil swap chunk and writing through it, and give back
// the write budget it reserved for the chunk.
func TestSaveDataAtFailsWhenSwapFileCannotBeCreated(t *testing.T) {
	for _, tc := range []struct {
		name       string
		accountant *WriteBufferAccountant
	}{
		{name: "no write budget"},
		{name: "write budget", accountant: NewWriteBufferAccountant(1 << 20)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			notADir := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(notADir, nil, 0600); err != nil {
				t.Fatal(err)
			}
			up := NewUploadPipeline(nil, 1024, nil, 16, filepath.Join(notADir, "swap"), tc.accountant)

			// A write that is not sequential goes to the swap file.
			n, err := up.SaveDataAt([]byte("data"), 0, false, 0)
			if err == nil {
				t.Fatalf("SaveDataAt = %d, nil; want an error", n)
			}
			if _, found := up.writableChunks[0]; found {
				t.Error("the failed write left a writable chunk behind")
			}
			if used := tc.accountant.Used(); used != 0 {
				t.Errorf("write budget still holds %d bytes", used)
			}
		})
	}
}
