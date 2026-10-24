package erasure_coding

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A decode hands the caller a volume and the caller then deletes the shards it
// came from. VerifyDecodedDatFile is what stands between a reconstruction that
// came up short -- a truncated shard, a partial write, a full disk -- and the
// deletion of the only other copy of the needles past the cut.
func TestVerifyDecodedDatFile(t *testing.T) {
	tests := []struct {
		name        string
		written     int64
		referenced  int64
		wantErr     bool
		errContains string
	}{
		{
			name:       "exactly the referenced extent is complete",
			written:    4096,
			referenced: 4096,
		},
		{
			name:       "longer than referenced is fine: padding is not missing data",
			written:    8192,
			referenced: 4096,
		},
		{
			name:        "one byte short still loses the last needle",
			written:     4095,
			referenced:  4096,
			wantErr:     true,
			errContains: "short of the 4096",
		},
		{
			name:        "an empty file is short of everything",
			written:     0,
			referenced:  4096,
			wantErr:     true,
			errContains: "is 0 bytes",
		},
		{
			name:       "an index referencing nothing accepts an empty file",
			written:    0,
			referenced: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "5")
			if err := os.WriteFile(base+".dat", make([]byte, tt.written), 0644); err != nil {
				t.Fatal(err)
			}

			err := VerifyDecodedDatFile(base, tt.referenced)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("wrote %d bytes against a %d-byte extent, want an error", tt.written, tt.referenced)
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not mention %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("wrote %d bytes against a %d-byte extent: %v", tt.written, tt.referenced, err)
			}
		})
	}
}

// A missing .dat is not a short one, and the two want different answers: the
// caller cannot tell whether the decode wrote nothing or wrote elsewhere, so
// the error has to name the file rather than a byte count.
func TestVerifyDecodedDatFileMissing(t *testing.T) {
	base := filepath.Join(t.TempDir(), "7")

	err := VerifyDecodedDatFile(base, 4096)
	if err == nil {
		t.Fatal("a missing .dat must not verify")
	}
	if !strings.Contains(err.Error(), "stat decoded") {
		t.Errorf("error %q does not say the file could not be read", err)
	}
}
