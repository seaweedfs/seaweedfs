package needle

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// readNeedleBodyBytes runs ReadNeedleBodyBytes and turns a panic into a test
// failure, so a regression reports which case broke instead of killing the run.
func readNeedleBodyBytes(t *testing.T, n *Needle, body []byte, version Version) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ReadNeedleBodyBytes panicked for size %d, body length %d: %v", n.Size, len(body), r)
		}
	}()
	return n.ReadNeedleBodyBytes(body, version)
}

// The size feeds the read buffer's length, so a negative one never reaches make().
func TestReadNeedleBlobRejectsNegativeSize(t *testing.T) {
	for _, version := range []Version{Version1, Version2, Version3} {
		for _, size := range []Size{TombstoneFileSize, -100} {
			if _, err := ReadNeedleBlob(nil, 0, size, version); !errors.Is(err, ErrorSizeInvalid) {
				t.Fatalf("version %d size %d: expected ErrorSizeInvalid, got %v", version, size, err)
			}
		}
	}
}

// A corrupted .dat header can carry a size that does not fit the body read for
// it. Vacuum used to panic on it with "slice bounds out of range [:-1]" (#6763).
func TestReadNeedleBodyBytesRejectsCorruptSize(t *testing.T) {
	for version := Version1; IsSupportedVersion(version); version++ {
		t.Run(versionString(version), func(t *testing.T) {
			// A size of -1 is the case from #6763: its body length is still
			// positive, so the scan reads a body and hands it over.
			bodyLength := NeedleBodyLength(-1, version)
			if bodyLength <= 0 {
				t.Fatalf("expected a positive body length for size -1, got %d", bodyLength)
			}

			cases := []struct {
				name string
				size Size
				body int
			}{
				{"size -1", -1, int(bodyLength)},
				{"size -12", -12, 32},
				{"size larger than body", 64, 32},
				{"no room for the tail", 32, 32},
				{"empty body", 0, 0},
				{"empty body with data size", 1, 0},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					n := &Needle{Size: c.size}
					err := readNeedleBodyBytes(t, n, make([]byte, c.body), version)
					if !errors.Is(err, ErrorCorrupted) {
						t.Fatalf("expected an error wrapping ErrorCorrupted, got %v", err)
					}
				})
			}
		})
	}
}

// The size guard must still accept every record the writer produces,
// including the size-0 record a delete appends.
func TestReadNeedleBodyBytesWrittenNeedles(t *testing.T) {
	for version := Version1; IsSupportedVersion(version); version++ {
		t.Run(versionString(version), func(t *testing.T) {
			for _, data := range [][]byte{nil, []byte("hello seaweed")} {
				written := &Needle{Id: 7, Cookie: 9, Data: data, Checksum: NewCRC(data), AppendAtNs: 42}
				buf := new(bytes.Buffer)
				if _, _, err := writeNeedleByVersion(version, written, 0, buf); err != nil {
					// Some builds can read a version they cannot write; skip
					// only that recognized case so a real writer regression
					// still fails the test.
					if strings.Contains(strings.ToLower(err.Error()), "unsupported version") {
						t.Skipf("version %d is not writable in this build: %v", version, err)
					}
					t.Fatalf("write needle: %v", err)
				}

				n := new(Needle)
				n.ParseNeedleHeader(buf.Bytes())
				body := buf.Bytes()[NeedleHeaderSize:]
				if int64(len(body)) != NeedleBodyLength(n.Size, version) {
					t.Fatalf("body length %d, want %d", len(body), NeedleBodyLength(n.Size, version))
				}
				if err := readNeedleBodyBytes(t, n, body, version); err != nil {
					t.Fatalf("read %d-byte needle: %v", len(data), err)
				}
				if !bytes.Equal(n.Data, data) {
					t.Fatalf("data %q, want %q", n.Data, data)
				}
			}
		})
	}
}
