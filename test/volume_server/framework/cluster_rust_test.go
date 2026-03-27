package framework

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestRustVolumeArgsIncludeReadMode(t *testing.T) {
	profile := matrix.P1()
	profile.ReadMode = "redirect"
	profile.ConcurrentUploadLimitMB = 7
	profile.ConcurrentDownloadLimitMB = 9
	profile.InflightUploadTimeout = 3 * time.Second
	profile.InflightDownloadTimeout = 4 * time.Second

	args := rustVolumeArgs(profile, "/tmp/config", 9333, 18080, 28080, 38080, "/tmp/data")

	assertArgPair(t, args, "--readMode", "redirect")
	assertArgPair(t, args, "--concurrentUploadLimitMB", "7")
	assertArgPair(t, args, "--concurrentDownloadLimitMB", "9")
	assertArgPair(t, args, "--inflightUploadDataTimeout", "3s")
	assertArgPair(t, args, "--inflightDownloadDataTimeout", "4s")
}

func assertArgPair(t *testing.T, args []string, flag string, want string) {
	t.Helper()
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == flag {
			if args[i+1] != want {
				t.Fatalf("%s value mismatch: got %q want %q", flag, args[i+1], want)
			}
			return
		}
	}
	t.Fatalf("missing %s in args: %v", flag, args)
}
