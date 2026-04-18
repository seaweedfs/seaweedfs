package fuse_p2p

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// peerReadSuccessMarker is the log line tryPeerRead emits when a peer
// fetch succeeded. The test greps non-writer mount logs for it to
// prove the p2p path fired. At glog verbosity 4 (framework sets -v=4).
const peerReadSuccessMarker = "peer read successful"

// seedConvergenceTimeout bounds how long the test waits for:
//   - the filer registry to list all mounts,
//   - every mount's seed view (via MountList poll) to include all peers,
//   - the first announcer flush cycle to publish chunk holders.
//
// With defaults the mount polls MountList every 30 s and flushes
// ChunkAnnounce every 15 s. Allowing 45 s absorbs one MountList refresh
// plus one announce cycle plus some slop for CI variance.
const seedConvergenceTimeout = 90 * time.Second

// TestPeerChunkSharing_ReadersPullFromPeerCache is the headline p2p
// integration test. It proves at least one non-writer mount can satisfy
// a read from the writer's chunk cache instead of the volume tier.
//
//  1. Bring up 1 master/volume/filer + 3 mounts, all with -peer.enable.
//  2. Mount 0 writes a ~8 MiB file and reads it back so chunks land in
//     its local cache and the announcer publishes them.
//  3. Wait for seed convergence + at least one announcer flush cycle.
//  4. BOTH mount 1 and mount 2 read the file.
//
// Why both readers: with 3 mounts, HRW picks one owner for the chunk.
// If that owner is mount 1, only mount 2's read will hit the peer
// path (mount 1's tryPeerRead bails on owner==self). If that owner is
// mount 2, only mount 1's read will. If that owner is mount 0 (the
// writer), both can. So by reading from both, we deterministically
// guarantee at least one non-writer mount exercises the peer path.
//
// Once the peer fetch populates its local cache, subsequent reads
// short-circuit on IsInCache — so we only get one real shot per mount
// per chunk. That's fine: one success is all the test needs.
func TestPeerChunkSharing_ReadersPullFromPeerCache(t *testing.T) {
	c := startP2PTestCluster(t, 3)

	// ~8 MiB, pseudo-random so compression doesn't collapse it to one block.
	payload := make([]byte, 8*1024*1024)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}

	const relPath = "p2p-test.bin"
	writer := c.MountDir(0)
	require.NoError(t, os.WriteFile(filepath.Join(writer, relPath), payload, 0644))

	// Warm mount 0's chunk cache by reading back through its own FUSE.
	// Without this the chunks are on the volume server but not yet
	// in anyone's peer-servable cache.
	readBack, err := os.ReadFile(filepath.Join(writer, relPath))
	require.NoError(t, err)
	require.True(t, bytes.Equal(readBack, payload), "write-then-read on writer mount should match")

	waitForSeedConvergence(t, c, seedConvergenceTimeout)

	// Give the announcer several flush windows to push chunk-holder
	// entries to the HRW owners. First flush may see the writer's seed
	// view incomplete (only self) and defer all fids; subsequent
	// flushes re-check against a refreshed seed view. announce interval
	// is 15 s, so 45 s covers three attempts.
	time.Sleep(45 * time.Second)

	// Read from both non-writer mounts. For any HRW outcome on any
	// chunk, at least one of these reads will NOT have the reader as
	// the HRW owner, so its tryPeerRead will proceed to ChunkLookup +
	// FetchChunk.
	for _, idx := range []int{1, 2} {
		got, err := os.ReadFile(filepath.Join(c.MountDir(idx), relPath))
		require.NoError(t, err, "read from mount %d must succeed\n--- mount%d ---\n%s",
			idx, idx, tailLines(c.MountLog(idx), 80))
		require.Equal(t, md5.Sum(payload), md5.Sum(got),
			"mount %d returned mismatched bytes (len got=%d want=%d)", idx, len(got), len(payload))
	}

	// Content matches alone doesn't prove p2p — the volume fallback
	// would also satisfy the reads. Require at least one non-writer
	// mount's log to contain the peer-read success marker.
	var sawPeerRead bool
	for _, idx := range []int{1, 2} {
		if strings.Contains(c.MountLog(idx), peerReadSuccessMarker) {
			sawPeerRead = true
			break
		}
	}
	if !sawPeerRead {
		t.Fatalf("no non-writer mount logged %q — peer read path never fired.\n"+
			"--- mount0 (writer) tail ---\n%s\n--- mount1 tail ---\n%s\n--- mount2 tail ---\n%s",
			peerReadSuccessMarker,
			tailLines(c.MountLog(0), 60), tailLines(c.MountLog(1), 60), tailLines(c.MountLog(2), 60))
	}
}

// waitForSeedConvergence polls each mount's log looking for any sign
// that MountList returned a peer list containing the other mounts.
func waitForSeedConvergence(t *testing.T, c *p2pTestCluster, timeout time.Duration) {
	t.Helper()
	// The first MountRegister happens synchronously during mount startup,
	// and the first MountList is pulled right after. 30 s is the refresh
	// interval; waiting one full cycle here guarantees every mount has
	// at minimum observed the others in its seed view.
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for i := range c.mountCmds {
			if !strings.Contains(c.MountLog(i), "peer-grpc listening on") {
				ready = false
				break
			}
		}
		if ready {
			time.Sleep(30 * time.Second)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("peer-grpc servers did not come up within %s", timeout)
}

// tailLines returns the last n newline-delimited lines of s, or the
// whole thing if shorter. Keeps test failures readable.
func tailLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return fmt.Sprintf("... (%d earlier lines omitted) ...\n%s",
		len(lines)-n, strings.Join(lines[len(lines)-n:], "\n"))
}
