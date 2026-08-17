//go:build linux || darwin

package fuse_failover

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The scenarios below come from the Docker Swarm report in discussion 10206:
// one mount appends to a file while a second mount reads it, and a volume
// server is stopped, started or restarted underneath. With 001 replication a
// single volume server loss must never surface as EIO on either side.

const (
	appendLines      = 200
	maxAppendLatency = 10 * time.Second
	convergeTimeout  = 30 * time.Second
)

type appendResult struct {
	errs       []error
	maxLatency time.Duration
	slowest    int
}

// appendLoop mimics `for i in ...; do echo $i >> file; done`: every line is its
// own open/write/close, so every line forces a flush and a chunk upload.
func appendLoop(path string, lines int, onLine func(i int)) *appendResult {
	res := &appendResult{}
	for i := 0; i < lines; i++ {
		start := time.Now()
		err := appendOnce(path, fmt.Sprintf("%07d\n", i))
		elapsed := time.Since(start)
		if elapsed > res.maxLatency {
			res.maxLatency, res.slowest = elapsed, i
		}
		if err != nil {
			res.errs = append(res.errs, fmt.Errorf("line %d: %w", i, err))
		}
		if onLine != nil {
			onLine(i)
		}
	}
	return res
}

func appendOnce(path, line string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if _, err = f.WriteString(line); err != nil {
		f.Close()
		return fmt.Errorf("write: %w", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

// reader is the `tail -f` side: it keeps re-reading the whole file from the
// other mount and records every failure that is not "not created yet".
type reader struct {
	stop chan struct{}
	done chan struct{}
	errs []error
}

func startReader(path string) *reader {
	r := &reader{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		for {
			select {
			case <-r.stop:
				return
			default:
			}
			if _, err := os.ReadFile(path); err != nil && !os.IsNotExist(err) {
				r.errs = append(r.errs, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	return r
}

func (r *reader) Stop() []error {
	close(r.stop)
	<-r.done
	return r.errs
}

// waitForContent re-reads path until it matches want or the timeout passes.
// A mount caches metadata for about a second, so a read taken the instant the
// writer's last close returned can legitimately still be behind; content that
// is wrong rather than merely late never converges and still fails.
func waitForContent(path string, want []byte, timeout time.Duration) (got []byte, ok bool) {
	deadline := time.Now().Add(timeout)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			got = data
			if bytes.Equal(got, want) {
				return got, true
			}
		}
		if time.Now().After(deadline) {
			return got, false
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func expectedAppendContent(lines int) []byte {
	var buf []byte
	for i := 0; i < lines; i++ {
		buf = append(buf, []byte(fmt.Sprintf("%07d\n", i))...)
	}
	return buf
}

// TestReadWithVolumeServerDown covers the original report: a file written while
// everything was healthy must stay readable from a second mount after any
// single volume server goes away.
func TestReadWithVolumeServerDown(t *testing.T) {
	c := startFailoverCluster(t, 3, 2)

	const fileSize = 8 << 20
	payloads := make(map[string][]byte)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("readfile-%d", i)
		data := make([]byte, fileSize)
		_, err := rand.Read(data)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(c.MountDir(0), name), data, 0644))
		payloads[name] = data
	}
	// Let the filer commit the last chunks before anything is torn down.
	time.Sleep(2 * time.Second)

	t.Logf("topology before chaos:\n%s", c.MasterGet("/dir/status?pretty=y"))

	alreadyRead := make(map[string]bool)
	for victim := 0; victim < 3; victim++ {
		// Placement decides which two of the three servers hold each volume, so
		// pick a file this victim actually backs: reading one it never held
		// would pass without exercising recovery at all. Prefer a file no
		// earlier iteration has read, whose chunks the reader has not cached.
		name := ""
		for _, allowRead := range []bool{false, true} {
			for i := 0; i < 3 && name == ""; i++ {
				candidate := fmt.Sprintf("readfile-%d", i)
				if alreadyRead[candidate] && !allowRead {
					continue
				}
				onVictim, err := c.FileIsOn("/"+candidate, c.VolumeServerAddress(victim))
				require.NoError(t, err, "resolve placement of %s", candidate)
				if onVictim {
					name = candidate
				}
			}
		}
		require.NotEmpty(t, name, "no test file has a replica on volume%d, nothing to fail over from\n%s",
			victim, c.MasterGet("/dir/status?pretty=y"))
		alreadyRead[name] = true

		c.KillVolume(victim)

		start := time.Now()
		got, err := os.ReadFile(filepath.Join(c.MountDir(1), name))
		elapsed := time.Since(start)
		if err != nil {
			t.Logf("topology with volume%d down:\n%s", victim, c.MasterGet("/dir/status?pretty=y"))
		}
		require.NoError(t, err, "read %s with volume %d down\n%s", name, victim, c.tailLog("mount1"))
		require.Equal(t, sha256.Sum256(payloads[name]), sha256.Sum256(got),
			"content mismatch for %s with volume %d down", name, victim)
		t.Logf("read %s (held by volume%d) with volume%d down in %v", name, victim, victim, elapsed)

		require.NoError(t, c.StartVolume(victim))
		time.Sleep(3 * time.Second) // let the master see the heartbeat again
	}
}

// TestAppendWithoutChaos is the control for the chaos runs below: the same
// append-and-tail workload with nothing being stopped or started.
func TestAppendWithoutChaos(t *testing.T) {
	c := startFailoverCluster(t, 3, 2)
	runChaosAppend(t, c, "no-chaos", func() {})
}

// TestAppendWhileVolumeServerStops is scenario "STOP volumes": append from
// mount0 and tail from mount1 while one volume server is killed mid-stream.
func TestAppendWhileVolumeServerStops(t *testing.T) {
	for victim := 0; victim < 3; victim++ {
		t.Run(fmt.Sprintf("volume%d", victim), func(t *testing.T) {
			c := startFailoverCluster(t, 3, 2)
			runChaosAppend(t, c, fmt.Sprintf("stop-%d", victim), func() {
				c.KillVolume(victim)
			})
		})
	}
}

// TestAppendWhileVolumeServerStarts is scenario "Start volumes": the cluster is
// already one server down when the writer starts, and that server comes back
// mid-stream. This is where the reporter saw 12-38 s stalls per append.
func TestAppendWhileVolumeServerStarts(t *testing.T) {
	for victim := 0; victim < 3; victim++ {
		t.Run(fmt.Sprintf("volume%d", victim), func(t *testing.T) {
			c := startFailoverCluster(t, 3, 2)
			c.KillVolume(victim)
			time.Sleep(3 * time.Second)
			runChaosAppend(t, c, fmt.Sprintf("start-%d", victim), func() {
				require.NoError(t, c.StartVolume(victim))
			})
		})
	}
}

// TestAppendWhileVolumeServerRestarts is scenario "Re-start volumes".
func TestAppendWhileVolumeServerRestarts(t *testing.T) {
	for victim := 0; victim < 3; victim++ {
		t.Run(fmt.Sprintf("volume%d", victim), func(t *testing.T) {
			c := startFailoverCluster(t, 3, 2)
			runChaosAppend(t, c, fmt.Sprintf("restart-%d", victim), func() {
				c.KillVolume(victim)
				time.Sleep(2 * time.Second)
				require.NoError(t, c.StartVolume(victim))
			})
		})
	}
}

// TestLargeWriteWhileVolumeServerStops is the part-2 variant: a single large
// copy instead of many small appends, checksum-verified from the other mount.
func TestLargeWriteWhileVolumeServerStops(t *testing.T) {
	c := startFailoverCluster(t, 3, 2)

	const size = 64 << 20
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)

	path := filepath.Join(c.MountDir(0), "largefile")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)

	killed := false
	const block = 1 << 20
	for off := 0; off < size; off += block {
		if !killed && off >= size/3 {
			c.KillVolume(2)
			killed = true
		}
		_, werr := f.Write(data[off : off+block])
		require.NoError(t, werr, "write at offset %d with volume2 down\n%s", off, c.tailLog("mount0"))
	}
	require.NoError(t, f.Close())

	got, converged := waitForContent(filepath.Join(c.MountDir(1), "largefile"), data, convergeTimeout)
	require.True(t, converged, "large file did not converge on mount1: want %d bytes %x, got %d bytes %x\n%s",
		len(data), sha256.Sum256(data), len(got), sha256.Sum256(got), c.tailLog("mount1"))
}

// runChaosAppend appends from mount0 with a reader tailing on mount1, firing
// chaos once the writer is a quarter of the way in.
func runChaosAppend(t *testing.T, c *failoverCluster, name string, chaos func()) {
	t.Helper()

	writePath := filepath.Join(c.MountDir(0), name)
	readPath := filepath.Join(c.MountDir(1), name)

	r := startReader(readPath)

	var once sync.Once
	res := appendLoop(writePath, appendLines, func(i int) {
		if i == appendLines/4 {
			once.Do(chaos)
		}
	})
	readErrs := r.Stop()

	require.Empty(t, res.errs, "append errors\n%s", c.tailLog("mount0"))
	require.Empty(t, readErrs, "reader errors\n%s", c.tailLog("mount1"))
	t.Logf("%s: slowest append was line %d at %v", name, res.slowest, res.maxLatency)
	// A healthy append stays well under a second; the reported regression parked
	// single appends at 12-38 s while a volume server was coming back.
	require.Less(t, res.maxLatency, maxAppendLatency,
		"append %d stalled for %v\n%s", res.slowest, res.maxLatency, c.tailLog("mount0"))

	wantBytes := expectedAppendContent(appendLines)
	want := string(wantBytes)
	gotBytes, converged := waitForContent(readPath, wantBytes, convergeTimeout)
	got := string(gotBytes)
	if converged {
		return
	}

	// The reader never caught up. Show where it diverges and what the writer's
	// own mount and the filer make of the same file, which says whether the
	// data was lost on the way in or is only invisible from this side.
	d := firstDiff(want, got)
	fromWriter, _ := os.ReadFile(writePath)
	viaFiler, filerErr := c.FilerGet("/" + name)
	require.Failf(t, "final content mismatch",
		"%s: first difference at offset %d (want %d bytes, got %d)\nwant %q\ngot  %q\nmount0 matches=%v filer matches=%v (err %v)\n%s",
		name, d, len(want), len(got), window(want, d), window(got, d),
		string(fromWriter) == want, string(viaFiler) == want, filerErr,
		c.tailLog("mount0"))
}

// firstDiff returns the offset of the first differing byte, or -1 when equal.
func firstDiff(want, got string) int {
	for i := 0; i < len(want) && i < len(got); i++ {
		if want[i] != got[i] {
			return i
		}
	}
	if len(want) != len(got) {
		return min(len(want), len(got))
	}
	return -1
}

func window(s string, at int) string {
	return s[max(0, at-24):min(len(s), at+24)]
}
