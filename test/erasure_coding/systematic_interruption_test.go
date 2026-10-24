package erasure_coding

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/shell"
)

// TestECInterruptionMatrix is the systematic counterpart of the randomized
// chaos harness: instead of killing operations at random times, it enumerates
// every phase of every interruptible EC operation and kills a real weed shell
// exactly when that phase announces itself on the command's output. Each
// (operation, phase) pair is one deterministic scenario: prepare the
// precondition, kill at the phase marker, run the prescribed recovery, and
// verify that every stored byte still reads back identical.
//
// The phase markers are the progress lines the pipelines print; each marker
// below names the code that prints it. A marker is the START of its phase, so
// killing on it lands the interruption inside that phase — deterministically
// per phase, byte-exact timing within the phase left to the scheduler. Every
// scenario REQUIRES its marker to appear: a marker that never prints means a
// pipeline refactor renamed or dropped the progress line, and silently
// degenerating into a no-interruption run would let CI pass without
// exercising the boundary the scenario names. Update the marker with the
// pipeline.
//
// Complementing this, weed/ec's TestECLifecycleModelExhaustive explores every
// schedule of the same pipelines — crashes between every step, rollback
// paths, restarts — exhaustively on a model whose steps mirror the code.
func TestECInterruptionMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EC interruption matrix in short mode")
	}

	testDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	cluster, err := startChaosCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	require.NoError(t, waitForServer(chaosMasterAddr, 30*time.Second))
	for i := 0; i < chaosServerCount; i++ {
		require.NoError(t, waitForServer("127.0.0.1:"+chaosVolumePort(i), 30*time.Second))
	}
	time.Sleep(8 * time.Second)

	commandEnv := shell.NewCommandEnv(&shell.ShellOptions{
		Masters:        stringPtr(chaosMasterAddr),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	})
	connectToMasterAndSync(ctx, t, commandEnv)

	r := newChaosRun(t, ctx, cluster, commandEnv, testDir, 1)
	r.relock()
	defer r.unlockIfHeld()
	r.seedAndSpread()
	r.verify("seeding")

	// One scenario per (operation, phase marker). The preparation puts a
	// dedicated volume into the operation's precondition state, so scenarios
	// stay independent even though they share the cluster.
	type scenario struct {
		op      string
		marker  string // printed by ↓
		printed string // the code that prints it, for the reader
	}
	encodeScenarios := []scenario{
		{"ec.encode", "markVolumeReadonly ", "weed/ec markVolumeReplicaWritable"},
		{"ec.encode", "generateEcShards ", "weed/ec generateEcShards"},
		{"ec.encode", "mount ", "weed/ec MountEcShards"},
		{"ec.encode", "Deleting original volumes", "weed/ec ProcessEcEncodeBatch, the commit boundary"},
	}
	decodeScenarios := []scenario{
		{"ec.decode", " shard locations:", "weed/ec DoEcDecode, before collect"},
		{"ec.decode", "generateNormalVolume", "weed/ec generateNormalVolume"},
		{"ec.decode", "unmount ec volume", "weed/ec unmountAndDeleteEcShards, teardown begin"},
		{"ec.decode", "delete ec volume", "weed/ec unmountAndDeleteEcShards, shard deletion"},
	}

	for _, sc := range encodeScenarios {
		sc := sc
		t.Run(fmt.Sprintf("encode@%s", trimMarker(sc.marker)), func(t *testing.T) {
			vid := r.ensureRegularVolume()
			r.unlockIfHeld()
			out, hit := runShellKillAtMarker(t, r.ctx,
				fmt.Sprintf("ec.encode -volumeId %d -collection %s -force", vid, chaosCollection), sc.marker)
			r.t.Logf("killed %s at %q (marker hit: %v); output:\n%s", sc.op, sc.marker, hit, out)
			r.relock()
			require.True(t, hit, "marker %q never appeared (printed by %s); update the marker with the pipeline", sc.marker, sc.printed)
			r.recoverInterruptedEncode(vid)
			r.verify(fmt.Sprintf("encode killed at %q", sc.marker))
		})
	}
	for _, sc := range decodeScenarios {
		sc := sc
		t.Run(fmt.Sprintf("decode@%s", trimMarker(sc.marker)), func(t *testing.T) {
			vid := r.ensureEncodedVolume()
			r.unlockIfHeld()
			out, hit := runShellKillAtMarker(t, r.ctx,
				fmt.Sprintf("ec.decode -volumeId %d -collection %s -checkMinFreeSpace=false", vid, chaosCollection), sc.marker)
			r.t.Logf("killed %s at %q (marker hit: %v); output:\n%s", sc.op, sc.marker, hit, out)
			r.relock()
			require.True(t, hit, "marker %q never appeared (printed by %s); update the marker with the pipeline", sc.marker, sc.printed)
			r.recoverInterruptedDecode(vid)
			r.verify(fmt.Sprintf("decode killed at %q", sc.marker))
		})
	}

	// Balance: encode without rebalancing first, so the standalone balance is
	// guaranteed to plan moves and the marker is guaranteed to print.
	t.Run("balance@moves", func(t *testing.T) {
		vid := r.ensureRegularVolume()
		out, err := r.shellCommand("ec.encode",
			"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-force", "-rebalance=false")
		r.t.Logf("clumped encode v%d output:\n%s", vid, out)
		require.NoError(t, err, "encode without rebalance")
		r.volumes[vid].encoded = true

		r.unlockIfHeld()
		killOut, hit := runShellKillAtMarker(t, r.ctx,
			fmt.Sprintf("ec.balance -collection %s -apply", chaosCollection), "moves ec shard")
		r.t.Logf("killed ec.balance at move (marker hit: %v); output:\n%s", hit, killOut)
		r.relock()
		require.True(t, hit, "the balance never printed a move; the -rebalance=false setup should have guaranteed one")
		r.recoverInterruptedBalance()
		r.verify("balance killed at move")
	})

	r.verify("final")
	t.Logf("interruption matrix done: %d payloads live, %d deleted, %d volumes tracked",
		len(r.payloads), len(r.deleted), len(r.volumes))
}

func trimMarker(m string) string {
	out := make([]rune, 0, len(m))
	for _, c := range m {
		if c == ' ' || c == ':' {
			continue
		}
		out = append(out, c)
	}
	return string(out)
}

// runShellKillAtMarker feeds "lock" plus the command to a weed shell
// subprocess, scans its combined output live, and kills the process the
// moment a line containing marker appears. Returns the captured output and
// whether the marker was seen before the shell exited on its own.
func runShellKillAtMarker(t *testing.T, ctx context.Context, command, marker string) (string, bool) {
	t.Helper()
	weedBinary := findWeedBinary()
	require.NotEmpty(t, weedBinary, "weed binary not found")
	cmd := exec.CommandContext(ctx, weedBinary, "shell", "-master="+chaosMasterAddr)

	pr, pw, err := os.Pipe()
	require.NoError(t, err)
	cmd.Stdout, cmd.Stderr = pw, pw
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	pw.Close() // the child keeps its dup; ours would hold the reader open

	var buf bytes.Buffer
	var mu sync.Mutex
	markerSeen := make(chan struct{})
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		signaled := false
		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			mu.Lock()
			buf.WriteString(line)
			buf.WriteByte('\n')
			mu.Unlock()
			if !signaled && bytesContains(line, marker) {
				signaled = true
				close(markerSeen)
			}
		}
	}()

	fmt.Fprintf(stdin, "lock\n%s\n", command)
	// Stdin stays open: the shell must die by kill, never by graceful EOF —
	// a graceful exit would release the cluster lock cleanly and dodge the
	// dead-holder cleanup path this test also exercises.
	hit := false
	select {
	case <-markerSeen:
		hit = true
	case <-scanDone: // shell exited before printing the marker
	case <-time.After(90 * time.Second):
	}
	if !hit {
		// A shell that printed the marker and exited immediately can make both
		// channels ready at once, and select picks between ready cases at
		// random; a printed marker must never be reported as missed.
		select {
		case <-markerSeen:
			hit = true
		default:
		}
	}
	cmd.Process.Kill()
	cmd.Wait()
	pr.Close()
	<-scanDone
	mu.Lock()
	defer mu.Unlock()
	return buf.String(), hit
}

func bytesContains(line, marker string) bool {
	return len(marker) > 0 && strings.Contains(line, marker)
}
