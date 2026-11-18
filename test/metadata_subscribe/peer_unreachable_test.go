//go:build !windows

package metadata_subscribe

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A peer removal is broadcast only to the filers connected to the master that
// saw the peer go. A peer that dies while no master is up (or across a leader
// change) is never removed: the filer keeps retrying its address, the peer's
// delivery watermark stays at zero, and every aggregated subscriber of that
// filer is held at the settled horizon (2 x LogFlushInterval) for good.
func TestAggregatedSubscriberNotHeldByPeerThatDiedUnnoticed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	weedBinary := findWeedBinary()
	require.NotEmpty(t, weedBinary, "weed binary not found")

	testDir, err := os.MkdirTemp("", "seaweedfs_peer_unreachable_")
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("logs kept at %s", testDir)
			return
		}
		os.RemoveAll(testDir)
	})

	ports, err := testutil.AllocateMiniPorts(3)
	require.NoError(t, err)
	masterPort, filer1Port, filer2Port := ports[0], ports[1], ports[2]
	master := pb.ServerAddress(fmt.Sprintf("127.0.0.1:%d", masterPort))
	filer1Address := fmt.Sprintf("127.0.0.1:%d", filer1Port)

	masterArgs := []string{"master",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(masterPort),
		"-mdir=" + mkdir(t, testDir, "master"),
		"-peers=none"}
	masterProcess := startProcess(t, weedBinary, filepath.Join(testDir, "master.log"), masterArgs...)
	require.NoError(t, waitForLeader(masterPort, 60*time.Second))
	startFiler(t, weedBinary, testDir, "filer1", filer1Port, masterPort)
	require.NoError(t, waitForHTTPServer(fmt.Sprintf("http://127.0.0.1:%d/", filer1Port), 30*time.Second))
	filer2 := startFiler(t, weedBinary, testDir, "filer2", filer2Port, masterPort)
	require.NoError(t, waitForHTTPServer(fmt.Sprintf("http://127.0.0.1:%d/", filer2Port), 30*time.Second))
	filer2Address := fmt.Sprintf("127.0.0.1:%d", filer2Port)

	// filer1 follows filer2: an entry created on filer2 reaches filer1
	createPeerEntry(t, filer2Address, "from-filer2")
	require.NoError(t, waitForPeerEntry(filer1Address, "from-filer2", 60*time.Second),
		"filer1 never followed filer2")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan *filer_pb.SubscribeMetadataResponse, 1000)
	sinceNs := time.Now().UnixNano()
	go subscribeToMetadataWithOptions(ctx, filer1Address, peerEntryDir, sinceNs, events)
	arrived := make(map[string]time.Time)
	waitArrival := func(name string, within time.Duration) bool {
		deadline := time.After(within)
		for {
			if _, ok := arrived[name]; ok {
				return true
			}
			select {
			case ev := <-events:
				if n := ev.GetEventNotification().GetNewEntry(); n != nil {
					arrived[n.Name] = time.Now()
				}
			case <-deadline:
				return false
			}
		}
	}

	createPeerEntry(t, filer1Address, "baseline")
	require.True(t, waitArrival("baseline", 20*time.Second), "the baseline event never reached the subscriber")

	// filer2 dies while no master is up, so no removal is ever broadcast
	stopProcess(masterProcess)
	stopProcess(filer2)
	startProcess(t, weedBinary, filepath.Join(testDir, "master.log"), masterArgs...)
	require.NoError(t, waitForLeader(masterPort, 60*time.Second))
	require.NoError(t, waitForClusterNode(master, filer1Address, 60*time.Second))
	diedAt := time.Now()

	// Unpatched, every event from here on arrives 2 x LogFlushInterval late.
	// The dead peer may hold reads for its unreachable bound plus the removal
	// grace (2 x LogFlushInterval each); after that delivery must be prompt.
	for i := 0; ; i++ {
		name := fmt.Sprintf("probe-%d", i)
		createPeerEntry(t, filer1Address, name)
		if waitArrival(name, 10*time.Second) {
			t.Logf("%s delivered promptly %v after the peer died", name, time.Since(diedAt).Round(time.Second))
			return
		}
		if time.Since(diedAt) > 6*time.Minute {
			t.Fatalf("aggregated delivery still held %v after the peer died unnoticed", time.Since(diedAt).Round(time.Second))
		}
	}
}
