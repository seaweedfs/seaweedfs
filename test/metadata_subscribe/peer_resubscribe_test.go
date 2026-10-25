//go:build !windows

package metadata_subscribe

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// A filer drops the metadata subscription to a peer that leaves, and only an
// add from the master brings it back. Those updates are broadcast to the
// clients connected at that moment, so a filer whose master stream broke while
// the peer came back used to stay unsubscribed for good, and metadata written
// on the peer never reached it again.
func TestFilerResubscribesToPeerAfterMasterReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	weedBinary := findWeedBinary()
	require.NotEmpty(t, weedBinary, "weed binary not found")

	testDir, err := os.MkdirTemp("", "seaweedfs_peer_resubscribe_")
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
	filer2Address := fmt.Sprintf("127.0.0.1:%d", filer2Port)
	filer1Log := filepath.Join(testDir, "filer1.log")

	masterArgs := []string{"master",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(masterPort),
		"-mdir=" + mkdir(t, testDir, "master"),
		"-peers=none"}
	masterProcess := startProcess(t, weedBinary, filepath.Join(testDir, "master.log"), masterArgs...)
	require.NoError(t, waitForLeader(masterPort, 60*time.Second))

	// one at a time: a filer bootstraps from the peers the master already knows,
	// and gives up if one of them is registered but not yet listening
	filer1 := startFiler(t, weedBinary, testDir, "filer1", filer1Port, masterPort)
	require.NoError(t, waitForHTTPServer(fmt.Sprintf("http://127.0.0.1:%d/", filer1Port), 30*time.Second))
	filer2 := startFiler(t, weedBinary, testDir, "filer2", filer2Port, masterPort)
	require.NoError(t, waitForHTTPServer(fmt.Sprintf("http://127.0.0.1:%d/", filer2Port), 30*time.Second))

	// the peer subscription works to begin with
	createPeerEntry(t, filer2Address, "baseline")
	require.NoError(t, waitForPeerEntry(filer1Address, "baseline", 60*time.Second),
		"filer1 never replicated the baseline entry from filer2")

	// filer2 leaves, and filer1 drops the subscription
	stopProcess(filer2)
	require.NoError(t, waitForLog(filer1Log, "stop subscribing peer "+filer2Address, 60*time.Second),
		"filer1 never dropped the subscription to filer2")

	// filer1 stops reading its master stream, and restarting the master breaks
	// it, so filer1 hears nothing until it reconnects
	require.NoError(t, filer1.Process.Signal(syscall.SIGSTOP))
	stopProcess(masterProcess)
	startProcess(t, weedBinary, filepath.Join(testDir, "master.log"), masterArgs...)
	require.NoError(t, waitForLeader(masterPort, 60*time.Second))

	// filer2 comes back and registers while filer1 cannot hear about it
	filer2 = startFiler(t, weedBinary, testDir, "filer2", filer2Port, masterPort)
	require.NoError(t, waitForHTTPServer(fmt.Sprintf("http://127.0.0.1:%d/", filer2Port), 30*time.Second))
	require.NoError(t, waitForClusterNode(master, filer2Address, 60*time.Second),
		"the master never registered filer2 again")

	require.NoError(t, filer1.Process.Signal(syscall.SIGCONT))
	require.NoError(t, waitForClusterNode(master, filer1Address, 60*time.Second),
		"filer1 never reconnected to the master")

	createPeerEntry(t, filer2Address, "after-reconnect")
	require.NoError(t, waitForPeerEntry(filer1Address, "after-reconnect", 90*time.Second),
		"filer1 reconnected to the master but never resubscribed to filer2")
}

const peerEntryDir = "/peer-resubscribe"

func createPeerEntry(t *testing.T, filerAddress, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := pb.WithFilerClient(false, 0, pb.ServerAddress(filerAddress), grpc.WithTransportCredentials(insecure.NewCredentials()), func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: peerEntryDir,
			Entry: &filer_pb.Entry{
				Name: name,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					FileMode: 0644,
				},
			},
		})
		return err
	})
	require.NoError(t, err, "create %s/%s on %s", peerEntryDir, name, filerAddress)
}

func waitForPeerEntry(filerAddress, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lastErr = pb.WithFilerClient(false, 0, pb.ServerAddress(filerAddress), grpc.WithTransportCredentials(insecure.NewCredentials()), func(client filer_pb.SeaweedFilerClient) error {
			_, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
				Directory: peerEntryDir,
				Name:      name,
			})
			return err
		})
		cancel()
		if lastErr == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("%s/%s not on %s within %v: %w", peerEntryDir, name, filerAddress, timeout, lastErr)
}

func waitForClusterNode(master pb.ServerAddress, address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		found := false
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pb.WithMasterClient(ctx, false, master, grpc.WithTransportCredentials(insecure.NewCredentials()), false, func(client master_pb.SeaweedClient) error {
			resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{ClientType: cluster.FilerType})
			if err != nil {
				return err
			}
			for _, node := range resp.ClusterNodes {
				// the master reports the grpc port too, as "host:port.grpcPort"
				if pb.ServerAddress(node.Address).Equals(pb.ServerAddress(address)) {
					found = true
				}
			}
			return nil
		})
		cancel()
		if err == nil && found {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("%s not registered within %v", address, timeout)
}

func waitForLeader(masterPort int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://127.0.0.1:%d/cluster/status", masterPort)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		if resp, err := client.Get(url); err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if strings.Contains(string(body), `"IsLeader":true`) {
				return nil
			}
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("master on %d has no leader within %v", masterPort, timeout)
}

func waitForLog(logFile, message string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		content, err := os.ReadFile(logFile)
		if err == nil && strings.Contains(string(content), message) {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("%q not in %s within %v", message, logFile, timeout)
}

func mkdir(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(path, 0755))
	return path
}

func startFiler(t *testing.T, weedBinary, testDir, name string, port, masterPort int) *exec.Cmd {
	t.Helper()
	return startProcess(t, weedBinary, filepath.Join(testDir, name+".log"), "filer",
		"-ip=127.0.0.1",
		"-port="+strconv.Itoa(port),
		"-master=127.0.0.1:"+strconv.Itoa(masterPort),
		"-defaultStoreDir="+mkdir(t, testDir, name))
}

func startProcess(t *testing.T, weedBinary, logFile string, args ...string) *exec.Cmd {
	t.Helper()
	log, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)

	cmd := exec.Command(weedBinary, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Signal(syscall.SIGCONT)
		stopProcess(cmd)
	})
	return cmd
}

// stopProcess kills the process outright: a filer takes seconds to shut down
// gracefully, long enough to register with the master again on the way out.
func stopProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	cmd.Process.Kill()
	cmd.Wait()
}
