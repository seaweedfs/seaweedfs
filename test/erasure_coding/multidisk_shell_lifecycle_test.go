package erasure_coding

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// TestMultiDiskECShellLifecycle drives the real shell commands — ec.encode,
// ec.balance, ec.rebuild — against a live multi-disk cluster, through the
// sequence a production repository goes through, with one invariant checked
// after every step: the bytes a client stored come back identical. Shard
// counting alone cannot tell a healthy volume from one that was rebuilt out
// of the wrong inputs; reading the payload back can.
//
// The damage step reproduces a support case: shard files removed from disk
// while the cluster still lists them, so the subsequent ec.rebuild sees copy
// failures ("CopyFile not found ec volume id ...") for locations the master
// believes exist, and must recover from the shards that are really there.
func TestMultiDiskECShellLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-disk EC shell lifecycle test in short mode")
	}

	testDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
	defer cancel()

	cluster, err := startMultiDiskCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	require.NoError(t, waitForServer("127.0.0.1:9334", 30*time.Second))
	for i := 0; i < 3; i++ {
		require.NoError(t, waitForServer(fmt.Sprintf("127.0.0.1:809%d", i), 30*time.Second))
	}
	t.Log("waiting for multi-disk volume servers to register...")
	time.Sleep(10 * time.Second)

	commandEnv := shell.NewCommandEnv(&shell.ShellOptions{
		Masters:        stringPtr("127.0.0.1:9334"),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	})
	connectToMasterAndSync(ctx, t, commandEnv)

	// A payload with no structure, so a rebuild that assembled the wrong bytes
	// cannot accidentally reproduce it.
	payload := make([]byte, 8192)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	var volumeId needle.VolumeId
	var fid string
	for retry := 0; retry < 5; retry++ {
		volumeId, fid, err = uploadPayload(payload)
		if err == nil {
			break
		}
		time.Sleep(3 * time.Second)
	}
	require.NoError(t, err, "failed to upload payload")
	for i := 0; i < 40; i++ {
		if _, _, e := uploadPayload([]byte(strings.Repeat("filler ", 128))); e != nil {
			break
		}
	}
	t.Logf("payload of %d bytes in volume %d as %s", len(payload), volumeId, fid)
	time.Sleep(3 * time.Second)

	requirePayload := func(step string) {
		t.Helper()
		require.Eventually(t, func() bool {
			got, err := readFid(fid, uint32(volumeId))
			return err == nil && bytes.Equal(got, payload)
		}, 30*time.Second, time.Second, "payload not readable byte-identical after %s", step)
		t.Logf("payload verified after %s", step)
	}
	requirePayload("upload")

	// Spread volumes across each node's disks before encoding; the master only
	// enumerates disks that already hold data, and shards only spread across
	// enumerated disks (see TestMultiDiskECBalanceNoShardLoss for the details).
	require.Eventually(t, func() bool {
		spread := nodeVolumeDiskCounts(t, commandEnv)
		if len(spread) == 3 && allAtLeast(spread, 2) {
			return true
		}
		for i := 0; i < 3; i++ {
			server := fmt.Sprintf("127.0.0.1:809%d", i)
			if spread[server] < 2 {
				captureCommandOutput(t, shell.Commands[findCommandIndex("volume.grow")],
					[]string{"-collection", "test", "-dataNode", server, "-count", "4"}, commandEnv)
			}
		}
		return false
	}, 60*time.Second, 2*time.Second, "volumes never spread across >=2 disks on all 3 nodes")

	locked, unlock := tryLockWithTimeout(t, commandEnv, 15*time.Second)
	require.True(t, locked, "could not acquire shell lock")
	defer unlock()

	// ── ec.encode ──
	out, err := captureCommandOutput(t, shell.Commands[findCommandIndex("ec.encode")],
		[]string{"-volumeId", fmt.Sprintf("%d", volumeId), "-collection", "test", "-force"}, commandEnv)
	t.Logf("ec.encode output:\n%s", out)
	require.NoError(t, err, "ec.encode failed")
	require.Eventually(t, func() bool {
		return len(collectDistinctShardIDs(testDir, uint32(volumeId))) == erasureShardCount
	}, 30*time.Second, time.Second, "expected all %d shards after encode", erasureShardCount)
	requirePayload("ec.encode")

	// ── ec.balance ──
	out, err = captureCommandOutput(t, shell.Commands[findCommandIndex("ec.balance")],
		[]string{"-collection", "test", "-force"}, commandEnv)
	t.Logf("ec.balance output:\n%s", out)
	require.NoError(t, err, "ec.balance failed")
	time.Sleep(3 * time.Second)
	require.Len(t, collectDistinctShardIDs(testDir, uint32(volumeId)), erasureShardCount,
		"ec.balance lost shards")
	requirePayload("ec.balance")

	// ── damage: two shard files vanish from disk ──
	// Removed straight off disk, then the servers are restarted so the master
	// relearns the shard set from disk rather than from stale heartbeat state.
	// This is the recoverable form of the support case: the volume drops to 12
	// of 14 shards, which is still above the 10 needed to read and rebuild.
	removed := removeTwoShardFiles(t, testDir, uint32(volumeId))
	t.Logf("removed shard files for shards %v", removed)
	require.Len(t, collectDistinctShardIDs(testDir, uint32(volumeId)), erasureShardCount-2)

	require.NoError(t, cluster.RestartVolumeServers(ctx))
	for i := 0; i < 3; i++ {
		require.NoError(t, waitForServer(fmt.Sprintf("127.0.0.1:809%d", i), 30*time.Second))
	}
	requirePayload("shard loss + restart") // 12 of 14 remain; reads must survive

	// The master must now see exactly the 12 shards on disk before any repair
	// runs. ec.rebuild is driven by the master's view, so a repair planned
	// while that view still lists the two dead shards would target the wrong
	// set. Restart-then-relearn is what makes disk truth and the master agree.
	require.Eventually(t, func() bool {
		return len(masterEcShardIds(commandEnv, uint32(volumeId))) == erasureShardCount-2
	}, 60*time.Second, 2*time.Second,
		"master never relearned the reduced shard set (got %v)",
		sortedKeysOf(masterEcShardIds(commandEnv, uint32(volumeId))))

	// The shell lock is dropped by the restart's master disconnect; retake it.
	relocked, reunlock := tryLockWithTimeout(t, commandEnv, 15*time.Second)
	require.True(t, relocked, "could not reacquire shell lock after restart")
	defer reunlock()

	// ── ec.rebuild, the command from the support case ──
	out, err = captureCommandOutput(t, shell.Commands[findCommandIndex("ec.rebuild")],
		[]string{"-collection", "test", "-apply"}, commandEnv)
	t.Logf("ec.rebuild output:\n%s", out)
	require.NoError(t, err, "ec.rebuild failed")
	require.Eventually(t, func() bool {
		return len(collectDistinctShardIDs(testDir, uint32(volumeId))) == erasureShardCount
	}, 60*time.Second, time.Second, "ec.rebuild did not restore all %d shards on disk", erasureShardCount)
	requirePayload("ec.rebuild")

	// The rebuilt shards are real: master's view and disk truth agree on all 14.
	require.Eventually(t, func() bool {
		registered := masterEcShardIds(commandEnv, uint32(volumeId))
		onDisk := collectDistinctShardIDs(testDir, uint32(volumeId))
		if len(registered) != len(onDisk) || len(onDisk) != erasureShardCount {
			return false
		}
		for id := range onDisk {
			if !registered[id] {
				return false
			}
		}
		return true
	}, 60*time.Second, 2*time.Second,
		"master's EC shard view diverged from disk truth after rebuild: master=%v disk=%v",
		sortedKeysOf(masterEcShardIds(commandEnv, uint32(volumeId))), sortedKeysOf(collectDistinctShardIDs(testDir, uint32(volumeId))))
}

// RestartVolumeServers kills every volume server process and starts fresh ones
// over the same directories, so registration is rebuilt purely from disk.
func (c *MultiDiskCluster) RestartVolumeServers(ctx context.Context) error {
	for _, cmd := range c.volumeServers {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}
	c.volumeServers = nil
	time.Sleep(2 * time.Second)
	return c.startVolumeServers(ctx)
}

// startVolumeServers launches the standard 3-server x 4-disk layout over the
// cluster's existing directories, mirroring startMultiDiskCluster's loop so a
// restart brings servers back exactly as they first started.
func (c *MultiDiskCluster) startVolumeServers(ctx context.Context) error {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return fmt.Errorf("weed binary not found")
	}
	const numServers = 3
	const disksPerServer = 4
	for i := 0; i < numServers; i++ {
		var diskDirs []string
		var maxVolumes []string
		for d := 0; d < disksPerServer; d++ {
			diskDirs = append(diskDirs, filepath.Join(c.testDir, fmt.Sprintf("server%d_disk%d", i, d)))
			maxVolumes = append(maxVolumes, "5")
		}
		volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
			"-port", fmt.Sprintf("809%d", i),
			"-dir", strings.Join(diskDirs, ","),
			"-max", strings.Join(maxVolumes, ","),
			"-master", "127.0.0.1:9334",
			"-ip", "127.0.0.1",
			"-dataCenter", "dc1",
			"-rack", fmt.Sprintf("rack%d", i),
		)
		logFile, err := os.OpenFile(filepath.Join(c.testDir, fmt.Sprintf("server%d_logs", i), "volume-restart.log"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("open restart log for server %d: %w", i, err)
		}
		c.logFiles = append(c.logFiles, logFile)
		volumeCmd.Stdout = logFile
		volumeCmd.Stderr = logFile
		if err := volumeCmd.Start(); err != nil {
			return fmt.Errorf("restart volume server %d: %w", i, err)
		}
		c.volumeServers = append(c.volumeServers, volumeCmd)
	}
	time.Sleep(8 * time.Second)
	return nil
}

// uploadPayload stores data in collection "test" and returns the volume id and
// fid so the same needle can be read back after every subsequent operation.
func uploadPayload(data []byte) (needle.VolumeId, string, error) {
	assignResult, err := operation.Assign(context.Background(), func(ctx context.Context) pb.ServerAddress {
		return pb.ServerAddress("127.0.0.1:9334")
	}, grpc.WithInsecure(), &operation.VolumeAssignRequest{
		Count:       1,
		Collection:  "test",
		Replication: "000",
	})
	if err != nil {
		return 0, "", err
	}
	uploader, err := operation.NewUploader()
	if err != nil {
		return 0, "", err
	}
	uploadResult, err, _ := uploader.Upload(context.Background(), bytes.NewReader(data), &operation.UploadOption{
		UploadUrl: "http://" + assignResult.Url + "/" + assignResult.Fid,
		Filename:  "payload.bin",
		MimeType:  "application/octet-stream",
	})
	if err != nil {
		return 0, "", err
	}
	if uploadResult.Error != "" {
		return 0, "", fmt.Errorf("upload error: %s", uploadResult.Error)
	}
	fidObj, err := needle.ParseFileIdFromString(assignResult.Fid)
	if err != nil {
		return 0, "", err
	}
	return fidObj.VolumeId, assignResult.Fid, nil
}

// readFid locates the volume via the master and fetches the needle bytes from
// a currently-registered location — the same route a client read takes, for a
// plain volume before encoding and for the EC read path after.
func readFid(fid string, volumeId uint32) ([]byte, error) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:9334/dir/lookup?volumeId=%d", volumeId))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var lookup struct {
		Locations []struct {
			Url string `json:"url"`
		} `json:"locations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&lookup); err != nil {
		return nil, err
	}
	if len(lookup.Locations) == 0 {
		return nil, fmt.Errorf("no locations for volume %d", volumeId)
	}
	var lastErr error
	for _, loc := range lookup.Locations {
		get, err := http.Get(fmt.Sprintf("http://%s/%s", loc.Url, fid))
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(get.Body)
		get.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if get.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("GET %s from %s: %d", fid, loc.Url, get.StatusCode)
			continue
		}
		return body, nil
	}
	return nil, lastErr
}

// removeTwoShardFiles deletes the files of two distinct shards straight off
// the disks, without telling any server — the divergence between disk truth
// and the master's view at the heart of the support case.
func removeTwoShardFiles(t *testing.T, testDir string, volumeId uint32) []int {
	t.Helper()
	var removed []int
	for server := 0; server < 3 && len(removed) < 2; server++ {
		for disk := 0; disk < 4 && len(removed) < 2; disk++ {
			diskDir := filepath.Join(testDir, fmt.Sprintf("server%d_disk%d", server, disk))
			files, err := listECShardFiles(diskDir, volumeId)
			if err != nil {
				continue
			}
			for _, f := range files {
				i := strings.LastIndex(f, ".ec")
				if i < 0 {
					continue
				}
				var id int
				if _, err := fmt.Sscanf(f[i+3:], "%d", &id); err != nil {
					continue
				}
				if err := os.Remove(filepath.Join(diskDir, f)); err != nil {
					t.Fatalf("remove shard file %s: %v", f, err)
				}
				removed = append(removed, id)
				break // at most one shard per disk, keep the loss spread out
			}
		}
	}
	require.Len(t, removed, 2, "could not find two shard files to remove")
	return removed
}

// masterEcShardIds reports which shard ids the master currently believes exist
// for the volume, across all nodes — the view every repair decision runs on.
func masterEcShardIds(commandEnv *shell.CommandEnv, volumeId uint32) map[int]bool {
	ids := map[int]bool{}
	var resp *master_pb.VolumeListResponse
	err := commandEnv.MasterClient.WithClient(context.Background(), false, func(client master_pb.SeaweedClient) error {
		var e error
		resp, e = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return e
	})
	if err != nil || resp.GetTopologyInfo() == nil {
		return ids
	}
	for _, dc := range resp.GetTopologyInfo().GetDataCenterInfos() {
		for _, r := range dc.GetRackInfos() {
			for _, dn := range r.GetDataNodeInfos() {
				for _, di := range dn.GetDiskInfos() {
					for _, eci := range di.GetEcShardInfos() {
						if eci.GetId() != volumeId {
							continue
						}
						for _, sid := range erasure_coding_ShardIds(eci.GetEcIndexBits()) {
							ids[sid] = true
						}
					}
				}
			}
		}
	}
	return ids
}

// erasure_coding_ShardIds expands an EC index bitmap into shard ids without
// importing the storage package into this test's namespace twice.
func erasure_coding_ShardIds(bits uint32) []int {
	var out []int
	for i := 0; i < 32; i++ {
		if bits&(1<<uint(i)) != 0 {
			out = append(out, i)
		}
	}
	return out
}
