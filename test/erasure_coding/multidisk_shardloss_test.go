package erasure_coding

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestMultiDiskECBalanceNoShardLoss is the end-to-end regression for issue 9593.
// It runs a real cluster of multi-disk volume servers (3 servers x 4 disks),
// EC-encodes a volume, then runs ec.balance, asserting hard invariants the older
// integration tests only logged:
//
//   - after encode the full set of 14 EC shards exists,
//   - ec.balance never loses a shard (still 14 distinct shards afterwards),
//   - shards end up spread across more than one disk per node, and
//   - cluster.status counts physical disks (not one per node) and matches the
//     real on-disk distribution.
func TestMultiDiskECBalanceNoShardLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-disk EC integration test in short mode")
	}

	testDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
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

	// Upload enough small files that the volume holds real data to encode.
	var volumeId needle.VolumeId
	for retry := 0; retry < 5; retry++ {
		volumeId, err = uploadTestDataToMaster([]byte(strings.Repeat("multidisk-ec-9593 ", 64)), "127.0.0.1:9334")
		if err == nil {
			break
		}
		time.Sleep(3 * time.Second)
	}
	require.NoError(t, err, "failed to upload test data")
	for i := 0; i < 40; i++ {
		if _, e := uploadTestDataToMaster([]byte(strings.Repeat("filler ", 128)), "127.0.0.1:9334"); e != nil {
			break
		}
	}
	t.Logf("using volume %d", volumeId)
	time.Sleep(3 * time.Second)

	// Populate every server's disks with volumes so the encode can see and target
	// each physical disk. The master only enumerates disks that already hold a
	// volume or EC shard — an empty disk leaves no trace in the topology (heartbeats
	// aggregate capacity per disk type, not per physical disk). ec.encode therefore
	// spreads a volume's shards only across the disks the master already knows hold
	// data on each node; if a node's data sits on a single disk, all its shards land
	// there and ec.balance cannot redistribute them (it has no within-node
	// cross-disk move). So spreading must be set up before encoding.
	//
	// volume.grow only tops up toward a writable target and stops on the first
	// allocation error, so a single -count grow can create far fewer volumes than
	// asked and leave a node on one disk. Grow repeatedly on the nodes that have not
	// spread yet (the volume server places each new volume on its least-loaded disk)
	// until the master's topology shows every node holding volumes on at least two
	// physical disks. This makes the multi-disk layout — and thus the post-encode
	// disk spread — deterministic instead of racing volume-growth and heartbeat.
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
	}, 60*time.Second, 2*time.Second,
		"volumes never spread across >=2 disks on all 3 nodes")

	locked, unlock := tryLockWithTimeout(t, commandEnv, 15*time.Second)
	require.True(t, locked, "could not acquire shell lock")
	defer unlock()

	// EC-encode the volume.
	out, err := captureCommandOutput(t, shell.Commands[findCommandIndex("ec.encode")],
		[]string{"-volumeId", fmt.Sprintf("%d", volumeId), "-collection", "test", "-force"}, commandEnv)
	t.Logf("ec.encode output:\n%s", out)
	require.NoError(t, err, "ec.encode failed")

	// All 14 shards must exist after encoding.
	require.Eventually(t, func() bool {
		return len(collectDistinctShardIDs(testDir, uint32(volumeId))) == erasureShardCount
	}, 30*time.Second, time.Second, "expected all %d EC shards after encode, got %v",
		erasureShardCount, collectDistinctShardIDs(testDir, uint32(volumeId)))

	beforeBalance := collectDistinctShardIDs(testDir, uint32(volumeId))
	t.Logf("after encode: %d distinct shards on %d disks", len(beforeBalance), disksWithShards(testDir, uint32(volumeId)))

	// Run ec.balance.
	out, err = captureCommandOutput(t, shell.Commands[findCommandIndex("ec.balance")],
		[]string{"-collection", "test", "-force"}, commandEnv)
	t.Logf("ec.balance output:\n%s", out)
	require.NoError(t, err, "ec.balance failed")
	time.Sleep(3 * time.Second)

	// The core regression: ec.balance must not lose any shard.
	afterBalance := collectDistinctShardIDs(testDir, uint32(volumeId))
	require.Equal(t, erasureShardCount, len(afterBalance),
		"ec.balance lost shards on multi-disk nodes: had %v, now %v", sortedKeysOf(beforeBalance), sortedKeysOf(afterBalance))

	// Shards must be spread across more than one physical disk per node overall.
	usedDisks := disksWithShards(testDir, uint32(volumeId))
	assert.Greater(t, usedDisks, 3, "EC shards should span more than one disk per node (got %d disks across 3 nodes)", usedDisks)

	// cluster.status must count physical disks, not collapse to one per node: it
	// must report at least the disks actually holding this volume's shards (which
	// is already >3 across the 3 nodes). Before the fix it reported 3 (node count).
	require.Eventually(t, func() bool {
		n, ok := clusterStatusDiskCount(t, commandEnv)
		return ok && n >= usedDisks
	}, 30*time.Second, 2*time.Second, "cluster.status never reported the >=%d physical disks holding shards (multi-disk count)", usedDisks)

	n, _ := clusterStatusDiskCount(t, commandEnv)
	t.Logf("cluster.status reports %d physical disks (>= %d holding this volume's shards)", n, usedDisks)
}

const erasureShardCount = 14 // 10 data + 4 parity

// collectDistinctShardIDs returns the set of EC shard ids present for a volume
// across every disk of every server in the multi-disk test layout.
func collectDistinctShardIDs(testDir string, volumeId uint32) map[int]bool {
	ids := map[int]bool{}
	for server := 0; server < 3; server++ {
		for disk := 0; disk < 4; disk++ {
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
				if n, err := strconv.Atoi(f[i+3:]); err == nil && n >= 0 && n < erasureShardCount {
					ids[n] = true
				}
			}
		}
	}
	return ids
}

// disksWithShards counts how many physical disks hold at least one shard.
func disksWithShards(testDir string, volumeId uint32) int {
	n := 0
	for _, disks := range countShardsPerDisk(testDir, volumeId) {
		for _, c := range disks {
			if c > 0 {
				n++
			}
		}
	}
	return n
}

// nodeVolumeDiskCounts returns, per volume server id, how many distinct physical
// disks hold at least one volume according to the master's topology. The master
// only enumerates disks that already hold a volume or EC shard (heartbeats
// aggregate capacity per disk type, not per physical disk), so this reports the
// disks ec.encode can actually spread a volume's shards across on each node.
func nodeVolumeDiskCounts(t *testing.T, commandEnv *shell.CommandEnv) map[string]int {
	t.Helper()
	var resp *master_pb.VolumeListResponse
	err := commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		var e error
		resp, e = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return e
	})
	counts := map[string]int{}
	if err != nil || resp.GetTopologyInfo() == nil {
		return counts
	}
	for _, dc := range resp.GetTopologyInfo().GetDataCenterInfos() {
		for _, r := range dc.GetRackInfos() {
			for _, dn := range r.GetDataNodeInfos() {
				disks := map[uint32]bool{}
				for _, di := range dn.GetDiskInfos() {
					for _, vi := range di.GetVolumeInfos() {
						disks[vi.GetDiskId()] = true
					}
				}
				counts[dn.Id] = len(disks)
			}
		}
	}
	return counts
}

func allAtLeast(counts map[string]int, min int) bool {
	for _, c := range counts {
		if c < min {
			return false
		}
	}
	return true
}

var diskCountRe = regexp.MustCompile(`(\d+)\s+disks?`)

// clusterStatusDiskCount runs cluster.status and parses the reported disk count.
func clusterStatusDiskCount(t *testing.T, commandEnv *shell.CommandEnv) (int, bool) {
	t.Helper()
	out, err := captureCommandOutput(t, shell.Commands[findCommandIndex("cluster.status")], []string{}, commandEnv)
	if err != nil {
		return 0, false
	}
	m := diskCountRe.FindStringSubmatch(out)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	return n, err == nil
}

func sortedKeysOf(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
