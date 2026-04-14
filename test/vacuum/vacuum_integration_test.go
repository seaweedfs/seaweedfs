package vacuum

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type TestCluster struct {
	masterCmd     *exec.Cmd
	volumeServers []*exec.Cmd
}

func (c *TestCluster) Stop() {
	for _, cmd := range c.volumeServers {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}
	if c.masterCmd != nil && c.masterCmd.Process != nil {
		c.masterCmd.Process.Kill()
		c.masterCmd.Wait()
	}
}

func startCluster(ctx context.Context, dataDir string) (*TestCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found - build with 'cd weed && go build' first")
	}

	cluster := &TestCluster{}

	masterDir := filepath.Join(dataDir, "master")
	os.MkdirAll(masterDir, 0755)

	// Empty security.toml to disable JWT in tests
	os.WriteFile(filepath.Join(dataDir, "security.toml"), []byte("# test\n"), 0644)

	// Start master
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", "9333",
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10",
		"-ip", "127.0.0.1",
	)
	masterCmd.Dir = dataDir
	masterLog, _ := os.Create(filepath.Join(masterDir, "master.log"))
	masterCmd.Stdout = masterLog
	masterCmd.Stderr = masterLog
	if err := masterCmd.Start(); err != nil {
		return nil, fmt.Errorf("start master: %v", err)
	}
	cluster.masterCmd = masterCmd
	time.Sleep(2 * time.Second)

	// Start 2 volume servers (enough for vacuum testing)
	for i := 0; i < 2; i++ {
		volumeDir := filepath.Join(dataDir, fmt.Sprintf("volume%d", i))
		os.MkdirAll(volumeDir, 0755)

		port := fmt.Sprintf("808%d", i)
		volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
			"-port", port,
			"-dir", volumeDir,
			"-max", "10",
			"-master", "127.0.0.1:9333",
			"-ip", "127.0.0.1",
		)
		volumeCmd.Dir = dataDir
		volumeLog, _ := os.Create(filepath.Join(volumeDir, "volume.log"))
		volumeCmd.Stdout = volumeLog
		volumeCmd.Stderr = volumeLog
		if err := volumeCmd.Start(); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("start volume server %d: %v", i, err)
		}
		cluster.volumeServers = append(cluster.volumeServers, volumeCmd)
	}

	time.Sleep(5 * time.Second)
	return cluster, nil
}

func findWeedBinary() string {
	candidates := []string{
		"../../weed/weed",
		"../weed/weed",
		"./weed",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			if abs, err := filepath.Abs(c); err == nil {
				return abs
			}
			return c
		}
	}
	if path, err := exec.LookPath("weed"); err == nil {
		return path
	}
	return ""
}

func waitForServer(address string, timeout time.Duration) error {
	start := time.Now()
	for time.Since(start) < timeout {
		if conn, err := net.DialTimeout("tcp", address, 1*time.Second); err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for server %s", address)
}

func uploadData(masterAddr, collection string, data []byte) (string, needle.VolumeId, error) {
	assignResult, err := operation.Assign(context.Background(), func(ctx context.Context) pb.ServerAddress {
		return pb.ServerAddress(masterAddr)
	}, grpc.WithInsecure(), &operation.VolumeAssignRequest{
		Count:      1,
		Collection: collection,
	})
	if err != nil {
		return "", 0, fmt.Errorf("assign: %v", err)
	}

	uploader, err := operation.NewUploader()
	if err != nil {
		return "", 0, fmt.Errorf("new uploader: %v", err)
	}

	uploadResult, err, _ := uploader.Upload(context.Background(), bytes.NewReader(data), &operation.UploadOption{
		UploadUrl: "http://" + assignResult.Url + "/" + assignResult.Fid,
		Filename:  "testfile.txt",
		MimeType:  "text/plain",
	})
	if err != nil {
		return "", 0, fmt.Errorf("upload: %v", err)
	}
	if uploadResult.Error != "" {
		return "", 0, fmt.Errorf("upload error: %s", uploadResult.Error)
	}

	fid, err := needle.ParseFileIdFromString(assignResult.Fid)
	if err != nil {
		return "", 0, err
	}
	return assignResult.Fid, fid.VolumeId, nil
}

func deleteFile(masterAddr string, fid string) error {
	results := operation.DeleteFileIds(func(ctx context.Context) pb.ServerAddress {
		return pb.ServerAddress(masterAddr)
	}, false, grpc.WithInsecure(), []string{fid})
	for _, r := range results {
		if r.Error != "" {
			return fmt.Errorf("delete %s: %s", fid, r.Error)
		}
	}
	return nil
}

func getGarbageRatio(volumeServerAddr string, volumeId uint32) (float64, error) {
	var ratio float64
	err := operation.WithVolumeServerClient(false, pb.ServerAddress(volumeServerAddr), grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			resp, err := client.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
				VolumeId: volumeId,
			})
			if err != nil {
				return err
			}
			ratio = resp.GarbageRatio
			return nil
		})
	return ratio, err
}

// TestVacuumIntegration tests the full vacuum flow:
// upload data → delete some → verify garbage → vacuum → verify cleanup
func TestVacuumIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDir := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	require.NoError(t, waitForServer("127.0.0.1:9333", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8080", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8081", 30*time.Second))

	masterAddr := "127.0.0.1:9333"
	collection := "vactest"

	// Upload files large enough that deleting most creates significant garbage.
	// With volumeSizeLimitMB=10, we need several MB of garbage to exceed the
	// 10% threshold passed to vacuum.
	const fileSize = 500 * 1024 // 500 KB per file
	const totalFiles = 16
	const filesToDelete = 12 // delete 75% → ~6 MB garbage out of ~8 MB

	var fids []string
	var payloads [][]byte
	var fileVolumes []needle.VolumeId
	for i := 0; i < totalFiles; i++ {
		data := bytes.Repeat([]byte{byte('A' + i%26)}, fileSize)
		fid, vid, err := uploadData(masterAddr, collection, data)
		require.NoError(t, err, "upload %d", i)
		fids = append(fids, fid)
		payloads = append(payloads, data)
		fileVolumes = append(fileVolumes, vid)
	}
	// Collect the set of volumes that will contain garbage after the deletes below.
	// The master may spread uploads across multiple volumes, so we cannot assume
	// a single volume id holds all the garbage.
	dirtyVolumesSet := map[needle.VolumeId]struct{}{}
	for i := 0; i < filesToDelete; i++ {
		dirtyVolumesSet[fileVolumes[i]] = struct{}{}
	}
	var dirtyVolumes []needle.VolumeId
	for v := range dirtyVolumesSet {
		dirtyVolumes = append(dirtyVolumes, v)
	}
	// Sort for deterministic log output and stable iteration order across runs.
	sort.Slice(dirtyVolumes, func(i, j int) bool { return dirtyVolumes[i] < dirtyVolumes[j] })
	t.Logf("Uploaded %d files (%d KB each) across volumes %v; will delete from volumes %v",
		totalFiles, fileSize/1024, fileVolumes, dirtyVolumes)

	// Wait for heartbeat to report sizes
	time.Sleep(6 * time.Second)

	// Delete most files to create garbage well above the threshold
	for i := 0; i < filesToDelete; i++ {
		err := deleteFile(masterAddr, fids[i])
		require.NoError(t, err, "delete %s", fids[i])
	}
	t.Logf("Deleted %d of %d files to create garbage", filesToDelete, totalFiles)

	// Wait for heartbeat to report deletions
	time.Sleep(6 * time.Second)

	// Verify garbage exists on every volume we deleted from.
	// Retry briefly in case heartbeats / deletions have not fully settled.
	// We require all dirty volumes to report garbage > threshold so that
	// the subsequent vacuum + cleanup check has a well-defined expectation
	// for every volume, not just the first one that happens to be ready.
	t.Run("verify_garbage_before_vacuum", func(t *testing.T) {
		deadline := time.Now().Add(20 * time.Second)
		var lastMissing needle.VolumeId
		for {
			ready := true
			for _, vid := range dirtyVolumes {
				volumeReady := false
				for _, addr := range []string{"127.0.0.1:8080", "127.0.0.1:8081"} {
					ratio, err := getGarbageRatio(addr, uint32(vid))
					if err != nil {
						continue
					}
					t.Logf("Garbage ratio for volume %d on %s: %.2f%%", vid, addr, ratio*100)
					if ratio > 0.1 {
						volumeReady = true
						break
					}
				}
				if !volumeReady {
					ready = false
					lastMissing = vid
					break
				}
			}
			if ready {
				return
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(1 * time.Second)
		}
		t.Fatalf("volume %d did not report garbage > 10%% — test data setup failed", lastMissing)
	})

	// Execute vacuum via shell command
	t.Run("run_vacuum", func(t *testing.T) {
		options := &shell.ShellOptions{
			Masters:        stringPtr(masterAddr),
			GrpcDialOption: grpc.WithInsecure(),
			FilerGroup:     stringPtr("default"),
		}
		commandEnv := shell.NewCommandEnv(options)

		shellCtx, shellCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer shellCancel()
		go commandEnv.MasterClient.KeepConnectedToMaster(shellCtx)
		commandEnv.MasterClient.WaitUntilConnected(shellCtx)
		time.Sleep(2 * time.Second)

		// Acquire lock (required by shell commands)
		locked, unlock := tryLock(t, commandEnv, 30*time.Second)
		require.True(t, locked, "could not acquire shell lock")
		defer unlock()

		// Find and execute vacuum command
		var output bytes.Buffer
		var found bool
		var err error
		for _, cmd := range shell.Commands {
			if cmd.Name() == "volume.vacuum" {
				err = cmd.Do(
					[]string{"-garbageThreshold", "0.1", "-collection", collection},
					commandEnv, &output,
				)
				found = true
				break
			}
		}
		require.True(t, found, "volume.vacuum command not found")
		t.Logf("Vacuum output: %s", output.String())
		require.NoError(t, err, "vacuum command failed")
		t.Log("Vacuum completed successfully")
	})

	// Verify garbage was cleaned on every volume we deleted from.
	// Vacuum + heartbeat reporting is asynchronous, so retry until each
	// volume reports a cleaned ratio or the deadline expires.
	t.Run("verify_cleanup_after_vacuum", func(t *testing.T) {
		deadline := time.Now().Add(30 * time.Second)
		remaining := map[needle.VolumeId]struct{}{}
		for _, vid := range dirtyVolumes {
			remaining[vid] = struct{}{}
		}
		failureReasons := map[needle.VolumeId]string{}
		for {
			for vid := range remaining {
				var volumeFound, cleanupVerified bool
				for _, addr := range []string{"127.0.0.1:8080", "127.0.0.1:8081"} {
					ratio, err := getGarbageRatio(addr, uint32(vid))
					if err != nil {
						continue
					}
					volumeFound = true
					t.Logf("Garbage ratio for volume %d after vacuum on %s: %.2f%%", vid, addr, ratio*100)
					if ratio < 0.05 {
						cleanupVerified = true
						break
					}
				}
				switch {
				case !volumeFound:
					failureReasons[vid] = fmt.Sprintf("no server reported volume %d after vacuum", vid)
				case !cleanupVerified:
					failureReasons[vid] = fmt.Sprintf("garbage on volume %d was not cleaned up after vacuum", vid)
				default:
					delete(remaining, vid)
					delete(failureReasons, vid)
				}
			}
			if len(remaining) == 0 {
				return
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(1 * time.Second)
		}
		stillFailing := make([]needle.VolumeId, 0, len(remaining))
		for vid := range remaining {
			stillFailing = append(stillFailing, vid)
		}
		sort.Slice(stillFailing, func(i, j int) bool { return stillFailing[i] < stillFailing[j] })
		msgs := make([]string, 0, len(stillFailing))
		for _, vid := range stillFailing {
			msgs = append(msgs, failureReasons[vid])
		}
		t.Fatalf("cleanup verification failed for %d volume(s): %s",
			len(stillFailing), strings.Join(msgs, "; "))
	})

	// Verify remaining files are still readable with correct contents
	t.Run("verify_remaining_data", func(t *testing.T) {
		for i := filesToDelete; i < totalFiles; i++ {
			fid := fids[i]
			expected := payloads[i]

			// Read file via HTTP from volume server
			client := &http.Client{Timeout: 5 * time.Second}
			url := fmt.Sprintf("http://127.0.0.1:8080/%s", fid)
			resp, err := client.Get(url)
			if err != nil || resp.StatusCode == http.StatusNotFound {
				if resp != nil {
					resp.Body.Close()
				}
				url = fmt.Sprintf("http://127.0.0.1:8081/%s", fid)
				resp, err = client.Get(url)
			}
			require.NoError(t, err, "read fid %s", fid)
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.NoError(t, err, "read body of fid %s", fid)
			require.Equal(t, http.StatusOK, resp.StatusCode, "fid %s returned %d", fid, resp.StatusCode)
			require.Equal(t, len(expected), len(body), "fid %s size mismatch", fid)
			require.True(t, bytes.Equal(expected, body), "fid %s content mismatch", fid)
			t.Logf("File %s verified (%d bytes)", fid, len(body))
		}
	})
}

func stringPtr(s string) *string {
	return &s
}

func tryLock(t *testing.T, commandEnv *shell.CommandEnv, timeout time.Duration) (locked bool, unlock func()) {
	t.Helper()
	type result struct {
		err error
	}
	done := make(chan result, 1)
	go func() {
		for _, cmd := range shell.Commands {
			if cmd.Name() == "lock" {
				var out bytes.Buffer
				done <- result{err: cmd.Do([]string{}, commandEnv, &out)}
				return
			}
		}
		done <- result{err: fmt.Errorf("lock command not found")}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			t.Logf("lock failed: %v", res.err)
			return false, nil
		}
		return true, func() {
			for _, cmd := range shell.Commands {
				if cmd.Name() == "unlock" {
					var out bytes.Buffer
					cmd.Do([]string{}, commandEnv, &out)
					return
				}
			}
		}
	case <-time.After(timeout):
		t.Log("lock timed out")
		return false, nil
	}
}
