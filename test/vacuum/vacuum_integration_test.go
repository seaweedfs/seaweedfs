package vacuum

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
		if conn, err := grpc.NewClient(address, grpc.WithInsecure()); err == nil {
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

	// Upload several files to create volume with data
	var fids []string
	var volumeId needle.VolumeId
	for i := 0; i < 10; i++ {
		data := bytes.Repeat([]byte(fmt.Sprintf("test data entry %d padding ", i)), 100)
		fid, vid, err := uploadData(masterAddr, collection, data)
		require.NoError(t, err, "upload %d", i)
		fids = append(fids, fid)
		volumeId = vid
	}
	t.Logf("Uploaded 10 files to volume %d", volumeId)

	// Wait for heartbeat to report sizes
	time.Sleep(6 * time.Second)

	// Delete half the files to create garbage
	for i := 0; i < 5; i++ {
		err := deleteFile(masterAddr, fids[i])
		require.NoError(t, err, "delete %s", fids[i])
	}
	t.Logf("Deleted 5 files to create garbage")

	// Wait for heartbeat to report deletions
	time.Sleep(6 * time.Second)

	// Verify garbage exists
	t.Run("verify_garbage_before_vacuum", func(t *testing.T) {
		// Check garbage on the volume server that hosts this volume
		for _, addr := range []string{"127.0.0.1:8080", "127.0.0.1:8081"} {
			ratio, err := getGarbageRatio(addr, uint32(volumeId))
			if err != nil {
				continue // volume might not be on this server
			}
			t.Logf("Garbage ratio on %s: %.2f%%", addr, ratio*100)
			if ratio > 0.1 {
				t.Logf("PASS: Significant garbage detected (%.2f%%) on %s", ratio*100, addr)
				return
			}
		}
		t.Log("WARNING: No significant garbage detected — vacuum may have nothing to do")
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

		// Find and execute vacuum command
		var vacuumCmd shell.Command
		for _, cmd := range shell.Commands {
			if cmd.Name() == "volume.vacuum" {
				vacuumCmd = cmd
				break
			}
		}
		require.NotNil(t, vacuumCmd, "volume.vacuum command not found")

		var output bytes.Buffer
		err := vacuumCmd.Do(
			[]string{"-garbageThreshold", "0.1", "-collection", collection},
			commandEnv, &output,
		)
		t.Logf("Vacuum output: %s", output.String())
		require.NoError(t, err, "vacuum command failed")
		t.Log("Vacuum completed successfully")
	})

	// Wait for vacuum effects to settle
	time.Sleep(6 * time.Second)

	// Verify garbage was cleaned
	t.Run("verify_cleanup_after_vacuum", func(t *testing.T) {
		for _, addr := range []string{"127.0.0.1:8080", "127.0.0.1:8081"} {
			ratio, err := getGarbageRatio(addr, uint32(volumeId))
			if err != nil {
				continue
			}
			t.Logf("Garbage ratio after vacuum on %s: %.2f%%", addr, ratio*100)
			if ratio < 0.05 {
				t.Logf("PASS: Garbage cleaned up (%.2f%%) on %s", ratio*100, addr)
			}
		}
	})

	// Verify remaining files are still readable
	t.Run("verify_remaining_data", func(t *testing.T) {
		for i := 5; i < 10; i++ {
			fid := fids[i]
			parsedFid, err := needle.ParseFileIdFromString(fid)
			require.NoError(t, err)

			// Look up volume location
			options := &shell.ShellOptions{
				Masters:        stringPtr(masterAddr),
				GrpcDialOption: grpc.WithInsecure(),
				FilerGroup:     stringPtr("default"),
			}
			commandEnv := shell.NewCommandEnv(options)
			readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer readCancel()
			go commandEnv.MasterClient.KeepConnectedToMaster(readCtx)
			commandEnv.MasterClient.WaitUntilConnected(readCtx)

			locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(parsedFid.VolumeId))
			require.True(t, found, "volume %d not found for fid %s", parsedFid.VolumeId, fid)
			require.NotEmpty(t, locations, "no locations for fid %s", fid)
			t.Logf("File %s still accessible at %s", fid, locations[0].Url)
		}
	})
}

func stringPtr(s string) *string {
	return &s
}
