package remote_cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	evictRemoteS3      = "http://localhost:28334"
	evictRemoteMaster  = "30334"
	evictRemoteFiler   = "28889"
	evictRemoteVolume  = "30341"
	evictRemoteWebdav  = "27334"
	evictRemoteMetrics = "30326"

	evictPrimaryS3      = "http://localhost:28333"
	evictPrimaryMaster  = "30333"
	evictPrimaryFiler   = "28887"
	evictPrimaryVolume  = "30340"
	evictPrimaryWebdav  = "27333"
	evictPrimaryMetrics = "30327"

	evictBucket = "evictsrc"
	evictMount  = "evictmnt"
)

var miniLogPaths []string

func startMini(t *testing.T, dir string, args ...string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0755))
	logPath := filepath.Join(dir, "weed.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	miniLogPaths = append(miniLogPaths, logPath)
	cmd := exec.Command(weedBinary, append([]string{"mini",
		"-dir=" + dir,
		"-s3.config=s3_config.json",
		"-s3.allowDeleteBucketNotEmpty=true",
		"-ip=127.0.0.1", "-ip.bind=127.0.0.1",
	}, args...)...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
		logFile.Close()
	})
}

func waitForHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(time.Second)
	}
	for _, p := range miniLogPaths {
		if data, err := os.ReadFile(p); err == nil {
			lines := strings.Split(string(data), "\n")
			if len(lines) > 30 {
				lines = lines[len(lines)-30:]
			}
			t.Logf("last lines of %s:\n%s", p, strings.Join(lines, "\n"))
		}
	}
	t.Fatalf("timed out waiting for %s", url)
}

func shellOn(t *testing.T, masterPort, command string) string {
	t.Helper()
	cmd := exec.Command(weedBinary, "shell", "-master=localhost:"+masterPort)
	cmd.Stdin = strings.NewReader(command + "\nexit\n")
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "shell %q failed: %s", command, out)
	return stripLogs(string(out))
}

func chunkCountOn(t *testing.T, masterPort, path string) string {
	meta := shellOn(t, masterPort, "fs.meta.cat "+path)
	idx := strings.LastIndex(meta, "chunks ")
	require.GreaterOrEqualf(t, idx, 0, "no chunk count in %s", meta)
	return strings.Fields(meta[idx+len("chunks "):])[0]
}

func volumeStats(t *testing.T, volumePort string) (size, garbage uint64) {
	t.Helper()
	resp, err := http.Get("http://localhost:" + volumePort + "/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	var status struct {
		Volumes []struct {
			Size             uint64 `json:"Size"`
			DeletedByteCount uint64 `json:"DeletedByteCount"`
		} `json:"Volumes"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&status))
	for _, v := range status.Volumes {
		size += v.Size
		garbage += v.DeletedByteCount
	}
	return size, garbage
}

func readViaFiler(t *testing.T, filerPort, path string) []byte {
	t.Helper()
	resp, err := http.Get("http://localhost:" + filerPort + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "read %s", path)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return data
}

// TestRemoteCacheEvictUnderPressure fills a cache constrained to two small
// volumes with remote-mounted objects until writes fail, then verifies the
// filer evicts the oldest synced entry, vacuums the garbage, and a later read
// caches again.
func TestRemoteCacheEvictUnderPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns two weed mini clusters")
	}
	if _, err := os.Stat(weedBinary); err != nil {
		t.Skipf("weed binary not found at %s; run make build-weed", weedBinary)
	}
	if isServerRunning(evictRemoteS3) || isServerRunning(evictPrimaryS3) {
		t.Skip("eviction test ports are already in use")
	}

	tmp := t.TempDir()
	startMini(t, filepath.Join(tmp, "remote"),
		"-s3.port=28334", "-master.port="+evictRemoteMaster,
		"-filer.port="+evictRemoteFiler, "-volume.port="+evictRemoteVolume,
		"-webdav.port="+evictRemoteWebdav, "-metricsPort="+evictRemoteMetrics)
	waitForHTTP(t, evictRemoteS3)
	startMini(t, filepath.Join(tmp, "primary"),
		"-s3.port=28333", "-master.port="+evictPrimaryMaster,
		"-filer.port="+evictPrimaryFiler, "-volume.port="+evictPrimaryVolume,
		"-webdav.port="+evictPrimaryWebdav, "-metricsPort="+evictPrimaryMetrics,
		"-volume.allowUntrustedRemoteEndpoints", "-filer.allowUntrustedRemoteEndpoints",
		"-s3.allowUntrustedRemoteEndpoints",
		"-master.volumeSizeLimitMB=32", "-volume.max=2",
		"-filer.remoteCacheEvictThreshold=0.99")
	waitForHTTP(t, evictPrimaryS3)

	remote := createS3Client(evictRemoteS3)
	_, err := remote.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(evictBucket)})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		data := make([]byte, 16*1024*1024)
		for j := range data {
			data[j] = byte(i + j%251)
		}
		_, err = remote.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(evictBucket),
			Key:    aws.String(fmt.Sprintf("obj%d.bin", i)),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)
	}

	shellOn(t, evictPrimaryMaster, fmt.Sprintf(
		"remote.configure -name=evictremote -type=s3 -s3.access_key=%s -s3.secret_key=%s -s3.endpoint=%s -s3.region=us-east-1",
		accessKey, secretKey, evictRemoteS3))
	shellOn(t, evictPrimaryMaster, fmt.Sprintf(
		"remote.mount -dir=/buckets/%s -remote=evictremote/%s -nonempty", evictMount, evictBucket))
	shellOn(t, evictPrimaryMaster, fmt.Sprintf("remote.meta.sync -dir=/buckets/%s", evictMount))
	time.Sleep(2 * time.Second)

	mount := "/buckets/" + evictMount

	// obj0 caches alone so it is the oldest evictable entry.
	first := readViaFiler(t, evictPrimaryFiler, mount+"/obj0.bin")
	require.Len(t, first, 16*1024*1024)
	require.NotEqual(t, "0", chunkCountOn(t, evictPrimaryMaster, mount+"/obj0.bin"), "obj0 should be cached")

	// 4 x 16MB against ~64MB of capacity: the fills overrun, hit the
	// capacity error path, and trigger eviction + vacuum.
	var wg sync.WaitGroup
	for i := 1; i <= 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := readViaFiler(t, evictPrimaryFiler, fmt.Sprintf("%s/obj%d.bin", mount, i))
			assert.Len(t, data, 16*1024*1024)
		}(i)
	}
	wg.Wait()

	// Eviction must have dropped obj0's local chunks and vacuumed the
	// tombstoned bytes, leaving only live data within the two volumes.
	require.True(t, waitForCondition(t, func() bool {
		size, garbage := volumeStats(t, evictPrimaryVolume)
		return garbage == 0 && size <= 72*1024*1024
	}, 2*time.Minute, "volumes to be reclaimed by eviction+vacuum"),
		"evicted chunks were not reclaimed")

	assert.Equal(t, "0", chunkCountOn(t, evictPrimaryMaster, mount+"/obj0.bin"),
		"oldest cached entry should be evicted back to remote-only")

	// The cache self-heals: reading the evicted object re-caches it. Fills
	// still running from the concurrent wave may evict it again, so retry
	// the read until a commit sticks.
	var again []byte
	require.True(t, waitForCondition(t, func() bool {
		again = readViaFiler(t, evictPrimaryFiler, mount+"/obj0.bin")
		return chunkCountOn(t, evictPrimaryMaster, mount+"/obj0.bin") != "0"
	}, 2*time.Minute, "obj0 to re-cache"),
		"evicted object did not re-cache on read")
	assert.Equal(t, first, again)
}
