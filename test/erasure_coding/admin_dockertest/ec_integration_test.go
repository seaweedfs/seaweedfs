package admin_dockertest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	AdminUrl  = "http://localhost:23646"
	MasterUrl = "http://localhost:9333"
	FilerUrl  = "http://localhost:8888"
)

// Helper to run commands in background and track PIDs for cleanup
var runningCmds []*exec.Cmd

func cleanup() {
	for _, cmd := range runningCmds {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}
}

func startWeed(t *testing.T, name string, args ...string) *exec.Cmd {
	cmd := exec.Command("./weed_bin", args...)

	// Create logs dir in local ./tmp
	wd, _ := os.Getwd()
	logDir := filepath.Join(wd, "tmp", "logs")
	os.MkdirAll(logDir, 0755)

	logFile, err := os.Create(filepath.Join(logDir, name+".log"))
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}

	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Set Cwd to test directory so it finds local ./tmp
	cmd.Dir = wd

	// assume "weed_bin" binary is in project root.
	rootDir := filepath.Dir(filepath.Dir(filepath.Dir(wd)))
	cmd.Path = filepath.Join(rootDir, "weed_bin")

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Failed to start weed %v: %v", args, err)
	}
	runningCmds = append(runningCmds, cmd)
	return cmd
}

func stopWeed(t *testing.T, cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		t.Logf("Stopping process %d", cmd.Process.Pid)
		cmd.Process.Kill()
		cmd.Wait()

		// Remove from runningCmds to avoid double kill in cleanup
		for i, c := range runningCmds {
			if c == cmd {
				runningCmds = append(runningCmds[:i], runningCmds[i+1:]...)
				break
			}
		}
	}
}

func ensureEnvironment(t *testing.T) {
	// 1. Build weed binary
	wd, _ := os.Getwd()
	rootDir := filepath.Dir(filepath.Dir(filepath.Dir(wd))) // Up 3 levels

	buildCmd := exec.Command("go", "build", "-o", "weed_bin", "./weed")
	buildCmd.Dir = rootDir
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build weed: %v", err)
	}
	t.Log("Successfully built weed binary")

	// 2. Start Master
	// Use local ./tmp/master
	os.RemoveAll("tmp")
	err := os.MkdirAll(filepath.Join("tmp", "master"), 0755)
	if err != nil {
		t.Fatalf("Failed to create tmp dir: %v", err)
	}

	startWeed(t, "master", "master", "-mdir=./tmp/master", "-port=9333", "-ip=localhost", "-peers=none", "-volumeSizeLimitMB=100")

	// Wait for master
	waitForUrl(t, MasterUrl+"/cluster/status", 10)

	// 3. Start Volume Server (Worker)
	// Start 14 volume servers to verify RS(10,4) default EC
	for i := 1; i <= 14; i++ {
		volName := fmt.Sprintf("volume%d", i)
		port := 8080 + i - 1
		dir := filepath.Join("tmp", volName)
		os.MkdirAll(dir, 0755)
		startWeed(t, volName, "volume", "-dir="+dir, "-mserver=localhost:9333", fmt.Sprintf("-port=%d", port), "-ip=localhost")
	}

	// 4. Start Filer
	os.MkdirAll(filepath.Join("tmp", "filer"), 0755)
	startWeed(t, "filer", "filer", "-defaultStoreDir=./tmp/filer", "-master=localhost:9333", "-port=8888", "-ip=localhost")
	waitForUrl(t, FilerUrl+"/", 60)

	// 5. Start Workers (Maintenance)
	// We need workers to execute EC tasks
	for i := 1; i <= 2; i++ {
		workerName := fmt.Sprintf("worker%d", i)
		metricsPort := 9327 + i - 1
		debugPort := 6060 + i
		dir, _ := filepath.Abs(filepath.Join("tmp", workerName))
		os.MkdirAll(dir, 0755)
		startWeed(t, workerName, "worker", "-admin=localhost:23646", "-workingDir="+dir, fmt.Sprintf("-metricsPort=%d", metricsPort), fmt.Sprintf("-debug.port=%d", debugPort))
	}

	// 6. Start Admin
	os.RemoveAll(filepath.Join("tmp", "admin"))
	os.MkdirAll(filepath.Join("tmp", "admin"), 0755)
	startWeed(t, "admin", "admin", "-master=localhost:9333", "-port=23646", "-dataDir=./tmp/admin")
	waitForUrl(t, AdminUrl+"/health", 60)

	t.Log("Environment started successfully")
}

func waitForUrl(t *testing.T, url string, retries int) {
	for i := 0; i < retries; i++ {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("Timeout waiting for %s", url)
}

func TestEcEndToEnd(t *testing.T) {
	defer cleanup()
	ensureEnvironment(t)

	client := &http.Client{}

	// 1. Configure Global Maintenance (Scan Interval = 1s) via API
	t.Log("Configuring Global Maintenance via API...")

	// 1.1 Fetch current config
	req, _ := http.NewRequest("GET", AdminUrl+"/api/maintenance/config", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to get global config: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to get global config (status %d): %s", resp.StatusCode, string(body))
	}

	var globalConfig map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&globalConfig); err != nil {
		t.Fatalf("Failed to decode global config: %v", err)
	}
	resp.Body.Close()

	// 1.2 Modify config
	globalConfig["enabled"] = true
	globalConfig["scan_interval_seconds"] = 1

	// Explicitly disable other tasks to avoid interference
	if globalConfig["policy"] == nil {
		globalConfig["policy"] = map[string]interface{}{}
	}
	policy, _ := globalConfig["policy"].(map[string]interface{})
	if taskPolicies, ok := policy["task_policies"].(map[string]interface{}); ok {
		if balancePolicy, ok := taskPolicies["balance"].(map[string]interface{}); ok {
			balancePolicy["enabled"] = false
		}
	}

	// Explicitly set required fields
	requiredFields := map[string]float64{
		"worker_timeout_seconds":   300,
		"task_timeout_seconds":     7200,
		"retry_delay_seconds":      900,
		"cleanup_interval_seconds": 86400,
		"task_retention_seconds":   604800,
		"max_retries":              3,
	}
	for field, val := range requiredFields {
		if _, ok := globalConfig[field]; !ok || globalConfig[field] == 0 {
			globalConfig[field] = val
		}
	}

	if _, ok := globalConfig["policy"]; !ok {
		globalConfig["policy"] = map[string]interface{}{}
	}
	policy, ok := globalConfig["policy"].(map[string]interface{})
	if ok {
		policy["global_max_concurrent"] = 4
		globalConfig["policy"] = policy
	}

	// 1.3 Update config
	jsonBody, _ := json.Marshal(globalConfig)
	req, _ = http.NewRequest("PUT", AdminUrl+"/api/maintenance/config", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("Failed to update global config: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to update global config (status %d): %s", resp.StatusCode, string(body))
	}
	resp.Body.Close()

	// 2. Configure EC Task (Short intervals) via Form API
	t.Log("Configuring EC Task via Form API...")
	formData := url.Values{}
	formData.Set("enabled", "true")
	formData.Set("scan_interval_seconds", "1")
	formData.Set("repeat_interval_seconds", "1")
	formData.Set("check_interval_seconds", "1")
	formData.Set("max_concurrent", "4")
	formData.Set("quiet_for_seconds_value", "1")
	formData.Set("quiet_for_seconds_unit", "seconds")
	formData.Set("min_size_mb", "1")
	formData.Set("fullness_ratio", "0.0001")

	req, _ = http.NewRequest("POST", AdminUrl+"/maintenance/config/erasure_coding", strings.NewReader(formData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("Failed to update EC config: %v", err)
	}
	if resp.StatusCode != 200 && resp.StatusCode != 303 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to update EC config (status %d): %s", resp.StatusCode, string(body))
	}
	resp.Body.Close()
	t.Log("EC Task Configuration updated")

	// 3. Restart Admin to pick up Global Config (Scan Interval)
	if len(runningCmds) > 0 {
		adminCmd := runningCmds[len(runningCmds)-1]
		t.Log("Restarting Admin Server to apply configuration...")
		stopWeed(t, adminCmd)
		time.Sleep(10 * time.Second)
		startWeed(t, "admin_restarted", "admin", "-master=localhost:9333", "-port=23646", "-port.grpc=33646", "-dataDir=./tmp/admin")
		waitForUrl(t, AdminUrl+"/health", 60)
	}

	// 4. Upload a file
	fileSize := 5 * 1024 * 1024
	data := make([]byte, fileSize)
	rand.Read(data)
	fileName := fmt.Sprintf("ec_test_file_%d", time.Now().Unix())
	t.Logf("Uploading %d bytes file %s to Filer...", fileSize, fileName)
	uploadUrl := FilerUrl + "/" + fileName

	var uploadErr error
	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest("PUT", uploadUrl, bytes.NewBuffer(data))
		resp, err := client.Do(req)
		if err == nil {
			if resp.StatusCode == 201 {
				resp.Body.Close()
				uploadErr = nil
				break
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			uploadErr = fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
		} else {
			uploadErr = err
		}
		t.Logf("Upload attempt %d failed: %v", i+1, uploadErr)
		time.Sleep(2 * time.Second)
	}

	if uploadErr != nil {
		t.Fatalf("Failed to upload file after retries: %v", uploadErr)
	}
	t.Log("Upload successful")

	// 5. Verify EC Encoding
	t.Log("Waiting for EC encoding (checking Master topology)...")
	startTime := time.Now()
	ecVerified := false
	var lastBody []byte

	for time.Since(startTime) < 300*time.Second {
		// 5.1 Check Master Topology
		resp, err := http.Get(MasterUrl + "/dir/status")
		if err == nil {
			lastBody, _ = ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			// Check total EC shards
			reShards := regexp.MustCompile(`"EcShards":\s*(\d+)`)
			matches := reShards.FindAllSubmatch(lastBody, -1)
			totalShards := 0
			for _, m := range matches {
				var count int
				fmt.Sscanf(string(m[1]), "%d", &count)
				totalShards += count
			}

			if totalShards > 0 {
				t.Logf("EC encoding verified (found %d total EcShards in topology) after %d seconds", totalShards, int(time.Since(startTime).Seconds()))
				ecVerified = true
				break
			}
		}

		// 5.2 Debug: Check workers and tasks
		wResp, wErr := http.Get(AdminUrl + "/api/maintenance/workers")
		workerCount := 0
		if wErr == nil {
			var workers []interface{}
			json.NewDecoder(wResp.Body).Decode(&workers)
			wResp.Body.Close()
			workerCount = len(workers)
		}

		tResp, tErr := http.Get(AdminUrl + "/api/maintenance/tasks")
		taskCount := 0
		if tErr == nil {
			var tasks []interface{}
			json.NewDecoder(tResp.Body).Decode(&tasks)
			tResp.Body.Close()
			taskCount = len(tasks)
		}
		t.Logf("Waiting for EC... (Workers: %d, Active Tasks: %d)", workerCount, taskCount)

		time.Sleep(10 * time.Second)
	}

	if !ecVerified {
		dumpLogs(t)
		t.Fatalf("Timed out waiting for EC encoding verified in Topology. Last body: %s", string(lastBody))
	}

	// 6. Verification: Read back the file
	t.Log("Reading back file...")
	resp, err = http.Get(uploadUrl)
	if err != nil {
		dumpLogs(t)
		t.Fatalf("Failed to read back file: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		dumpLogs(t)
		t.Fatalf("Read back failed status: %d", resp.StatusCode)
	}
	content, _ := io.ReadAll(resp.Body)
	if len(content) != fileSize {
		dumpLogs(t)
		t.Fatalf("Read back size mismatch: got %d, want %d", len(content), fileSize)
	}

	t.Log("Test PASS: EC encoding and read back successful!")
}

func dumpLogs(t *testing.T) {
	wd, _ := os.Getwd()
	logDir := filepath.Join(wd, "tmp", "logs")
	files, _ := os.ReadDir(logDir)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			content, _ := os.ReadFile(filepath.Join(logDir, f.Name()))
			t.Logf("--- LOG DUMP: %s ---\n%s\n--- END LOG ---", f.Name(), string(content))
		}
	}
}
