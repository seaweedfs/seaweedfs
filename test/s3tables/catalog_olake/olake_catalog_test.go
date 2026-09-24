// Package catalog_olake provides an integration test for OLake
// (github.com/datazip-inc/olake) against the SeaweedFS Iceberg REST Catalog.
//
// OLake matters here for two reasons that no other engine in this directory
// covers. First, it does not write Iceberg from Go: it spawns a Java sidecar
// over gRPC and writes through the official Apache Iceberg library, so this is
// a strict Java client -- the class that iceberg/metadata_compliance.go exists
// to serve. Second, it is a CDC tool, so its upsert path emits equality
// deletes, which neither the ClickHouse nor the Doris test exercises.
package catalog_olake

import (
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

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const (
	olakeDefaultImage    = "olakego/source-postgres:latest"
	postgresDefaultImage = "postgres:16"
	readerImage          = "seaweedfs-olake-reader:test"

	tableBucketName = "olake-tables"
	sourceDatabase  = "olakedb"
	sourceUser      = "olake"
	sourcePassword  = "olakepw"

	// OLake derives the destination namespace from the source: it joins the
	// connector name, database and schema. Asserting the derived name rather
	// than configuring one keeps the test honest about what OLake actually
	// does with our catalog.
	expectedNamespace = "postgres_olakedb_public"
	expectedTable     = "orders"

	postgresStartTimeout = 90 * time.Second
	olakeRunTimeout      = 6 * time.Minute
)

type TestEnvironment struct {
	seaweedDir string
	weedBinary string
	dataDir    string
	configDir  string
	bindIP     string

	masterPort     int
	masterGrpcPort int
	volumePort     int
	volumeGrpcPort int
	filerPort      int
	filerGrpcPort  int
	s3Port         int
	s3GrpcPort     int
	icebergPort    int
	postgresPort   int

	accessKey string
	secretKey string

	weedProcess       *exec.Cmd
	weedCancel        func()
	postgresContainer string
}

func TestOLakeIcebergCatalog(t *testing.T) {
	requireOLakeRuntime(t)

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	env.StartSeaweedFS(t)
	env.startPostgres(t)
	env.seedSource(t)

	buildReaderImage(t)
	env.writeOLakeConfigs(t)

	t.Run("CheckDestination", func(t *testing.T) {
		out := env.runOLake(t, "check",
			"--config", "/mnt/config/source.json",
			"--destination", "/mnt/config/destination.json")
		if !strings.Contains(out, `"status":"SUCCEEDED"`) {
			t.Fatalf("olake check did not report SUCCEEDED.\n%s", tailLines(out, 40))
		}
		// The Java sidecar, not the Go process, is what talks to our catalog.
		// If this line is missing the check passed without exercising the
		// Iceberg REST path at all.
		if !strings.Contains(out, "org.apache.iceberg.rest.RESTSessionCatalog") {
			t.Errorf("check passed but never loaded the Iceberg REST catalog; "+
				"the destination may not have been contacted.\n%s", tailLines(out, 40))
		}
	})

	t.Run("Discover", func(t *testing.T) {
		out := env.runOLake(t, "discover",
			"--config", "/mnt/config/source.json",
			"--destination", "/mnt/config/destination.json")
		if !strings.Contains(out, `"stream_name":"`+expectedTable+`"`) {
			t.Fatalf("discover did not report the %s stream.\n%s", expectedTable, tailLines(out, 20))
		}
		if _, err := os.Stat(filepath.Join(env.configDir, "streams.json")); err != nil {
			t.Fatalf("discover did not write streams.json: %v", err)
		}
	})

	t.Run("FullSyncAppendsRows", func(t *testing.T) {
		out := env.runOLake(t, "sync",
			"--config", "/mnt/config/source.json",
			"--destination", "/mnt/config/destination.json",
			"--streams", "/mnt/config/streams.json",
			"--state", "/mnt/config/state.json")
		if !strings.Contains(out, "Total records read: 3") {
			t.Fatalf("sync did not read the three seeded rows.\n%s", tailLines(out, 30))
		}
		if !strings.Contains(out, "Committed snapshot") {
			t.Fatalf("sync never committed an Iceberg snapshot.\n%s", tailLines(out, 30))
		}
	})

	t.Run("StrictReaderSeesRows", func(t *testing.T) {
		rows := env.readTable(t, "rows")
		want := []string{"1,us-east,120.50", "2,us-west,87.20", "3,eu-west,210.00"}
		got := nonEmptyLines(rows)
		if len(got) != len(want) {
			t.Fatalf("PyIceberg read %d rows, want %d.\n%s", len(got), len(want), rows)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("row %d = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("UpsertProducesEqualityDelete", func(t *testing.T) {
		// Push the row past the incremental cursor so the next sync re-emits it
		// as an update rather than skipping it.
		env.execSQL(t, "UPDATE orders SET amount=999.99, region='ap-south' WHERE id=1")

		out := env.runOLake(t, "sync",
			"--config", "/mnt/config/source.json",
			"--destination", "/mnt/config/destination.json",
			"--streams", "/mnt/config/streams.json",
			"--state", "/mnt/config/state.json")
		if !strings.Contains(out, "delete files") {
			t.Fatalf("re-sync committed no delete files.\n%s", tailLines(out, 30))
		}

		meta := env.readTable(t, "snapshots")
		if !strings.Contains(meta, "format-version=2") {
			t.Errorf("expected Iceberg format-version 2, got:\n%s", meta)
		}
		if !strings.Contains(meta, "identifier-field-ids=") ||
			strings.Contains(meta, "identifier-field-ids=\n") {
			t.Errorf("table carries no identifier fields, so OLake could not "+
				"have upserted:\n%s", meta)
		}

		var sawOverwrite bool
		for _, line := range nonEmptyLines(meta) {
			if !strings.HasPrefix(line, "snapshot operation=") {
				continue
			}
			if strings.Contains(line, "operation=Operation.OVERWRITE") &&
				!strings.Contains(line, "added-equality-deletes=0") {
				sawOverwrite = true
			}
		}
		if !sawOverwrite {
			t.Errorf("no snapshot recorded an overwrite carrying equality "+
				"deletes:\n%s", meta)
		}
		if !strings.Contains(meta, "current-manifest-kinds=") ||
			!strings.Contains(meta, "DELETES") {
			t.Errorf("current snapshot has no delete manifest:\n%s", meta)
		}
		// Deliberately NOT asserted here: that a reader sees the updated value
		// and no duplicate row. PyIceberg refuses to scan a table carrying
		// equality deletes (apache/iceberg#6568), so a rows-mode read would
		// fail rather than pass, and swapping in an engine that can apply them
		// costs this test a multi-gigabyte image. Applying deletes on read is
		// the engine's contract; recording the commit correctly is ours, and
		// that is what the assertions above cover. See the README.
	})

	t.Run("CompliantWriterNeedsNoRepair", func(t *testing.T) {
		names := env.listTableMetadata(t)
		if len(names) == 0 {
			t.Fatalf("no metadata files found for %s.%s", expectedNamespace, expectedTable)
		}
		for _, n := range names {
			if strings.HasPrefix(n, "repaired-") {
				t.Errorf("catalog repaired a manifest written by OLake (%s); "+
					"the official Iceberg Java writer is expected to be "+
					"spec-compliant, so this is a regression in the repair "+
					"gate rather than in OLake", n)
			}
		}
	})
}

func NewTestEnvironment(t *testing.T) *TestEnvironment {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	seaweedDir := wd
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(seaweedDir, "go.mod")); err == nil {
			break
		}
		seaweedDir = filepath.Dir(seaweedDir)
	}

	weedBinary := filepath.Join(seaweedDir, "weed", "weed")
	if info, err := os.Stat(weedBinary); err != nil || info.IsDir() {
		weedBinary = filepath.Join(seaweedDir, "weed", "weed", "weed")
		if info, err := os.Stat(weedBinary); err != nil || info.IsDir() {
			weedBinary = "weed"
			if _, err := exec.LookPath(weedBinary); err != nil {
				t.Skip("weed binary not found, skipping integration test")
			}
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-olake-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	configDir := filepath.Join(dataDir, "olake")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create config dir: %v", err)
	}

	// 9 for the mini cluster, 1 for the Postgres source mapped on the host.
	ports := testutil.MustAllocatePorts(t, 10)

	return &TestEnvironment{
		seaweedDir:     seaweedDir,
		weedBinary:     weedBinary,
		dataDir:        dataDir,
		configDir:      configDir,
		bindIP:         testutil.FindBindIP(),
		masterPort:     ports[0],
		masterGrpcPort: ports[1],
		volumePort:     ports[2],
		volumeGrpcPort: ports[3],
		filerPort:      ports[4],
		filerGrpcPort:  ports[5],
		s3Port:         ports[6],
		s3GrpcPort:     ports[7],
		icebergPort:    ports[8],
		postgresPort:   ports[9],
		accessKey:      "AKIAIOSFODNN7EXAMPLE",
		secretKey:      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
}

func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	iamConfigPath, err := testutil.WriteIAMConfig(env.dataDir, env.accessKey, env.secretKey)
	if err != nil {
		t.Fatalf("Failed to create IAM config: %v", err)
	}

	cmd := exec.Command(env.weedBinary, "mini",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-master.port.grpc", fmt.Sprintf("%d", env.masterGrpcPort),
		"-volume.port", fmt.Sprintf("%d", env.volumePort),
		"-volume.port.grpc", fmt.Sprintf("%d", env.volumeGrpcPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-filer.port.grpc", fmt.Sprintf("%d", env.filerGrpcPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.grpc", fmt.Sprintf("%d", env.s3GrpcPort),
		"-s3.port.iceberg", fmt.Sprintf("%d", env.icebergPort),
		"-s3.config", iamConfigPath,
		// Pre-create the table bucket the way an operator would, rather than
		// reaching for the S3 Tables control plane from the test.
		"-tableBucket", tableBucketName,
		"-ip", env.bindIP,
		"-ip.bind", "0.0.0.0",
		"-dir", env.dataDir,
	)
	cmd.Dir = env.dataDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+env.accessKey,
		"AWS_SECRET_ACCESS_KEY="+env.secretKey,
	)

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start SeaweedFS: %v", err)
	}
	env.weedProcess = cmd
	env.weedCancel = func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}

	url := fmt.Sprintf("http://%s:%d/v1/config", env.bindIP, env.icebergPort)
	if !waitForService(url, 45*time.Second) {
		t.Fatalf("Iceberg REST API did not become ready at %s", url)
	}
}

func (env *TestEnvironment) startPostgres(t *testing.T) {
	t.Helper()

	name := fmt.Sprintf("olake-pg-%d", env.postgresPort)
	_ = exec.Command("docker", "rm", "-f", name).Run()

	cmd := exec.Command("docker", "run", "-d", "--name", name,
		"-e", "POSTGRES_USER="+sourceUser,
		"-e", "POSTGRES_PASSWORD="+sourcePassword,
		"-e", "POSTGRES_DB="+sourceDatabase,
		"-p", fmt.Sprintf("%d:5432", env.postgresPort),
		postgresImage(),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start Postgres: %v\n%s", err, out)
	}
	env.postgresContainer = name

	deadline := time.Now().Add(postgresStartTimeout)
	for time.Now().Before(deadline) {
		probe := exec.Command("docker", "exec", name,
			"pg_isready", "-U", sourceUser, "-d", sourceDatabase)
		if err := probe.Run(); err == nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Postgres did not become ready within %s", postgresStartTimeout)
}

func (env *TestEnvironment) seedSource(t *testing.T) {
	t.Helper()
	env.execSQL(t, `
CREATE TABLE orders (
    id       int PRIMARY KEY,
    region   text,
    amount   numeric(10,2),
    order_ts timestamp
);
INSERT INTO orders VALUES
    (1,'us-east',120.50,'2026-07-27 09:15'),
    (2,'us-west', 87.20,'2026-07-27 09:20'),
    (3,'eu-west',210.00,'2026-07-27 09:31');
ALTER TABLE orders REPLICA IDENTITY FULL;`)
}

func (env *TestEnvironment) execSQL(t *testing.T, sqlText string) {
	t.Helper()
	cmd := exec.Command("docker", "exec", env.postgresContainer,
		"psql", "-v", "ON_ERROR_STOP=1", "-U", sourceUser, "-d", sourceDatabase,
		"-c", sqlText)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("psql failed: %v\n%s", err, out)
	}
}

// writeOLakeConfigs writes the source and destination configs. The destination
// is the point of this test: nothing in it is SeaweedFS-specific. catalog_type
// is the generic "rest", auth is the standard OAuth2 client-credentials flow,
// and path-style access is not even set here because OLake turns it on by
// itself whenever s3_endpoint is non-empty.
func (env *TestEnvironment) writeOLakeConfigs(t *testing.T) {
	t.Helper()

	source := map[string]any{
		"host":            env.bindIP,
		"port":            env.postgresPort,
		"database":        sourceDatabase,
		"username":        sourceUser,
		"password":        sourcePassword,
		"jdbc_url_params": map[string]any{},
		"ssl":             map[string]any{"mode": "disable"},
		"update_method":   map[string]any{"type": "Standalone"},
		"max_threads":     2,
		"retry_count":     0,
	}

	catalogURL := fmt.Sprintf("http://%s:%d", env.bindIP, env.icebergPort)
	destination := map[string]any{
		"type": "ICEBERG",
		"writer": map[string]any{
			"catalog_type":     "rest",
			"rest_catalog_url": catalogURL,
			"catalog_name":     "olake",
			"iceberg_s3_path":  "s3://" + tableBucketName,
			"s3_endpoint":      fmt.Sprintf("http://%s:%d", env.bindIP, env.s3Port),
			"s3_use_ssl":       false,
			"s3_path_style":    true,
			"aws_region":       "us-east-1",
			"aws_access_key":   env.accessKey,
			"aws_secret_key":   env.secretKey,
			"rest_auth_type":   "oauth2",
			"oauth2_uri":       catalogURL + "/v1/oauth/tokens",
			"credential":       env.accessKey + ":" + env.secretKey,
		},
	}

	writeJSON(t, filepath.Join(env.configDir, "source.json"), source)
	writeJSON(t, filepath.Join(env.configDir, "destination.json"), destination)
	if err := os.WriteFile(filepath.Join(env.configDir, "state.json"), []byte("{}\n"), 0644); err != nil {
		t.Fatalf("Failed to write state.json: %v", err)
	}
}

func (env *TestEnvironment) runOLake(t *testing.T, args ...string) string {
	t.Helper()

	full := append([]string{
		"run", "--rm",
		"-v", dockerMount(env.configDir) + ":/mnt/config",
		olakeImage(),
	}, args...)

	cmd := exec.Command("docker", full...)
	done := make(chan struct{})
	var out []byte
	var err error
	go func() {
		out, err = cmd.CombinedOutput()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(olakeRunTimeout):
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		t.Fatalf("olake %s did not finish within %s", args[0], olakeRunTimeout)
	}
	if err != nil {
		t.Fatalf("olake %s failed: %v\n%s", args[0], err, tailLines(string(out), 40))
	}
	return string(out)
}

func (env *TestEnvironment) readTable(t *testing.T, mode string) string {
	t.Helper()

	cmd := exec.Command("docker", "run", "--rm", readerImage, mode,
		"--catalog-url", fmt.Sprintf("http://%s:%d", env.bindIP, env.icebergPort),
		"--warehouse", "s3://"+tableBucketName,
		"--prefix", tableBucketName,
		"--s3-endpoint", fmt.Sprintf("http://%s:%d", env.bindIP, env.s3Port),
		"--access-key", env.accessKey,
		"--secret-key", env.secretKey,
		"--namespace", expectedNamespace,
		"--table", expectedTable,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reader (%s) failed: %v\n%s", mode, err, tailLines(string(out), 30))
	}
	return string(out)
}

// listTableMetadata lists the table's metadata directory through the filer so
// the repair assertion looks at real objects rather than at what the catalog
// reports about itself.
func (env *TestEnvironment) listTableMetadata(t *testing.T) []string {
	t.Helper()

	url := fmt.Sprintf("http://%s:%d/buckets/%s/%s/%s/metadata/?limit=200",
		env.bindIP, env.filerPort, tableBucketName, expectedNamespace, expectedTable)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("Failed to build filer request: %v", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("filer listing failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("filer listing returned %d: %s", resp.StatusCode, body)
	}

	var parsed struct {
		Entries []struct {
			FullPath string `json:"FullPath"`
		} `json:"Entries"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("Failed to parse filer listing: %v\n%s", err, body)
	}

	names := make([]string, 0, len(parsed.Entries))
	for _, e := range parsed.Entries {
		names = append(names, e.FullPath[strings.LastIndex(e.FullPath, "/")+1:])
	}
	return names
}

func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.postgresContainer != "" {
		_ = exec.Command("docker", "rm", "-f", env.postgresContainer).Run()
	}
	if env.weedCancel != nil {
		env.weedCancel()
	}
	if env.weedProcess != nil {
		_ = env.weedProcess.Wait()
	}
	if env.dataDir != "" {
		_ = os.RemoveAll(env.dataDir)
	}
}

func buildReaderImage(t *testing.T) {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	cmd := exec.Command("docker", "build",
		"-f", filepath.Join(wd, "Dockerfile.reader"),
		"-t", readerImage, wd)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build reader image: %v\n%s", err, tailLines(string(out), 30))
	}
}

func olakeImage() string {
	if v := os.Getenv("OLAKE_IMAGE"); v != "" {
		return v
	}
	return olakeDefaultImage
}

func postgresImage() string {
	if v := os.Getenv("POSTGRES_IMAGE"); v != "" {
		return v
	}
	return postgresDefaultImage
}

func requireOLakeRuntime(t *testing.T) {
	t.Helper()
	if os.Getenv("SEAWEEDFS_SKIP_OLAKE_TESTS") != "" {
		t.Skip("SEAWEEDFS_SKIP_OLAKE_TESTS set")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available, skipping OLake integration test")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not reachable, skipping OLake integration test")
	}
}

func waitForService(url string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 3 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return true
			}
		}
		time.Sleep(time.Second)
	}
	return false
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal %s: %v", path, err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0644); err != nil {
		t.Fatalf("Failed to write %s: %v", path, err)
	}
}

// dockerMount normalises a host path for a -v bind mount. Docker Desktop
// accepts forward slashes on Windows; the native separator it does not.
func dockerMount(path string) string {
	return strings.ReplaceAll(path, `\`, "/")
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
