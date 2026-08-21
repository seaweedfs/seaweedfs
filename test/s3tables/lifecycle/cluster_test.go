// Package lifecycle takes a table through everything that happens to it: made
// through the catalog, filled by a real client, maintained by the worker, read
// again, and dropped.
//
// The step that matters is the read after maintenance. A compaction once
// rewrote every dictionary-encoded column onto a single value and went out in a
// release, because the tests we had checked the bookkeeping - sequence numbers,
// manifest entries, metadata versions - and none of them opened the file the
// worker had just written. A tally taken before maintenance and the same tally
// taken after is the whole idea, and it is the same idea for both formats:
// Iceberg compaction merges parquet files, Lance compaction merges fragments,
// and either can hand back a table that reads without complaint and answers
// wrongly.
package lifecycle

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

const (
	// The keys the issue's own recipe uses. weed mini turns them into the
	// admin identity, which is what the clients then sign with.
	accessKey = "AKIAIOSFODNN7EXAMPLE"
	secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	startupTimeout = 60 * time.Second
	clientTimeout  = 20 * time.Minute
)

// shared is the one cluster both formats run against. They use separate table
// buckets, and a bucket holds one format only.
var shared *environment

// errNoBinary is the one setup failure worth skipping over.
var errNoBinary = errors.New("weed binary not found")

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	// A checkout without a weed binary cannot run this and says so. Anything
	// else - ports, a cluster that will not come up - is a failure, because a
	// suite that turns its own breakage into a green run is the thing this
	// directory exists to stop.
	env, err := newEnvironment()
	if errors.Is(err, errNoBinary) {
		fmt.Fprintf(os.Stderr, "SKIP: %v\n", err)
		os.Exit(m.Run())
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	if err := env.start(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: weed mini did not start: %v\n", err)
		env.cleanup()
		os.Exit(1)
	}
	shared = env

	code := m.Run()
	shared.cleanup()
	os.Exit(code)
}

type environment struct {
	weedBinary string
	rootDir    string
	testDir    string
	dataDir    string

	masterPort     int
	masterGrpcPort int
	volumePort     int
	volumeGrpcPort int
	filerPort      int
	filerGrpcPort  int
	s3Port         int
	s3GrpcPort     int
	icebergPort    int
	lancePort      int

	weedCancel context.CancelFunc
	weedCmd    *exec.Cmd
}

func newEnvironment() (*environment, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("get working directory: %w", err)
	}
	seaweedDir := wd
	for i := 0; i < 5; i++ {
		if _, err := os.Stat(filepath.Join(seaweedDir, "go.mod")); err == nil {
			break
		}
		seaweedDir = filepath.Dir(seaweedDir)
	}

	weedBinary := filepath.Join(seaweedDir, "weed", "weed")
	if info, statErr := os.Stat(weedBinary); statErr == nil && !info.IsDir() {
		// A plain `go test` will otherwise drive a binary from days ago and
		// report a pass for code it never ran.
		fmt.Fprintf(os.Stderr, "using %s, built %s\n", weedBinary, info.ModTime().Format(time.RFC3339))
	} else {
		weedBinary = "weed"
		if _, err := exec.LookPath(weedBinary); err != nil {
			return nil, errNoBinary
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-lifecycle-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	ports, err := testutil.AllocatePorts(10)
	if err != nil {
		return nil, fmt.Errorf("allocate ports: %w", err)
	}
	return &environment{
		weedBinary:     weedBinary,
		rootDir:        seaweedDir,
		testDir:        wd,
		dataDir:        dataDir,
		masterPort:     ports[0],
		masterGrpcPort: ports[1],
		volumePort:     ports[2],
		volumeGrpcPort: ports[3],
		filerPort:      ports[4],
		filerGrpcPort:  ports[5],
		s3Port:         ports[6],
		s3GrpcPort:     ports[7],
		icebergPort:    ports[8],
		lancePort:      ports[9],
	}, nil
}

func (env *environment) start() error {
	ctx, cancel := context.WithCancel(context.Background())
	env.weedCancel = cancel

	cmd := exec.CommandContext(ctx, env.weedBinary, "mini",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-master.port.grpc", fmt.Sprintf("%d", env.masterGrpcPort),
		"-volume.port", fmt.Sprintf("%d", env.volumePort),
		"-volume.port.grpc", fmt.Sprintf("%d", env.volumeGrpcPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-filer.port.grpc", fmt.Sprintf("%d", env.filerGrpcPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.grpc", fmt.Sprintf("%d", env.s3GrpcPort),
		"-s3.port.iceberg", fmt.Sprintf("%d", env.icebergPort),
		"-s3.port.lance", fmt.Sprintf("%d", env.lancePort),
		"-ip.bind", "0.0.0.0",
		"-dir", env.dataDir,
	)
	cmd.Dir = env.dataDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// mini makes its admin identity from these, so they are the keys the
	// clients sign with rather than a separate IAM file to keep in step.
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+accessKey,
		"AWS_SECRET_ACCESS_KEY="+secretKey,
	)

	if err := cmd.Start(); err != nil {
		cancel()
		return err
	}
	env.weedCmd = cmd

	if !testutil.WaitForService(env.catalogURL()+"/v1/config", startupTimeout) {
		cancel()
		return fmt.Errorf("the Iceberg catalog never answered on port %d", env.icebergPort)
	}
	if !testutil.WaitForPort(env.lancePort, startupTimeout) {
		cancel()
		return fmt.Errorf("the Lance namespace never answered on port %d", env.lancePort)
	}
	return nil
}

func (env *environment) cleanup() {
	if env.weedCancel != nil {
		env.weedCancel()
	}
	if env.weedCmd != nil {
		_ = env.weedCmd.Wait()
	}
	if env.dataDir != "" {
		_ = os.RemoveAll(env.dataDir)
	}
}

func (env *environment) catalogURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.icebergPort)
}

// A container reaches the same gateway by another name than the host does.
func (env *environment) containerURL(port int) string {
	return fmt.Sprintf("http://host.docker.internal:%d", port)
}

// createTableBucket makes a bucket declared for one format, so the catalog
// refuses tables of any other kind in it.
func (env *environment) createTableBucket(t *testing.T, bucket, format string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, env.weedBinary, "shell",
		fmt.Sprintf("-master=127.0.0.1:%d.%d", env.masterPort, env.masterGrpcPort),
	)
	cmd.Stdin = strings.NewReader(fmt.Sprintf(
		"s3tables.bucket -create -name %s -format %s -account 000000000000\nexit\n", bucket, format))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create the %s table bucket %s: %v\n%s", format, bucket, err, out)
	}
}

// filerClient dials the filer the maintenance handlers talk to.
func (env *environment) filerClient(t *testing.T) filer_pb.SeaweedFilerClient {
	t.Helper()

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", env.filerGrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial the filer: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return filer_pb.NewSeaweedFilerClient(conn)
}

// entryExists tells "the catalog forgot the table" from "the data is gone".
func (env *environment) entryExists(t *testing.T, path string) bool {
	t.Helper()

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", env.filerPort, path))
	if err != nil {
		t.Fatalf("filer GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

func randomSuffix() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffix := make([]byte, 8)
	for i := range suffix {
		suffix[i] = charset[rand.Intn(len(charset))]
	}
	return string(suffix)
}
