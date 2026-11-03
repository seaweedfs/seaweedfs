//go:build integration

package shell

import (
	"bytes"
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Opt-in: runs report-only and confirmed-purge scans against real services
// after a volume is deleted, and checks that a stopped server blocks the purge.
func TestVolumeFsckFindsReferenceAfterRealVolumeDeletion(t *testing.T) {
	weed := buildWeedForIntegration(t)
	base := t.TempDir()
	for _, name := range []string{"logs", "master", "volume", "filer"} {
		if err := os.MkdirAll(filepath.Join(base, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	portPairs := freeIntegrationPortPairs(t, 3)
	masterHTTP, masterGRPC := portPairs[0][0], portPairs[0][1]
	volumeHTTP, volumeGRPC := portPairs[1][0], portPairs[1][1]
	filerHTTP, filerGRPC := portPairs[2][0], portPairs[2][1]
	masterAddress := fmt.Sprintf("127.0.0.1:%d", masterHTTP)

	startIntegrationProcess(t, base, weed, "master",
		"master", "-ip=127.0.0.1", "-ip.bind=127.0.0.1",
		fmt.Sprintf("-port=%d", masterHTTP), fmt.Sprintf("-port.grpc=%d", masterGRPC),
		"-mdir="+filepath.Join(base, "master"), "-defaultReplication=000", "-volumeSizeLimitMB=64")
	waitIntegrationTCP(t, masterHTTP, filepath.Join(base, "logs", "master.log"))
	waitIntegrationTCP(t, masterGRPC, filepath.Join(base, "logs", "master.log"))

	volumeProcess := startIntegrationProcess(t, base, weed, "volume",
		"volume", "-ip=127.0.0.1", "-ip.bind=127.0.0.1",
		fmt.Sprintf("-port=%d", volumeHTTP), fmt.Sprintf("-port.grpc=%d", volumeGRPC),
		"-master="+masterAddress, "-dir="+filepath.Join(base, "volume"), "-max=10")
	waitIntegrationTCP(t, volumeHTTP, filepath.Join(base, "logs", "volume.log"))
	waitIntegrationTCP(t, volumeGRPC, filepath.Join(base, "logs", "volume.log"))
	waitForMasterVolumeRegistration(t, masterHTTP, volumeHTTP, filepath.Join(base, "logs", "volume.log"))

	startIntegrationProcess(t, base, weed, "filer",
		"filer", "-ip=127.0.0.1", "-ip.bind=127.0.0.1",
		fmt.Sprintf("-port=%d", filerHTTP), fmt.Sprintf("-port.grpc=%d", filerGRPC),
		"-master="+masterAddress, "-defaultReplicaPlacement=000",
		"-defaultStoreDir="+filepath.Join(base, "filer"))
	waitIntegrationTCP(t, filerHTTP, filepath.Join(base, "logs", "filer.log"))
	waitIntegrationTCP(t, filerGRPC, filepath.Join(base, "logs", "filer.log"))

	filerURL := fmt.Sprintf("http://127.0.0.1:%d", filerHTTP)
	request, err := http.NewRequest(http.MethodPut, filerURL+"/fsck-probe.txt", strings.NewReader("live integration data"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upload probe through Filer: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusAccepted {
		t.Fatalf("Filer PUT status = %d, want 201 or 202", response.StatusCode)
	}

	filerAddress := fmt.Sprintf("127.0.0.1:%d", filerHTTP)
	meta := runIntegrationShell(t, weed, masterAddress, filerAddress, "lock\nfs.meta.cat /fsck-probe.txt\nunlock\n")
	fidPattern := regexp.MustCompile(`"fid"\s*:\s*\{\s*"volumeId"\s*:\s*(\d+)`)
	match := fidPattern.FindStringSubmatch(meta)
	if len(match) != 2 {
		t.Fatalf("could not read chunk volume ID from Filer metadata: %s", meta)
	}
	volumeID := match[1]

	// Delete only the physical volume. The Filer entry intentionally remains
	// so fsck must discover a dangling reference rather than a missing file.
	deleteOutput := runIntegrationShell(t, weed, masterAddress, filerAddress,
		fmt.Sprintf("lock\nvolume.delete -node=127.0.0.1:%d -volumeId=%s\nunlock\n", volumeHTTP, volumeID))
	recentOutput := runIntegrationShell(t, weed, masterAddress, filerAddress,
		fmt.Sprintf("lock\nvolume.fsck -findMissingChunksInFiler -volumeId=%s\nunlock\n", volumeID))
	if strings.Contains(recentOutput, "/fsck-probe.txt") {
		t.Fatalf("default cutoff unexpectedly reported a just-written file: %q", recentOutput)
	}

	fsckOutput := runIntegrationShell(t, weed, masterAddress, filerAddress,
		fmt.Sprintf("lock\nvolume.fsck -findMissingChunksInFiler -volumeId=%s -cutoffTimeAgo=0\nunlock\n", volumeID))
	if !strings.Contains(fsckOutput, "scanning filer references for selected volume(s)") {
		t.Fatalf("fsck did not treat the deleted ID as a missing-volume target; delete=%q fsck=%q", deleteOutput, fsckOutput)
	}
	if !strings.Contains(fsckOutput, "/fsck-probe.txt") || !strings.Contains(fsckOutput, "volume not found") {
		t.Fatalf("fsck did not report the stale Filer reference: %q", fsckOutput)
	}

	scopedPurgeOutput := runIntegrationShell(t, weed, masterAddress, filerAddress,
		"lock\nvolume.fsck -findMissingChunksInFiler -reallyDeleteFilerEntries -collection=logs -cutoffTimeAgo=0\nunlock\n")
	if !strings.Contains(scopedPurgeOutput, "collection cannot be verified for volumes absent from master") {
		t.Fatalf("fsck did not warn that a missing volume cannot be collection-scoped: %q", scopedPurgeOutput)
	}
	if strings.Contains(scopedPurgeOutput, "deleting path /fsck-probe.txt after volume not found") {
		t.Fatalf("collection-filtered fsck queued an unverified missing-volume path: %q", scopedPurgeOutput)
	}

	// Report-only fsck must not delete the stale Filer entry.
	metaAfter := runIntegrationShell(t, weed, masterAddress, filerAddress, "lock\nfs.meta.cat /fsck-probe.txt\nunlock\n")
	if !strings.Contains(metaAfter, "fsck-probe.txt") {
		t.Fatalf("report-only fsck changed or removed the Filer entry: %q", metaAfter)
	}
	// An entry whose chunk manifest lived on the retired volume is unreadable too.
	filerConn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", filerGRPC), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial Filer gRPC: %v", err)
	}
	defer filerConn.Close()
	filerClient := filer_pb.NewSeaweedFilerClient(filerConn)
	volumeIDUint, err := strconv.ParseUint(volumeID, 10, 32)
	if err != nil {
		t.Fatalf("parse volume ID %q: %v", volumeID, err)
	}
	manifestChunk := &filer_pb.FileChunk{
		Fid:             &filer_pb.FileId{VolumeId: uint32(volumeIDUint), FileKey: 987654321, Cookie: 1},
		Size:            21,
		IsChunkManifest: true,
	}
	manifestChunk.FileId = manifestChunk.GetFileIdString()
	now := time.Now().Unix()
	if err := filer_pb.CreateEntry(context.Background(), filerClient, &filer_pb.CreateEntryRequest{
		Directory: "/",
		Entry: &filer_pb.Entry{
			Name:       "manifest-probe.txt",
			Attributes: &filer_pb.FuseAttributes{FileSize: 21, FileMode: 0o644, Mtime: now, Crtime: now},
			Chunks:     []*filer_pb.FileChunk{manifestChunk},
		},
	}); err != nil {
		t.Fatalf("create unresolved-manifest probe: %v", err)
	}

	purgeOutput := runIntegrationShell(t, weed, masterAddress, filerAddress,
		fmt.Sprintf("lock\nvolume.fsck -findMissingChunksInFiler -reallyDeleteFilerEntries -confirmMissingVolumeDeletion -volumeId=%s -cutoffTimeAgo=0\nunlock\n", volumeID))
	if !strings.Contains(purgeOutput, "ResolveChunkManifest failed") {
		t.Fatalf("fsck did not surface the unresolved manifest: %q", purgeOutput)
	}
	if !strings.Contains(purgeOutput, "deleting path /manifest-probe.txt") {
		t.Fatalf("confirmed purge did not queue the entry whose manifest was on the retired volume: %q", purgeOutput)
	}
	manifestAfterPurge, err := http.Get(filerURL + "/manifest-probe.txt")
	if err != nil {
		t.Fatalf("read manifest probe after confirmed purge: %v", err)
	}
	manifestAfterPurge.Body.Close()
	if manifestAfterPurge.StatusCode != http.StatusNotFound {
		t.Fatalf("confirmed purge manifest probe GET status = %d, want 404", manifestAfterPurge.StatusCode)
	}

	if !strings.Contains(purgeOutput, "deleting path /fsck-probe.txt after volume not found") {
		t.Fatalf("confirmed purge did not queue the direct missing-volume entry: %q", purgeOutput)
	}
	probeAfterPurge, err := http.Get(filerURL + "/fsck-probe.txt")
	if err != nil {
		t.Fatalf("read Filer probe after confirmed purge: %v", err)
	}
	probeAfterPurge.Body.Close()
	if probeAfterPurge.StatusCode != http.StatusNotFound {
		t.Fatalf("confirmed purge Filer GET status = %d, want 404", probeAfterPurge.StatusCode)
	}
	// A volume still listed by the master but whose server is unreachable is
	// not proof that its references are stale. Purge must fail before touching
	// the Filer entry when the volume index cannot be read.
	offlineRequest, err := http.NewRequest(http.MethodPut, filerURL+"/offline-probe.txt", strings.NewReader("offline safety probe"))
	if err != nil {
		t.Fatal(err)
	}
	offlineResponse, err := http.DefaultClient.Do(offlineRequest)
	if err != nil {
		t.Fatalf("upload offline safety probe: %v", err)
	}
	offlineResponse.Body.Close()
	if offlineResponse.StatusCode != http.StatusCreated && offlineResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("offline safety PUT status = %d", offlineResponse.StatusCode)
	}
	offlineMeta := runIntegrationShell(t, weed, masterAddress, filerAddress, "lock\nfs.meta.cat /offline-probe.txt\nunlock\n")
	offlineMatch := fidPattern.FindStringSubmatch(offlineMeta)
	if len(offlineMatch) != 2 {
		t.Fatalf("could not read offline probe volume ID: %s", offlineMeta)
	}
	if err := volumeProcess.Process.Kill(); err != nil {
		t.Fatalf("stop volume server: %v", err)
	}
	waitIntegrationTCPClosed(t, volumeHTTP)

	shellCtx, cancelShell := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShell()
	fsckCmd := exec.CommandContext(shellCtx, weed, "shell", "-master="+masterAddress, "-filer="+filerAddress)
	fsckCmd.Stdin = strings.NewReader(fmt.Sprintf("lock\nvolume.fsck -findMissingChunksInFiler -reallyDeleteFilerEntries -volumeId=%s -cutoffTimeAgo=0\nunlock\n", offlineMatch[1]))
	fsckBytes, fsckErr := fsckCmd.CombinedOutput()
	offlineFsckOutput := string(fsckBytes)
	if fsckErr == nil {
		t.Fatalf("fsck unexpectedly purged metadata for a volume server outage: %q", offlineFsckOutput)
	}
	if !strings.Contains(offlineFsckOutput, "confirmMissingVolumeDeletion") && !strings.Contains(offlineFsckOutput, "failed to collect file ids") {
		t.Fatalf("fsck failed for an unexpected reason: err=%v output=%q", fsckErr, offlineFsckOutput)
	}
	offlineMetaAfter := runIntegrationShell(t, weed, masterAddress, filerAddress, "lock\nfs.meta.cat /offline-probe.txt\nunlock\n")
	if !strings.Contains(offlineMetaAfter, "offline-probe.txt") {
		t.Fatalf("offline-volume fsck removed the live Filer entry: %q", offlineMetaAfter)
	}
}

func buildWeedForIntegration(t *testing.T) string {
	t.Helper()
	// Reuse the CI/integration binary when supplied; otherwise build this checkout.
	if binary := os.Getenv("WEED_BINARY"); binary != "" {
		if _, err := os.Stat(binary); err != nil {
			t.Fatalf("WEED_BINARY=%q is unavailable: %v", binary, err)
		}
		return binary
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	binary := filepath.Join(t.TempDir(), "weed")
	cmd := exec.Command("go", "build", "-o", binary, "./weed")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build weed: %v\n%s", err, out)
	}
	return binary
}

// freeIntegrationPortPairs reserves HTTP ports with the CLI's conventional
// HTTP+10000 gRPC mapping. The low range also keeps the derived ports valid
// for weed shell's master/Filer client configuration.
func freeIntegrationPortPairs(t *testing.T, count int) [][2]int {
	t.Helper()
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()

	pairs := make([][2]int, 0, count)
	for httpPort := 20000; httpPort < 30000 && len(pairs) < count; httpPort++ {
		grpcPort := httpPort + 10000
		httpListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", httpPort))
		if err != nil {
			continue
		}
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", grpcPort))
		if err != nil {
			httpListener.Close()
			continue
		}
		listeners = append(listeners, httpListener, grpcListener)
		pairs = append(pairs, [2]int{httpPort, grpcPort})
	}
	if len(pairs) != count {
		t.Fatalf("reserved %d of %d required HTTP/gRPC port pairs", len(pairs), count)
	}
	return pairs
}

func startIntegrationProcess(t *testing.T, base, weed, name string, args ...string) *exec.Cmd {
	t.Helper()
	logFile, err := os.Create(filepath.Join(base, "logs", name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(weed, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("start %s: %v", name, err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		_ = logFile.Close()
	})
	return cmd
}

// waitForMasterVolumeRegistration prevents racing a successful HTTP listener
// start against the volume server's first heartbeat to the master.
func waitForMasterVolumeRegistration(t *testing.T, masterPort, volumePort int, logPath string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	wantAddress := fmt.Sprintf("127.0.0.1:%d", volumePort)
	for time.Now().Before(deadline) {
		response, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/dir/status", masterPort))
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr == nil && bytes.Contains(body, []byte(wantAddress)) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	log, _ := os.ReadFile(logPath)
	t.Fatalf("master did not register volume server %s; log:\n%s", wantAddress, bytes.TrimSpace(log))
}

func waitIntegrationTCPClosed(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("service still listens on port %d after process was stopped", port)
}

func waitIntegrationTCP(t *testing.T, port int, logPath string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log, _ := os.ReadFile(logPath)
	t.Fatalf("service did not listen on port %d; log:\n%s", port, bytes.TrimSpace(log))
}

func runIntegrationShell(t *testing.T, weed, master, filer, input string) string {
	t.Helper()
	cmd := exec.Command(weed, "shell", "-master="+master, "-filer="+filer)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("weed shell failed: %v\n%s", err, out)
	}
	return string(out)
}
