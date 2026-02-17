package command

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestBuildPluginWorkerHandler(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	handler, err := buildPluginWorkerHandler("vacuum", dialOption)
	if err != nil {
		t.Fatalf("buildPluginWorkerHandler(vacuum) err = %v", err)
	}
	if handler == nil {
		t.Fatalf("expected non-nil handler")
	}

	handler, err = buildPluginWorkerHandler("", dialOption)
	if err != nil {
		t.Fatalf("buildPluginWorkerHandler(default) err = %v", err)
	}
	if handler == nil {
		t.Fatalf("expected non-nil default handler")
	}

	_, err = buildPluginWorkerHandler("unknown", dialOption)
	if err == nil {
		t.Fatalf("expected unsupported job type error")
	}
}

func TestResolvePluginWorkerID(t *testing.T) {
	dir := t.TempDir()

	explicit, err := resolvePluginWorkerID("worker-x", dir)
	if err != nil {
		t.Fatalf("resolvePluginWorkerID(explicit) err = %v", err)
	}
	if explicit != "worker-x" {
		t.Fatalf("expected explicit id, got %q", explicit)
	}

	generated, err := resolvePluginWorkerID("", dir)
	if err != nil {
		t.Fatalf("resolvePluginWorkerID(generate) err = %v", err)
	}
	if generated == "" {
		t.Fatalf("expected generated id")
	}
	if len(generated) < 7 || generated[:7] != "plugin-" {
		t.Fatalf("expected generated id prefix plugin-, got %q", generated)
	}

	persistedPath := filepath.Join(dir, "plugin.worker.id")
	if _, statErr := os.Stat(persistedPath); statErr != nil {
		t.Fatalf("expected persisted worker id file: %v", statErr)
	}

	reused, err := resolvePluginWorkerID("", dir)
	if err != nil {
		t.Fatalf("resolvePluginWorkerID(reuse) err = %v", err)
	}
	if reused != generated {
		t.Fatalf("expected reused id %q, got %q", generated, reused)
	}
}

func TestParsePluginWorkerAdminAddress(t *testing.T) {
	host, httpPort, hasExplicitGrpcPort, err := parsePluginWorkerAdminAddress("localhost:23646")
	if err != nil {
		t.Fatalf("parsePluginWorkerAdminAddress(localhost:23646) err = %v", err)
	}
	if host != "localhost" || httpPort != 23646 || hasExplicitGrpcPort {
		t.Fatalf("unexpected parse result: host=%q httpPort=%d hasExplicit=%v", host, httpPort, hasExplicitGrpcPort)
	}

	host, httpPort, hasExplicitGrpcPort, err = parsePluginWorkerAdminAddress("localhost:23646.33646")
	if err != nil {
		t.Fatalf("parsePluginWorkerAdminAddress(localhost:23646.33646) err = %v", err)
	}
	if host != "localhost" || httpPort != 23646 || !hasExplicitGrpcPort {
		t.Fatalf("unexpected dotted parse result: host=%q httpPort=%d hasExplicit=%v", host, httpPort, hasExplicitGrpcPort)
	}

	if _, _, _, err = parsePluginWorkerAdminAddress("localhost"); err == nil {
		t.Fatalf("expected parse error for invalid address")
	}
}

func TestResolvePluginWorkerAdminServerUsesStatusGrpcPort(t *testing.T) {
	const grpcPort = 35432

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/plugin/status" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(fmt.Sprintf(`{"worker_grpc_port":%d}`, grpcPort)))
	}))
	defer server.Close()

	adminAddress := strings.TrimPrefix(server.URL, "http://")
	host, httpPort, _, err := parsePluginWorkerAdminAddress(adminAddress)
	if err != nil {
		t.Fatalf("parsePluginWorkerAdminAddress(%s) err = %v", adminAddress, err)
	}

	resolved := resolvePluginWorkerAdminServer(adminAddress)
	expected := fmt.Sprintf("%s:%d.%d", host, httpPort, grpcPort)
	if resolved != expected {
		t.Fatalf("unexpected resolved admin address: got=%q want=%q", resolved, expected)
	}
}

func TestResolvePluginWorkerAdminServerKeepsDefaultGrpcOffset(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/plugin/status" {
			http.NotFound(w, r)
			return
		}
		address := strings.TrimPrefix(server.URL, "http://")
		_, httpPort, _, parseErr := parsePluginWorkerAdminAddress(address)
		if parseErr != nil {
			http.Error(w, parseErr.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(fmt.Sprintf(`{"worker_grpc_port":%d}`, httpPort+10000)))
	}))
	defer server.Close()

	adminAddress := strings.TrimPrefix(server.URL, "http://")
	resolved := resolvePluginWorkerAdminServer(adminAddress)
	if resolved != adminAddress {
		t.Fatalf("expected admin address to remain unchanged, got=%q want=%q", resolved, adminAddress)
	}
}
