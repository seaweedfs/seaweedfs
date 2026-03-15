package command

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestBuildPluginWorkerHandlerExplicitTypes(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	testMaxConcurrency := int(pluginworker.DefaultMaxExecutionConcurrency)

	for _, jobType := range []string{"vacuum", "volume_balance", "erasure_coding", "admin_script", "iceberg_maintenance"} {
		handlers, err := buildPluginWorkerHandlers(jobType, dialOption, testMaxConcurrency, "")
		if err != nil {
			t.Fatalf("buildPluginWorkerHandlers(%s) err = %v", jobType, err)
		}
		if len(handlers) != 1 {
			t.Fatalf("expected 1 handler for %s, got %d", jobType, len(handlers))
		}
	}
}

func TestBuildPluginWorkerHandlerAliases(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	testMaxConcurrency := int(pluginworker.DefaultMaxExecutionConcurrency)

	for _, alias := range []string{"balance", "ec", "iceberg", "admin", "script"} {
		handlers, err := buildPluginWorkerHandlers(alias, dialOption, testMaxConcurrency, "")
		if err != nil {
			t.Fatalf("buildPluginWorkerHandlers(%s) err = %v", alias, err)
		}
		if len(handlers) != 1 {
			t.Fatalf("expected 1 handler for alias %s, got %d", alias, len(handlers))
		}
	}
}

func TestBuildPluginWorkerHandlerUnknown(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	_, err := buildPluginWorkerHandlers("unknown", dialOption, 1, "")
	if err == nil {
		t.Fatalf("expected error for unknown job type")
	}
}

func TestBuildPluginWorkerHandlers(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	testMaxConcurrency := int(pluginworker.DefaultMaxExecutionConcurrency)

	handlers, err := buildPluginWorkerHandlers("vacuum,volume_balance,erasure_coding", dialOption, testMaxConcurrency, "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(list) err = %v", err)
	}
	if len(handlers) != 3 {
		t.Fatalf("expected 3 handlers, got %d", len(handlers))
	}

	handlers, err = buildPluginWorkerHandlers("balance,ec,vacuum,balance", dialOption, testMaxConcurrency, "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(aliases) err = %v", err)
	}
	if len(handlers) != 3 {
		t.Fatalf("expected deduped 3 handlers, got %d", len(handlers))
	}

	_, err = buildPluginWorkerHandlers("unknown,vacuum", dialOption, testMaxConcurrency, "")
	if err == nil {
		t.Fatalf("expected unsupported job type error")
	}
}

func TestBuildPluginWorkerHandlersCategories(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	testMaxConcurrency := int(pluginworker.DefaultMaxExecutionConcurrency)

	allHandlers, err := buildPluginWorkerHandlers("all", dialOption, testMaxConcurrency, "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(all) err = %v", err)
	}
	// "all" must include at least vacuum and erasure_coding (one default, one heavy)
	allNames := handlerJobTypes(allHandlers)
	for _, required := range []string{"vacuum", "erasure_coding", "iceberg_maintenance"} {
		if !allNames[required] {
			t.Fatalf("'all' missing expected job type %q, got %v", required, allNames)
		}
	}

	defaultHandlers, err := buildPluginWorkerHandlers("default", dialOption, testMaxConcurrency, "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(default) err = %v", err)
	}
	defaultNames := handlerJobTypes(defaultHandlers)

	heavyHandlers, err := buildPluginWorkerHandlers("heavy", dialOption, testMaxConcurrency, "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(heavy) err = %v", err)
	}
	heavyNames := handlerJobTypes(heavyHandlers)

	// default and heavy must both be non-empty subsets of all
	if len(defaultNames) == 0 {
		t.Fatalf("'default' resolved no handlers")
	}
	if len(heavyNames) == 0 {
		t.Fatalf("'heavy' resolved no handlers")
	}
	for name := range defaultNames {
		if !allNames[name] {
			t.Fatalf("default handler %q not in 'all'", name)
		}
	}
	for name := range heavyNames {
		if !allNames[name] {
			t.Fatalf("heavy handler %q not in 'all'", name)
		}
	}

	// default and heavy must be disjoint and their union must equal all
	for name := range defaultNames {
		if heavyNames[name] {
			t.Fatalf("handler %q appears in both default and heavy", name)
		}
	}
	if len(defaultNames)+len(heavyNames) != len(allNames) {
		t.Fatalf("union(default=%d, heavy=%d) != all(%d)", len(defaultNames), len(heavyNames), len(allNames))
	}

	// mix category + explicit: "default,iceberg" adds one heavy to default set
	mixedHandlers, err := buildPluginWorkerHandlers("default,iceberg", dialOption, testMaxConcurrency, "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(default,iceberg) err = %v", err)
	}
	if len(mixedHandlers) != len(defaultHandlers)+1 {
		t.Fatalf("expected default+1 handlers for 'default,iceberg', got %d (default=%d)", len(mixedHandlers), len(defaultHandlers))
	}
}

func TestPluginWorkerDefaultJobTypes(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	testMaxConcurrency := int(pluginworker.DefaultMaxExecutionConcurrency)

	// defaultPluginWorkerJobTypes is "all", so it should match the "all" category exactly
	defaultHandlers, err := buildPluginWorkerHandlers(defaultPluginWorkerJobTypes, dialOption, testMaxConcurrency, "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(default setting) err = %v", err)
	}
	allHandlers, err := buildPluginWorkerHandlers("all", dialOption, testMaxConcurrency, "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(all) err = %v", err)
	}
	if len(defaultHandlers) != len(allHandlers) {
		t.Fatalf("default setting resolved %d handlers, 'all' resolved %d", len(defaultHandlers), len(allHandlers))
	}
}

// handlerJobTypes returns the set of job type names from a slice of handlers.
func handlerJobTypes(handlers []pluginworker.JobHandler) map[string]bool {
	m := make(map[string]bool, len(handlers))
	for _, h := range handlers {
		m[h.Capability().JobType] = true
	}
	return m
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
	if len(generated) < 2 || generated[:2] != "w-" {
		t.Fatalf("expected generated id prefix w-, got %q", generated)
	}

	persistedPath := filepath.Join(dir, "worker.id")
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
