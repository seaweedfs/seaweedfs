package command

import (
	"os"
	"path/filepath"
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
