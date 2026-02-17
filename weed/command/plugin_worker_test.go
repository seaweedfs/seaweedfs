package command

import (
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
