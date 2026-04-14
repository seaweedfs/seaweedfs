package nfs

import (
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestStartReturnsNotImplemented(t *testing.T) {
	server, err := NewServer(&Option{
		Filer:              pb.ServerAddress("localhost:8888"),
		BindIp:             "0.0.0.0",
		Port:               2049,
		FilerRootPath:      "/",
		VolumeServerAccess: "direct",
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	err = server.Start()
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Start() error = %v, want ErrNotImplemented", err)
	}
	if got := err.Error(); !strings.Contains(got, "nfs protocol serving is not implemented yet") {
		t.Fatalf("Start() error = %q, want clear not-implemented message", got)
	}
}
