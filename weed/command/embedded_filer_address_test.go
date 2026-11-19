package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestEmbeddedFilerAddressDefaultsGrpcPort(t *testing.T) {
	p := 0
	if got := embeddedFilerAddress("10.0.0.1", 8889, &p); got != "10.0.0.1:8889.18889" {
		t.Fatalf("address: want 10.0.0.1:8889.18889, got %s", got)
	}
	if p != 18889 {
		t.Fatalf("grpc port: want the default 18889 written back, got %d", p)
	}
}

func TestEmbeddedFilerAddressKeepsExplicitGrpcPort(t *testing.T) {
	p := 19999
	if got := embeddedFilerAddress("10.0.0.1", 8889, &p); got != "10.0.0.1:8889.19999" {
		t.Fatalf("address: want 10.0.0.1:8889.19999, got %s", got)
	}
	if p != 19999 {
		t.Fatalf("grpc port changed: %d", p)
	}
}

// The embedded S3 gateway must name its filer the way the filer registers in
// the master, or discovery treats them as different filers.
func TestEmbeddedFilerAddressMatchesRegisteredForm(t *testing.T) {
	p := 0
	got := embeddedFilerAddress("10.0.0.1", 8889, &p)
	if want := string(pb.NewServerAddress("10.0.0.1", 8889, 18889)); got != want {
		t.Fatalf("address: want registered form %s, got %s", want, got)
	}
}
