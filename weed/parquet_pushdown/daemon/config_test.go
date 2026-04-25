package daemon

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/parquet_pushdown"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func baseConfig() Config {
	return Config{
		IP:             "127.0.0.1",
		Port:           18888,
		FilerAddresses: []pb.ServerAddress{"localhost:8888"},
		Version:        "test",
	}
}

func TestConfig_ValidateOK(t *testing.T) {
	c := baseConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	if c.TrustMode != parquet_pushdown.TrustModeCatalogValidated {
		t.Errorf("default trust mode = %q, want %q", c.TrustMode, parquet_pushdown.TrustModeCatalogValidated)
	}
}

func TestConfig_RejectsBadPort(t *testing.T) {
	c := baseConfig()
	c.Port = 0
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for port=0")
	}
	c.Port = 70000
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for port>65535")
	}
}

func TestConfig_RequiresFiler(t *testing.T) {
	c := baseConfig()
	c.FilerAddresses = nil
	if err := c.Validate(); err == nil {
		t.Fatal("expected error when filer addresses empty")
	}
}

// Connector-trusted is dev-only and must require an explicit
// acknowledgement flag. Operators must not be able to flip it on by
// passing -trust=connector-trusted alone.
func TestConfig_ConnectorTrustedRequiresAck(t *testing.T) {
	c := baseConfig()
	c.TrustMode = parquet_pushdown.TrustModeConnectorTrusted
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error when connector-trusted lacks ack")
	}
	if !strings.Contains(err.Error(), "connector_trusted_ack") {
		t.Errorf("error %q should mention the ack flag", err.Error())
	}

	c.ConnectorTrustedAck = true
	if err := c.Validate(); err != nil {
		t.Fatalf("connector-trusted with ack should pass, got %v", err)
	}
}

func TestConfig_RejectsUnknownTrustMode(t *testing.T) {
	c := baseConfig()
	c.TrustMode = parquet_pushdown.TrustMode("nonsense")
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for unknown trust mode")
	}
}
