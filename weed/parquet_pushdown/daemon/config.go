// Package daemon hosts the standalone `weed pushdown` process.
// It owns gRPC server bootstrap, the filer client used by later
// milestones to read Parquet data and side-index blobs, and the
// process-level config plumbing.
package daemon

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/parquet_pushdown"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// Config controls how the daemon listens, talks to the filer, and
// validates incoming requests. Caller (the `weed pushdown` command)
// builds this from CLI flags.
type Config struct {
	// IP is the bind address for the gRPC listener. Empty defaults to
	// the auto-detected host address.
	IP string

	// Port is the gRPC listener port.
	Port int

	// FilerAddresses is the comma-resolved list of filer endpoints
	// the daemon talks to for data reads and (in M3) catalog access.
	// Required: at least one entry.
	FilerAddresses []pb.ServerAddress

	// TrustMode is the request-validation strictness. Default is
	// "catalog-validated"; "connector-trusted" is a developer-only
	// mode that the daemon refuses to accept in production builds
	// (see ConnectorTrustedAck).
	TrustMode parquet_pushdown.TrustMode

	// ConnectorTrustedAck is the explicit acknowledgement required to
	// run with TrustMode == TrustModeConnectorTrusted. The flag exists
	// to make the dev-only mode awkward to enable by accident.
	ConnectorTrustedAck bool

	// Version is reported in PingResponse / PushdownStats. Caller
	// usually fills this with weed/util/version.Version().
	Version string
}

// Validate checks the config before the daemon starts.
func (c *Config) Validate() error {
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("port %d is out of range", c.Port)
	}
	if len(c.FilerAddresses) == 0 {
		return errors.New("at least one filer address is required")
	}
	switch c.TrustMode {
	case "", parquet_pushdown.TrustModeCatalogValidated:
		c.TrustMode = parquet_pushdown.TrustModeCatalogValidated
	case parquet_pushdown.TrustModeConnectorTrusted:
		if !c.ConnectorTrustedAck {
			return errors.New("connector-trusted is a developer-only trust mode; pass -dev.connector_trusted_ack to enable")
		}
	default:
		return fmt.Errorf("unknown trust mode %q", c.TrustMode)
	}
	return nil
}
