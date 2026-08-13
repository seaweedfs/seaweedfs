// Package volume_move implements the volume and EC shard move sequences shared
// by the interactive shell commands (volume.move, volume.balance, ec.balance,
// volume.tier.move, ...) and the maintenance workers (balance, ec_balance,
// volume_tiering). The RPCs go through an injectable ClientFunc so every
// sequence can be unit tested against a fake volume server client.
package volume_move

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

// ClientFunc runs fn with a client for the volume server at addr. It matches
// operation.WithVolumeServerClient, which production movers use to dial real
// servers; tests substitute a fake client.
type ClientFunc func(streamingMode bool, addr pb.ServerAddress, fn func(client volume_server_pb.VolumeServerClient) error) error

// Mover executes volume and EC shard moves against volume servers.
type Mover struct {
	withClient ClientFunc
}

func NewMover(grpcDialOption grpc.DialOption) *Mover {
	return &Mover{withClient: func(streamingMode bool, addr pb.ServerAddress, fn func(client volume_server_pb.VolumeServerClient) error) error {
		// Validated here, the single point every mover RPC dials through:
		// handing a malformed address (an unvalidated -source/-target flag)
		// to the dialer would abort the whole process instead of failing the
		// move.
		if err := checkDialable(addr); err != nil {
			return err
		}
		return operation.WithVolumeServerClient(streamingMode, addr, grpcDialOption, fn)
	}}
}

// checkDialable rejects an address whose grpc normalization would abort the
// process: for the "host:port" form, ServerAddress.ToGrpcAddress falls back
// to a parser that calls glog.Fatalf when the port is not numeric. Everything
// else either normalizes cleanly or fails at dial time as an ordinary error.
// It also guards the source addresses embedded in copy/tail requests: the
// receiving volume server dials those through the same fatal parser, so an
// unchecked malformed source would terminate the destination server.
func checkDialable(addr pb.ServerAddress) error {
	s := string(addr)
	colon := strings.LastIndex(s, ":")
	if colon < 0 || colon+1 >= len(s) {
		return nil // no port part; handed to the dialer untouched
	}
	ports := s[colon+1:]
	if dot := strings.LastIndex(ports, "."); dot >= 0 {
		// "port.grpcPort": different dial paths canonicalize a half-malformed
		// form differently (the method splices the grpc part in unparsed; the
		// string helper falls back to the http part + 10000), so a bad
		// component could dial an unintended server or reach the fatal
		// parser. Require both components numeric.
		if _, err := strconv.ParseUint(ports[:dot], 10, 64); err != nil {
			return fmt.Errorf("invalid volume server address %q: port %q is not a number", s, ports[:dot])
		}
		if _, err := strconv.ParseUint(ports[dot+1:], 10, 64); err != nil {
			return fmt.Errorf("invalid volume server address %q: grpc port %q is not a number", s, ports[dot+1:])
		}
		return nil
	}
	if _, err := strconv.ParseUint(ports, 10, 64); err != nil {
		return fmt.Errorf("invalid volume server address %q: port %q is not a number", s, ports)
	}
	return nil
}

// NewMoverWithClientFunc builds a Mover on a custom transport.
func NewMoverWithClientFunc(withClient ClientFunc) *Mover {
	return &Mover{withClient: withClient}
}

// SameServer reports whether two addresses name the same volume server. The
// gRPC endpoint identifies the server process — each has exactly one — so
// "node:8080" and "node:8080.18080" compare equal, while two servers sharing
// a degenerate HTTP address (e.g. port 0 in test harnesses) stay distinct.
// The normalization is non-fatal, unlike ServerAddress.ToGrpcAddress, whose
// parser exits the process on a malformed port; anything unparsable compares
// as its literal self.
func SameServer(a, b pb.ServerAddress) bool {
	return grpcEndpoint(string(a)) == grpcEndpoint(string(b))
}

// grpcEndpoint mirrors ServerAddress.ToGrpcAddress ("host:port" gets the
// +10000 grpc port; "host:port.grpcPort" names it explicitly) but returns a
// malformed address unchanged instead of exiting.
func grpcEndpoint(addr string) string {
	colon := strings.LastIndex(addr, ":")
	if colon < 0 || colon+1 >= len(addr) {
		return addr
	}
	host, ports := addr[:colon], addr[colon+1:]
	if dot := strings.LastIndex(ports, "."); dot >= 0 {
		if grpcPort, err := strconv.Atoi(ports[dot+1:]); err == nil {
			return util.JoinHostPort(host, grpcPort)
		}
		return addr
	}
	httpPort, err := strconv.Atoi(ports)
	if err != nil {
		return addr
	}
	return util.JoinHostPort(host, httpPort+10000)
}
