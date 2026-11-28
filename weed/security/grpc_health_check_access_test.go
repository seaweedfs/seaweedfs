package security

import (
	"context"
	"crypto/x509"
	"net"
	"testing"
	"time"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// startServerWithAllOptions starts an mTLS server with every option
// LoadServerTLS returns, as the SeaweedFS commands do.
func startServerWithAllOptions(t *testing.T, config *util.ViperProxy, component string) string {
	t.Helper()
	creds, healthCheckOnly := LoadServerTLS(config, component)
	if creds == nil {
		t.Fatal("LoadServerTLS returned nil")
	}
	opts := []grpc.ServerOption{creds}
	if healthCheckOnly != nil {
		opts = append(opts, healthCheckOnly)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(opts...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}

// callOther calls a method outside the health service. No such service is
// registered, so a client allowed past the access check gets Unimplemented.
func callOther(t *testing.T, addr string, dialOption grpc.DialOption) error {
	t.Helper()
	conn, err := grpc.NewClient(addr, dialOption)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return conn.Invoke(ctx, "/master_pb.Seaweed/VolumeList", &emptypb.Empty{}, &emptypb.Empty{})
}

func TestHealthCheckCommonNamesReachOnlyHealth(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	clientEku := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	serverCert, serverKey := ca.issue(t, dir, "master", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	v.Set("grpc.master.allowed_commonNames", "volume,both")
	v.Set("grpc.health_check_commonNames", "blackbox, both")
	for _, name := range []string{"volume", "blackbox", "both", "intruder"} {
		cert, key := ca.issue(t, dir, name, clientEku)
		v.Set("grpc."+name+".cert", cert)
		v.Set("grpc."+name+".key", key)
	}
	addr := startServerWithAllOptions(t, v, "grpc.master")

	tests := []struct {
		name       string
		healthOK   bool
		otherCode  codes.Code
		handshakes bool
	}{
		{name: "volume", healthOK: true, otherCode: codes.Unimplemented, handshakes: true},
		{name: "blackbox", healthOK: true, otherCode: codes.PermissionDenied, handshakes: true},
		// The narrower grant wins when a name is on both lists.
		{name: "both", healthOK: true, otherCode: codes.PermissionDenied, handshakes: true},
		{name: "intruder"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dial := LoadClientTLS(v, "grpc."+tt.name)
			err := healthCheck(t, addr, dial)
			if tt.healthOK != (err == nil) {
				t.Fatalf("health check: got err=%v, want ok=%v", err, tt.healthOK)
			}
			err = callOther(t, addr, dial)
			if !tt.handshakes {
				if err == nil || status.Code(err) != codes.Unavailable {
					t.Fatalf("expected the handshake to fail, got %v", err)
				}
				return
			}
			if got := status.Code(err); got != tt.otherCode {
				t.Fatalf("other RPC: got %v (%v), want %v", got, err, tt.otherCode)
			}
		})
	}
}

// With no allowed_commonNames every CA-signed certificate has full access, so
// the health check list is what keeps the probe's certificate narrow.
func TestHealthCheckCommonNamesWithoutAllowlist(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	clientEku := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	serverCert, serverKey := ca.issue(t, dir, "master", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	v.Set("grpc.health_check_commonNames", "blackbox")
	for _, name := range []string{"volume", "blackbox"} {
		cert, key := ca.issue(t, dir, name, clientEku)
		v.Set("grpc."+name+".cert", cert)
		v.Set("grpc."+name+".key", key)
	}
	addr := startServerWithAllOptions(t, v, "grpc.master")

	if err := healthCheck(t, addr, LoadClientTLS(v, "grpc.blackbox")); err != nil {
		t.Fatalf("probe health check: %v", err)
	}
	if got := status.Code(callOther(t, addr, LoadClientTLS(v, "grpc.blackbox"))); got != codes.PermissionDenied {
		t.Fatalf("probe other RPC: got %v, want PermissionDenied", got)
	}
	if got := status.Code(callOther(t, addr, LoadClientTLS(v, "grpc.volume"))); got != codes.Unimplemented {
		t.Fatalf("component other RPC: got %v, want Unimplemented", got)
	}
}

// Unset, LoadServerTLS adds no option and the server behaves as before.
func TestHealthCheckCommonNamesUnsetAddsNoOption(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	serverCert, serverKey := ca.issue(t, dir, "master", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	v := util.NewViperProxy(viper.New())
	v.Set("grpc.ca", ca.file)
	v.Set("grpc.master.cert", serverCert)
	v.Set("grpc.master.key", serverKey)
	if _, opt := LoadServerTLS(v, "grpc.master"); opt != nil {
		t.Fatal("expected no extra server option without grpc.health_check_commonNames")
	}
}
