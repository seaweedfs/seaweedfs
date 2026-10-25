package command

import (
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
)

func holdPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l.Addr().(*net.TCPAddr).Port
}

// An occupied auxiliary catalog port must not take the S3 gateway down, must
// return no server for shutdown registration, and must not report the catalog
// as bound for mini's readiness checks.
func TestAuxListenerBindFailureIsNonFatal(t *testing.T) {
	icebergPort := holdPort(t)
	lancePort := holdPort(t)

	bind := "127.0.0.1"
	empty := ""
	zero := 0
	idle := 10
	s3opt := &S3Options{
		bindIp:                &bind,
		ip:                    &bind,
		port:                  &zero,
		portHttps:             &zero,
		portIceberg:           &icebergPort,
		portLance:             &lancePort,
		idleTimeout:           &idle,
		icebergCredentialRole: &empty,
		externalUrl:           &empty,
		tlsPrivateKey:         &empty,
	}
	s3a := &s3api.S3ApiServer{}

	if s := s3opt.startIcebergServer(s3a, false); s != nil {
		s.Close()
		t.Error("startIcebergServer on an occupied port should return nil")
	}
	if s3opt.icebergBound.Load() {
		t.Error("icebergBound must stay false when the listener failed to bind")
	}
	if s := s3opt.startLanceServer(s3a, false); s != nil {
		s.Close()
		t.Error("startLanceServer on an occupied port should return nil")
	}
	if s3opt.lanceBound.Load() {
		t.Error("lanceBound must stay false when the listener failed to bind")
	}
}
