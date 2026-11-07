package command

import (
	"context"
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
	s3opt.lanceServer = s3opt.newLanceServer(s3a)
	if s := s3opt.startLanceServer(false); s != nil {
		s.Close()
		t.Error("startLanceServer on an occupied port should return nil")
	}
	if s3opt.lanceBound.Load() {
		t.Error("lanceBound must stay false when the listener failed to bind")
	}
}

// -iceberg.credentialRole reaches the S3 server once, before either catalog
// is built, so a gateway that serves no Iceberg port vends too.
func TestCredentialRoleIsSetForBothCatalogs(t *testing.T) {
	bind := "127.0.0.1"
	empty := ""
	zero := 0
	role := "arn:aws:iam::000000000000:role/vend"
	s3opt := &S3Options{
		bindIp:                &bind,
		ip:                    &bind,
		port:                  &zero,
		portHttps:             &zero,
		portIceberg:           &zero,
		portLance:             &zero,
		icebergCredentialRole: &role,
		externalUrl:           &empty,
		tlsPrivateKey:         &empty,
	}
	s3a := &s3api.S3ApiServer{}
	s3opt.setCredentialRole(s3a)
	if !s3opt.vendsCredentials() {
		t.Fatal("a role is configured but vending is reported off")
	}
	// With no role, vending is off and answers nil without checking anything.
	if _, err := s3a.VendTableCredentials(context.Background(), "", "bkt", "vector/x", true); err == nil {
		t.Fatal("vending is off: the credential role was not set")
	}
}
