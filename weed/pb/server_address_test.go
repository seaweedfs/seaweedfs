package pb

import (
	"reflect"
	"testing"
)

func TestServerAddresses_ToAddressMapOrSrv_shouldRemovePrefix(t *testing.T) {
	str := ServerAddresses("dnssrv+hello.srv.consul")

	d := str.ToServiceDiscovery()

	expected := ServerSrvAddress("hello.srv.consul")
	if *d.srvRecord != expected {
		t.Fatalf(`ServerAddresses("dnssrv+hello.srv.consul") = %s, expected %s`, *d.srvRecord, expected)
	}
}

func TestServerAddresses_ToAddressMapOrSrv_shouldHandleIPPortList(t *testing.T) {
	str := ServerAddresses("10.0.0.1:23,10.0.0.2:24")

	d := str.ToServiceDiscovery()

	if d.srvRecord != nil {
		t.Fatalf(`ServerAddresses("dnssrv+hello.srv.consul") = %s, expected nil`, *d.srvRecord)
	}

	expected := []ServerAddress{
		ServerAddress("10.0.0.1:23"),
		ServerAddress("10.0.0.2:24"),
	}

	if !reflect.DeepEqual(d.list, expected) {
		t.Fatalf(`Expected %q, got %q`, expected, d.list)
	}
}

func TestIPv6ServerAddressFormatting(t *testing.T) {
	testCases := []struct {
		name         string
		sa           ServerAddress
		expectedHttp string
		expectedGrpc string
	}{
		{
			name:         "unbracketed IPv6",
			sa:           NewServerAddress("2001:db8::1", 8080, 18080),
			expectedHttp: "[2001:db8::1]:8080",
			expectedGrpc: "[2001:db8::1]:18080",
		},
		{
			name:         "bracketed IPv6",
			sa:           NewServerAddressWithGrpcPort("[2001:db8::1]:8080", 18080),
			expectedHttp: "[2001:db8::1]:8080",
			expectedGrpc: "[2001:db8::1]:18080",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if httpAddr := tc.sa.ToHttpAddress(); httpAddr != tc.expectedHttp {
				t.Errorf("%s: ToHttpAddress() = %s, want %s", tc.name, httpAddr, tc.expectedHttp)
			}
			if grpcAddr := tc.sa.ToGrpcAddress(); grpcAddr != tc.expectedGrpc {
				t.Errorf("%s: ToGrpcAddress() = %s, want %s", tc.name, grpcAddr, tc.expectedGrpc)
			}
		})
	}
}
