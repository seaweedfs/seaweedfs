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

func TestServerAddress_ToHost(t *testing.T) {
	testCases := []struct {
		name     string
		address  ServerAddress
		expected string
	}{
		{
			name:     "hostname with port",
			address:  ServerAddress("master.example.com:9333"),
			expected: "master.example.com",
		},
		{
			name:     "IPv4 with port",
			address:  ServerAddress("192.168.1.1:9333"),
			expected: "192.168.1.1",
		},
		{
			name:     "IPv6 with port",
			address:  ServerAddress("[2001:db8::1]:9333"),
			expected: "2001:db8::1",
		},
		{
			name:     "hostname without port",
			address:  ServerAddress("master.example.com"),
			expected: "master.example.com",
		},
		{
			name:     "hostname with port.grpcPort",
			address:  ServerAddress("master.example.com:443.10443"),
			expected: "master.example.com",
		},
		{
			name:     "IPv4 with port.grpcPort",
			address:  ServerAddress("192.168.1.1:8080.18080"),
			expected: "192.168.1.1",
		},
		{
			name:     "IPv6 with port.grpcPort",
			address:  ServerAddress("[2001:db8::1]:8080.18080"),
			expected: "2001:db8::1",
		},
		{
			name:     "bracketed IPv6 without port",
			address:  ServerAddress("[2001:db8::1]"),
			expected: "2001:db8::1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.address.ToHost()
			if got != tc.expected {
				t.Errorf("ServerAddress(%q).ToHost() = %q, want %q", tc.address, got, tc.expected)
			}
		})
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
