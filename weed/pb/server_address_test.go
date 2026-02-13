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

func TestIPv6ServerAddress(t *testing.T) {
	host := "2001:db8::1"
	port := 8080
	grpcPort := 18080
	sa := NewServerAddress(host, port, grpcPort)

	expectedHttp := "[2001:db8::1]:8080"
	if httpAddr := sa.ToHttpAddress(); httpAddr != expectedHttp {
		t.Errorf("ToHttpAddress() = %s, want %s", httpAddr, expectedHttp)
	}

	expectedGrpc := "[2001:db8::1]:18080"
	if grpcAddr := sa.ToGrpcAddress(); grpcAddr != expectedGrpc {
		t.Errorf("ToGrpcAddress() = %s, want %s", grpcAddr, expectedGrpc)
	}
}

func TestIPv6ServerAddressWithBrackets(t *testing.T) {
	host := "[2001:db8::1]"
	grpcPort := 18080
	sa := NewServerAddressWithGrpcPort(host+":8080", grpcPort)

	expectedHttp := "[2001:db8::1]:8080"
	if httpAddr := sa.ToHttpAddress(); httpAddr != expectedHttp {
		t.Errorf("ToHttpAddress() = %s, want %s", httpAddr, expectedHttp)
	}

	expectedGrpc := "[2001:db8::1]:18080"
	if grpcAddr := sa.ToGrpcAddress(); grpcAddr != expectedGrpc {
		t.Errorf("ToGrpcAddress() = %s, want %s", grpcAddr, expectedGrpc)
	}
}
