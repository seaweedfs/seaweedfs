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
