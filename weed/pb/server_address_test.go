package pb

import (
	"reflect"
	"testing"
)

func TestServerAddresses_ToAddressMapOrSrv_shouldRemovePrefix(t *testing.T) {
	str := ServerAddresses("dnssrv+hello.srv.consul")

	_, srv := str.ToAddressMapOrSrv()

	expected := ServerSrvAddress("hello.srv.consul")
	if *srv != expected {
		t.Fatalf(`ServerAddresses("dnssrv+hello.srv.consul") = %s, expected %s`, *srv, expected)
	}
}

func TestServerAddresses_ToAddressMapOrSrv_shouldHandleIPPortList(t *testing.T) {
	str := ServerAddresses("10.0.0.1:23,10.0.0.2:24")

	m, srv := str.ToAddressMapOrSrv()

	if srv != nil {
		t.Fatalf(`ServerAddresses("dnssrv+hello.srv.consul") = %s, expected nil`, *srv)
	}

	expected := map[string]ServerAddress{
		"10.0.0.1:23": ServerAddress("10.0.0.1:23"),
		"10.0.0.2:24": ServerAddress("10.0.0.2:24"),
	}

	if !reflect.DeepEqual(m, expected) {
		t.Fatalf(`Expected %q, got %q`, expected, m)
	}
}
