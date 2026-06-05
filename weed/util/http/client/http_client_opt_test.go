package client

import (
	"net"
	"testing"
)

func TestNewDialerSetsLocalAddress(t *testing.T) {
	ip := net.ParseIP("127.0.0.1")
	dialer := newDialer(ip)

	addr, ok := dialer.LocalAddr.(*net.TCPAddr)
	if !ok {
		t.Fatalf("LocalAddr = %T, want *net.TCPAddr", dialer.LocalAddr)
	}
	if !addr.IP.Equal(ip) {
		t.Fatalf("LocalAddr IP = %v, want %v", addr.IP, ip)
	}
}

func TestNewDialerSkipsUnspecifiedLocalAddress(t *testing.T) {
	dialer := newDialer(net.ParseIP("0.0.0.0"))
	if dialer.LocalAddr != nil {
		t.Fatalf("LocalAddr = %v, want nil", dialer.LocalAddr)
	}
}

func TestDefaultDialLocalAddressCopiesInput(t *testing.T) {
	original := net.ParseIP("127.0.0.1")
	SetDefaultDialLocalAddress(original)
	t.Cleanup(func() { SetDefaultDialLocalAddress(nil) })

	original[0] = 0xff
	got := getDefaultDialLocalAddress()
	if !got.Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("default dial local address = %v, want 127.0.0.1", got)
	}
}
