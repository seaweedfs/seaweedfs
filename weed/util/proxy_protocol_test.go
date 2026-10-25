package util

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"
)

func acceptOne(t *testing.T, ln net.Listener) net.Conn {
	t.Helper()
	accept := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accept <- conn
		}
	}()
	select {
	case conn := <-accept:
		return conn
	case <-time.After(2 * time.Second):
		t.Fatal("Accept blocked waiting for the peer to send data")
		return nil
	}
}

func newPipePair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	serverConn, client := net.Pipe()
	ln := WrapProxyProtocolListener(&oneShotListener{conn: serverConn})
	return client, acceptOne(t, ln)
}

type oneShotListener struct {
	conn net.Conn
	done bool
}

func (l *oneShotListener) Accept() (net.Conn, error) {
	if l.done {
		return nil, io.EOF
	}
	l.done = true
	return l.conn, nil
}
func (l *oneShotListener) Close() error   { return l.conn.Close() }
func (l *oneShotListener) Addr() net.Addr { return l.conn.LocalAddr() }

func TestProxyProtocolHeaderRewritesRemoteAddr(t *testing.T) {
	client, server := newPipePair(t)
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = client.Write([]byte("PROXY TCP4 10.99.0.1 192.0.2.5 45678 8080\r\nGET / HTTP/1.0\r\n\r\n"))
	}()

	got, err := bufio.NewReader(server).ReadString('\n')
	if err != nil {
		t.Fatalf("read request line: %v", err)
	}
	if got != "GET / HTTP/1.0\r\n" {
		t.Fatalf("request line = %q, want GET / HTTP/1.0", got)
	}
	if addr, ok := server.RemoteAddr().(*net.TCPAddr); !ok || addr.IP.String() != "10.99.0.1" || addr.Port != 45678 {
		t.Fatalf("RemoteAddr = %v, want 10.99.0.1:45678", server.RemoteAddr())
	}
}

func TestProxyProtocolAbsentKeepsRemoteAddr(t *testing.T) {
	client, server := newPipePair(t)
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
	}()

	got, err := bufio.NewReader(server).ReadString('\n')
	if err != nil {
		t.Fatalf("read request line: %v", err)
	}
	if got != "GET / HTTP/1.0\r\n" {
		t.Fatalf("request line = %q, want GET / HTTP/1.0", got)
	}
	if server.RemoteAddr().String() != "pipe" {
		t.Fatalf("RemoteAddr = %v, want underlying conn address", server.RemoteAddr())
	}
}

func TestProxyProtocolTruncatedHeaderKeepsRemoteAddr(t *testing.T) {
	client, server := newPipePair(t)
	defer server.Close()

	go func() {
		_, _ = client.Write([]byte("PROXY TCP4 10.99.0.1"))
		_ = client.Close()
	}()

	_, _ = server.Read(make([]byte, 64))
	if server.RemoteAddr().String() != "pipe" {
		t.Fatalf("RemoteAddr = %v, want underlying conn address", server.RemoteAddr())
	}
}
