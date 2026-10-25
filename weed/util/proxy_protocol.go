package util

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
)

// WrapProxyProtocolListener returns a listener whose accepted connections
// read and strip a HAProxy PROXY protocol v1 header line, exposing the
// advertised source address as the connection's RemoteAddr. Connections
// that do not start with a PROXY header keep their real remote address,
// so direct clients continue to work (and fail any address-based checks
// against their actual address).
//
// Only enable this on listeners reachable solely through a trusted proxy:
// any direct client could otherwise spoof its reported address.
func WrapProxyProtocolListener(listener net.Listener) net.Listener {
	return &proxyProtocolListener{Listener: listener}
}

type proxyProtocolListener struct {
	net.Listener
}

func (l *proxyProtocolListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	proxyConn, err := readProxyProtocolHeader(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return proxyConn, nil
}

type proxyProtocolConn struct {
	net.Conn
	reader *bufio.Reader
	remote net.Addr
}

func (c *proxyProtocolConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

func (c *proxyProtocolConn) RemoteAddr() net.Addr {
	if c.remote != nil {
		return c.remote
	}
	return c.Conn.RemoteAddr()
}

func readProxyProtocolHeader(conn net.Conn) (net.Conn, error) {
	reader := bufio.NewReader(conn)
	proxyConn := &proxyProtocolConn{Conn: conn, reader: reader}

	head, err := reader.Peek(len("PROXY "))
	if err != nil {
		if err == io.EOF {
			return proxyConn, nil
		}
		return nil, err
	}
	if string(head) != "PROXY " {
		return proxyConn, nil
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(strings.TrimRight(line, "\r\n"))
	if len(fields) == 6 && fields[0] == "PROXY" && (fields[1] == "TCP4" || fields[1] == "TCP6") {
		if srcIP := net.ParseIP(fields[2]); srcIP != nil {
			if srcPort, err := strconv.Atoi(fields[4]); err == nil {
				proxyConn.remote = &net.TCPAddr{IP: srcIP, Port: srcPort}
			}
		}
	}
	return proxyConn, nil
}
