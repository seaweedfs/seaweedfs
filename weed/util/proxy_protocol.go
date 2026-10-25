package util

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

// WrapProxyProtocolListener returns a listener whose accepted connections
// read and strip a HAProxy PROXY protocol v1 header line, exposing the
// advertised source address as the connection's RemoteAddr. Connections
// that do not start with a PROXY header keep their real remote address,
// so direct clients continue to work (and fail any address-based checks
// against their actual address).
//
// The header is parsed lazily on the first Read or RemoteAddr call, inside
// the connection's own read path; a silent or malformed peer occupies only
// its own connection, never the accept loop.
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
	return &proxyProtocolConn{Conn: conn}, nil
}

type proxyProtocolConn struct {
	net.Conn
	parseOnce sync.Once
	reader    *bufio.Reader
	remote    net.Addr
	headerErr error
}

func (c *proxyProtocolConn) Read(b []byte) (int, error) {
	c.parse()
	if c.headerErr != nil {
		return 0, c.headerErr
	}
	return c.reader.Read(b)
}

func (c *proxyProtocolConn) RemoteAddr() net.Addr {
	c.parse()
	if c.remote != nil {
		return c.remote
	}
	return c.Conn.RemoteAddr()
}

func (c *proxyProtocolConn) parse() {
	c.parseOnce.Do(func() {
		c.reader = bufio.NewReader(c.Conn)
		head, err := c.reader.Peek(len("PROXY "))
		if err != nil {
			if err != io.EOF {
				c.headerErr = err
			}
			return
		}
		if string(head) != "PROXY " {
			return
		}
		line, err := c.reader.ReadSlice('\n')
		if err != nil {
			// A truncated or overlong header means the peer never sent a
			// parseable line; keep the real address and let the buffered
			// bytes fail the downstream protocol on their own.
			c.discardHeaderLine()
			return
		}
		fields := strings.Fields(strings.TrimRight(string(line), "\r\n"))
		if len(fields) == 6 && fields[0] == "PROXY" && (fields[1] == "TCP4" || fields[1] == "TCP6") {
			if srcIP := net.ParseIP(fields[2]); srcIP != nil {
				if srcPort, err := strconv.Atoi(fields[4]); err == nil {
					c.remote = &net.TCPAddr{IP: srcIP, Port: srcPort}
				}
			}
		}
	})
}

// discardHeaderLine drops the remainder of a malformed PROXY line so the
// following bytes still reach the wrapped connection.
func (c *proxyProtocolConn) discardHeaderLine() {
	discard := make([]byte, c.reader.Buffered())
	_, _ = io.ReadFull(c.reader, discard)
}
