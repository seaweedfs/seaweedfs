package postgres

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func writePgMessage(conn net.Conn, msgType byte, payload []byte) error {
	var buf []byte
	if msgType != 0 {
		buf = append(buf, msgType)
	}
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(payload)+4))
	buf = append(buf, length...)
	buf = append(buf, payload...)
	_, err := conn.Write(buf)
	return err
}

func readPgMessage(r *bufio.Reader) (byte, []byte, error) {
	msgType, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	length := make([]byte, 4)
	if _, err := io.ReadFull(r, length); err != nil {
		return 0, nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(length)-4)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return msgType, body, nil
}

func runStartup(t *testing.T, method AuthMethod, user, password string) ([]byte, error) {
	t.Helper()
	s := &PostgreSQLServer{config: &PostgreSQLServerConfig{
		AuthMethod:     method,
		Users:          map[string]string{"admin": "secret"},
		StartupTimeout: 5 * time.Second,
		Database:       "default",
	}}
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	session := &PostgreSQLSession{
		conn:       serverConn,
		reader:     bufio.NewReader(serverConn),
		writer:     bufio.NewWriter(serverConn),
		parameters: make(map[string]string),
	}

	startupErr := make(chan error, 1)
	go func() {
		startupErr <- s.handleStartup(session)
		serverConn.Close()
	}()

	clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	startup := make([]byte, 4)
	binary.BigEndian.PutUint32(startup, PG_PROTOCOL_VERSION_3)
	startup = append(startup, []byte("user\x00"+user+"\x00database\x00default\x00\x00")...)
	if err := writePgMessage(clientConn, 0, startup); err != nil {
		t.Fatalf("write startup: %v", err)
	}

	reader := bufio.NewReader(clientConn)
	msgType, body, err := readPgMessage(reader)
	if err != nil || msgType != PG_RESP_AUTH_OK || len(body) < 4 {
		t.Fatalf("expected auth request, got %q %v", msgType, err)
	}
	response := password
	if method == AuthMD5 {
		inner := md5.Sum([]byte(password + user))
		response = fmt.Sprintf("md5%x", md5.Sum(append([]byte(fmt.Sprintf("%x", inner)), body[4:8]...)))
	}
	if err := writePgMessage(clientConn, PG_MSG_PASSWORD, []byte(response+"\x00")); err != nil {
		t.Fatalf("write password: %v", err)
	}

	var received []byte
	for {
		msgType, body, err := readPgMessage(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read after %q: %v", received, err)
		}
		if msgType == PG_RESP_ERROR && !strings.Contains(string(body), "\x00C28P01\x00") {
			t.Fatalf("expected SQLSTATE 28P01, got %q", body)
		}
		received = append(received, msgType)
	}
	return received, <-startupErr
}

func TestStartupRejectsFailedAuthentication(t *testing.T) {
	cases := []struct {
		name     string
		method   AuthMethod
		user     string
		password string
	}{
		{"password wrong", AuthPassword, "admin", "wrong"},
		{"password unknown user", AuthPassword, "nobody", "secret"},
		{"md5 wrong", AuthMD5, "admin", "wrong"},
		{"md5 unknown user", AuthMD5, "nobody", "secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			received, err := runStartup(t, tc.method, tc.user, tc.password)
			if err == nil {
				t.Fatalf("startup succeeded after failed authentication, server sent %q", received)
			}
			if string(received) != string([]byte{PG_RESP_ERROR}) {
				t.Fatalf("expected only an error response, got %q", received)
			}
		})
	}
}

func TestStartupAcceptsValidPassword(t *testing.T) {
	for _, method := range []AuthMethod{AuthPassword, AuthMD5} {
		received, err := runStartup(t, method, "admin", "secret")
		if err != nil {
			t.Fatalf("method %d: startup failed: %v", method, err)
		}
		if len(received) == 0 || received[len(received)-1] != PG_RESP_READY {
			t.Fatalf("method %d: expected ReadyForQuery last, got %q", method, received)
		}
	}
}
