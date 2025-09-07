package postgres

import (
	"bufio"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/query/engine"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

// PostgreSQL protocol constants
const (
	// Protocol versions
	PG_PROTOCOL_VERSION_3 = 196608   // PostgreSQL 3.0 protocol (0x00030000)
	PG_SSL_REQUEST        = 80877103 // SSL request (0x04d2162f)
	PG_GSSAPI_REQUEST     = 80877104 // GSSAPI request (0x04d21630)

	// Message types from client
	PG_MSG_STARTUP   = 0x00
	PG_MSG_QUERY     = 'Q'
	PG_MSG_PARSE     = 'P'
	PG_MSG_BIND      = 'B'
	PG_MSG_EXECUTE   = 'E'
	PG_MSG_DESCRIBE  = 'D'
	PG_MSG_CLOSE     = 'C'
	PG_MSG_FLUSH     = 'H'
	PG_MSG_SYNC      = 'S'
	PG_MSG_TERMINATE = 'X'
	PG_MSG_PASSWORD  = 'p'

	// Response types to client
	PG_RESP_AUTH_OK        = 'R'
	PG_RESP_BACKEND_KEY    = 'K'
	PG_RESP_PARAMETER      = 'S'
	PG_RESP_READY          = 'Z'
	PG_RESP_COMMAND        = 'C'
	PG_RESP_DATA_ROW       = 'D'
	PG_RESP_ROW_DESC       = 'T'
	PG_RESP_PARSE_COMPLETE = '1'
	PG_RESP_BIND_COMPLETE  = '2'
	PG_RESP_CLOSE_COMPLETE = '3'
	PG_RESP_ERROR          = 'E'
	PG_RESP_NOTICE         = 'N'

	// Transaction states
	PG_TRANS_IDLE    = 'I'
	PG_TRANS_INTRANS = 'T'
	PG_TRANS_ERROR   = 'E'

	// Authentication methods
	AUTH_OK    = 0
	AUTH_CLEAR = 3
	AUTH_MD5   = 5
	AUTH_TRUST = 10

	// PostgreSQL data types
	PG_TYPE_BOOL      = 16
	PG_TYPE_BYTEA     = 17
	PG_TYPE_INT8      = 20
	PG_TYPE_INT4      = 23
	PG_TYPE_TEXT      = 25
	PG_TYPE_FLOAT4    = 700
	PG_TYPE_FLOAT8    = 701
	PG_TYPE_VARCHAR   = 1043
	PG_TYPE_TIMESTAMP = 1114
	PG_TYPE_JSON      = 114
	PG_TYPE_JSONB     = 3802

	// Default values
	DEFAULT_POSTGRES_PORT = 5432
)

// Authentication method type
type AuthMethod int

const (
	AuthTrust AuthMethod = iota
	AuthPassword
	AuthMD5
)

// PostgreSQL server configuration
type PostgreSQLServerConfig struct {
	Host           string
	Port           int
	AuthMethod     AuthMethod
	Users          map[string]string
	TLSConfig      *tls.Config
	MaxConns       int
	IdleTimeout    time.Duration
	StartupTimeout time.Duration // Timeout for client startup handshake
	Database       string
}

// PostgreSQL server
type PostgreSQLServer struct {
	config     *PostgreSQLServerConfig
	listener   net.Listener
	sqlEngine  *engine.SQLEngine
	sessions   map[uint32]*PostgreSQLSession
	sessionMux sync.RWMutex
	shutdown   chan struct{}
	wg         sync.WaitGroup
	nextConnID uint32
}

// PostgreSQL session
type PostgreSQLSession struct {
	conn             net.Conn
	reader           *bufio.Reader
	writer           *bufio.Writer
	authenticated    bool
	username         string
	database         string
	parameters       map[string]string
	preparedStmts    map[string]*PreparedStatement
	portals          map[string]*Portal
	transactionState byte
	processID        uint32
	secretKey        uint32
	created          time.Time
	lastActivity     time.Time
	mutex            sync.Mutex
}

// Prepared statement
type PreparedStatement struct {
	Name       string
	Query      string
	ParamTypes []uint32
	Fields     []FieldDescription
}

// Portal (cursor)
type Portal struct {
	Name       string
	Statement  string
	Parameters [][]byte
	Suspended  bool
}

// Field description
type FieldDescription struct {
	Name     string
	TableOID uint32
	AttrNum  int16
	TypeOID  uint32
	TypeSize int16
	TypeMod  int32
	Format   int16
}

// NewPostgreSQLServer creates a new PostgreSQL protocol server
func NewPostgreSQLServer(config *PostgreSQLServerConfig, masterAddr string) (*PostgreSQLServer, error) {
	if config.Port <= 0 {
		config.Port = DEFAULT_POSTGRES_PORT
	}
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Database == "" {
		config.Database = "default"
	}
	if config.MaxConns <= 0 {
		config.MaxConns = 100
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = time.Hour
	}
	if config.StartupTimeout <= 0 {
		config.StartupTimeout = 30 * time.Second
	}

	// Create SQL engine (now uses CockroachDB parser for PostgreSQL compatibility)
	sqlEngine := engine.NewSQLEngine(masterAddr)

	server := &PostgreSQLServer{
		config:     config,
		sqlEngine:  sqlEngine,
		sessions:   make(map[uint32]*PostgreSQLSession),
		shutdown:   make(chan struct{}),
		nextConnID: 1,
	}

	return server, nil
}

// Start begins listening for PostgreSQL connections
func (s *PostgreSQLServer) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)

	var listener net.Listener
	var err error

	if s.config.TLSConfig != nil {
		listener, err = tls.Listen("tcp", addr, s.config.TLSConfig)
		glog.Infof("PostgreSQL Server with TLS listening on %s", addr)
	} else {
		listener, err = net.Listen("tcp", addr)
		glog.Infof("PostgreSQL Server listening on %s", addr)
	}

	if err != nil {
		return fmt.Errorf("failed to start PostgreSQL server on %s: %v", addr, err)
	}

	s.listener = listener

	// Start accepting connections
	s.wg.Add(1)
	go s.acceptConnections()

	// Start cleanup routine
	s.wg.Add(1)
	go s.cleanupSessions()

	return nil
}

// Stop gracefully shuts down the PostgreSQL server
func (s *PostgreSQLServer) Stop() error {
	close(s.shutdown)

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all sessions
	s.sessionMux.Lock()
	for _, session := range s.sessions {
		session.close()
	}
	s.sessions = make(map[uint32]*PostgreSQLSession)
	s.sessionMux.Unlock()

	s.wg.Wait()
	glog.Infof("PostgreSQL Server stopped")
	return nil
}

// acceptConnections handles incoming PostgreSQL connections
func (s *PostgreSQLServer) acceptConnections() {
	defer s.wg.Done()

	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return
			default:
				glog.Errorf("Failed to accept PostgreSQL connection: %v", err)
				continue
			}
		}

		// Check connection limit
		s.sessionMux.RLock()
		sessionCount := len(s.sessions)
		s.sessionMux.RUnlock()

		if sessionCount >= s.config.MaxConns {
			glog.Warningf("Maximum connections reached (%d), rejecting connection from %s",
				s.config.MaxConns, conn.RemoteAddr())
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection processes a single PostgreSQL connection
func (s *PostgreSQLServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// Generate unique connection ID
	connID := s.generateConnectionID()
	secretKey := s.generateSecretKey()

	// Create session
	session := &PostgreSQLSession{
		conn:             conn,
		reader:           bufio.NewReader(conn),
		writer:           bufio.NewWriter(conn),
		authenticated:    false,
		database:         s.config.Database,
		parameters:       make(map[string]string),
		preparedStmts:    make(map[string]*PreparedStatement),
		portals:          make(map[string]*Portal),
		transactionState: PG_TRANS_IDLE,
		processID:        connID,
		secretKey:        secretKey,
		created:          time.Now(),
		lastActivity:     time.Now(),
	}

	// Register session
	s.sessionMux.Lock()
	s.sessions[connID] = session
	s.sessionMux.Unlock()

	// Clean up on exit
	defer func() {
		s.sessionMux.Lock()
		delete(s.sessions, connID)
		s.sessionMux.Unlock()
	}()

	glog.V(2).Infof("New PostgreSQL connection from %s (ID: %d)", conn.RemoteAddr(), connID)

	// Handle startup
	err := s.handleStartup(session)
	if err != nil {
		// Handle common disconnection scenarios more gracefully
		if strings.Contains(err.Error(), "client disconnected") {
			glog.V(1).Infof("Client startup disconnected from %s (ID: %d): %v", conn.RemoteAddr(), connID, err)
		} else if strings.Contains(err.Error(), "timeout") {
			glog.Warningf("Startup timeout for connection %d from %s: %v", connID, conn.RemoteAddr(), err)
		} else {
			glog.Errorf("Startup failed for connection %d from %s: %v", connID, conn.RemoteAddr(), err)
		}
		return
	}

	// Handle messages
	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		// Set read timeout
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		err := s.handleMessage(session)
		if err != nil {
			if err == io.EOF {
				glog.Infof("PostgreSQL client disconnected (ID: %d)", connID)
			} else {
				glog.Errorf("Error handling PostgreSQL message (ID: %d): %v", connID, err)
			}
			return
		}

		session.lastActivity = time.Now()
	}
}

// handleStartup processes the PostgreSQL startup sequence
func (s *PostgreSQLServer) handleStartup(session *PostgreSQLSession) error {
	// Set a startup timeout to prevent hanging connections
	startupTimeout := s.config.StartupTimeout
	session.conn.SetReadDeadline(time.Now().Add(startupTimeout))
	defer session.conn.SetReadDeadline(time.Time{}) // Clear timeout

	for {
		// Read startup message length
		length := make([]byte, 4)
		_, err := io.ReadFull(session.reader, length)
		if err != nil {
			if err == io.EOF {
				// Client disconnected during startup - this is common for health checks
				return fmt.Errorf("client disconnected during startup handshake")
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return fmt.Errorf("startup handshake timeout after %v", startupTimeout)
			}
			return fmt.Errorf("failed to read message length during startup: %v", err)
		}

		msgLength := binary.BigEndian.Uint32(length) - 4
		if msgLength > 10000 { // Reasonable limit for startup messages
			return fmt.Errorf("startup message too large: %d bytes", msgLength)
		}

		// Read startup message content
		msg := make([]byte, msgLength)
		_, err = io.ReadFull(session.reader, msg)
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("client disconnected while reading startup message")
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return fmt.Errorf("startup message read timeout")
			}
			return fmt.Errorf("failed to read startup message: %v", err)
		}

		// Parse protocol version
		protocolVersion := binary.BigEndian.Uint32(msg[0:4])

		switch protocolVersion {
		case PG_SSL_REQUEST:
			// Reject SSL request - send 'N' to indicate SSL not supported
			_, err = session.conn.Write([]byte{'N'})
			if err != nil {
				return fmt.Errorf("failed to reject SSL request: %v", err)
			}
			// Continue loop to read the actual startup message
			continue

		case PG_GSSAPI_REQUEST:
			// Reject GSSAPI request - send 'N' to indicate GSSAPI not supported
			_, err = session.conn.Write([]byte{'N'})
			if err != nil {
				return fmt.Errorf("failed to reject GSSAPI request: %v", err)
			}
			// Continue loop to read the actual startup message
			continue

		case PG_PROTOCOL_VERSION_3:
			// This is the actual startup message, break out of loop
			break

		default:
			return fmt.Errorf("unsupported protocol version: %d", protocolVersion)
		}

		// Parse parameters
		params := strings.Split(string(msg[4:]), "\x00")
		for i := 0; i < len(params)-1; i += 2 {
			if params[i] == "user" {
				session.username = params[i+1]
			} else if params[i] == "database" {
				session.database = params[i+1]
			}
			session.parameters[params[i]] = params[i+1]
		}

		// Break out of the main loop - we have the startup message
		break
	}

	// Handle authentication
	err := s.handleAuthentication(session)
	if err != nil {
		return err
	}

	// Send parameter status messages
	err = s.sendParameterStatus(session, "server_version", fmt.Sprintf("%s (SeaweedFS)", version.VERSION_NUMBER))
	if err != nil {
		return err
	}
	err = s.sendParameterStatus(session, "server_encoding", "UTF8")
	if err != nil {
		return err
	}
	err = s.sendParameterStatus(session, "client_encoding", "UTF8")
	if err != nil {
		return err
	}
	err = s.sendParameterStatus(session, "DateStyle", "ISO, MDY")
	if err != nil {
		return err
	}
	err = s.sendParameterStatus(session, "integer_datetimes", "on")
	if err != nil {
		return err
	}

	// Send backend key data
	err = s.sendBackendKeyData(session)
	if err != nil {
		return err
	}

	// Send ready for query
	err = s.sendReadyForQuery(session)
	if err != nil {
		return err
	}

	session.authenticated = true
	return nil
}

// handleAuthentication processes authentication
func (s *PostgreSQLServer) handleAuthentication(session *PostgreSQLSession) error {
	switch s.config.AuthMethod {
	case AuthTrust:
		return s.sendAuthenticationOk(session)
	case AuthPassword:
		return s.handlePasswordAuth(session)
	case AuthMD5:
		return s.handleMD5Auth(session)
	default:
		return fmt.Errorf("unsupported authentication method")
	}
}

// sendAuthenticationOk sends authentication OK message
func (s *PostgreSQLServer) sendAuthenticationOk(session *PostgreSQLSession) error {
	msg := make([]byte, 9)
	msg[0] = PG_RESP_AUTH_OK
	binary.BigEndian.PutUint32(msg[1:5], 8)
	binary.BigEndian.PutUint32(msg[5:9], AUTH_OK)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// handlePasswordAuth handles clear password authentication
func (s *PostgreSQLServer) handlePasswordAuth(session *PostgreSQLSession) error {
	// Send password request
	msg := make([]byte, 9)
	msg[0] = PG_RESP_AUTH_OK
	binary.BigEndian.PutUint32(msg[1:5], 8)
	binary.BigEndian.PutUint32(msg[5:9], AUTH_CLEAR)

	_, err := session.writer.Write(msg)
	if err != nil {
		return err
	}
	err = session.writer.Flush()
	if err != nil {
		return err
	}

	// Read password response
	msgType := make([]byte, 1)
	_, err = io.ReadFull(session.reader, msgType)
	if err != nil {
		return err
	}

	if msgType[0] != PG_MSG_PASSWORD {
		return fmt.Errorf("expected password message, got %c", msgType[0])
	}

	length := make([]byte, 4)
	_, err = io.ReadFull(session.reader, length)
	if err != nil {
		return err
	}

	msgLength := binary.BigEndian.Uint32(length) - 4
	password := make([]byte, msgLength)
	_, err = io.ReadFull(session.reader, password)
	if err != nil {
		return err
	}

	// Verify password
	expectedPassword, exists := s.config.Users[session.username]
	if !exists || string(password[:len(password)-1]) != expectedPassword { // Remove null terminator
		return s.sendError(session, "28P01", "authentication failed for user \""+session.username+"\"")
	}

	return s.sendAuthenticationOk(session)
}

// handleMD5Auth handles MD5 password authentication
func (s *PostgreSQLServer) handleMD5Auth(session *PostgreSQLSession) error {
	// Generate salt
	salt := make([]byte, 4)
	_, err := rand.Read(salt)
	if err != nil {
		return err
	}

	// Send MD5 request
	msg := make([]byte, 13)
	msg[0] = PG_RESP_AUTH_OK
	binary.BigEndian.PutUint32(msg[1:5], 12)
	binary.BigEndian.PutUint32(msg[5:9], AUTH_MD5)
	copy(msg[9:13], salt)

	_, err = session.writer.Write(msg)
	if err != nil {
		return err
	}
	err = session.writer.Flush()
	if err != nil {
		return err
	}

	// Read password response
	msgType := make([]byte, 1)
	_, err = io.ReadFull(session.reader, msgType)
	if err != nil {
		return err
	}

	if msgType[0] != PG_MSG_PASSWORD {
		return fmt.Errorf("expected password message, got %c", msgType[0])
	}

	length := make([]byte, 4)
	_, err = io.ReadFull(session.reader, length)
	if err != nil {
		return err
	}

	msgLength := binary.BigEndian.Uint32(length) - 4
	response := make([]byte, msgLength)
	_, err = io.ReadFull(session.reader, response)
	if err != nil {
		return err
	}

	// Verify MD5 hash
	expectedPassword, exists := s.config.Users[session.username]
	if !exists {
		return s.sendError(session, "28P01", "authentication failed for user \""+session.username+"\"")
	}

	// Calculate expected hash: md5(md5(password + username) + salt)
	inner := md5.Sum([]byte(expectedPassword + session.username))
	expected := fmt.Sprintf("md5%x", md5.Sum(append([]byte(fmt.Sprintf("%x", inner)), salt...)))

	if string(response[:len(response)-1]) != expected { // Remove null terminator
		return s.sendError(session, "28P01", "authentication failed for user \""+session.username+"\"")
	}

	return s.sendAuthenticationOk(session)
}

// generateConnectionID generates a unique connection ID
func (s *PostgreSQLServer) generateConnectionID() uint32 {
	s.sessionMux.Lock()
	defer s.sessionMux.Unlock()
	id := s.nextConnID
	s.nextConnID++
	return id
}

// generateSecretKey generates a secret key for the connection
func (s *PostgreSQLServer) generateSecretKey() uint32 {
	key := make([]byte, 4)
	rand.Read(key)
	return binary.BigEndian.Uint32(key)
}

// close marks the session as closed
func (s *PostgreSQLSession) close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// cleanupSessions periodically cleans up idle sessions
func (s *PostgreSQLServer) cleanupSessions() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdown:
			return
		case <-ticker.C:
			s.cleanupIdleSessions()
		}
	}
}

// cleanupIdleSessions removes sessions that have been idle too long
func (s *PostgreSQLServer) cleanupIdleSessions() {
	now := time.Now()

	s.sessionMux.Lock()
	defer s.sessionMux.Unlock()

	for id, session := range s.sessions {
		if now.Sub(session.lastActivity) > s.config.IdleTimeout {
			glog.Infof("Closing idle PostgreSQL session %d", id)
			session.close()
			delete(s.sessions, id)
		}
	}
}

// GetAddress returns the server address
func (s *PostgreSQLServer) GetAddress() string {
	return fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
}
