package weed_server

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/query/engine"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// JDBCServer provides JDBC-compatible access to SeaweedFS SQL engine
type JDBCServer struct {
	host        string
	port        int
	masterAddr  string
	listener    net.Listener
	sqlEngine   *engine.SQLEngine
	connections map[net.Conn]*JDBCConnection
	connMutex   sync.RWMutex
	shutdown    chan struct{}
	wg          sync.WaitGroup
}

// JDBCConnection represents a single JDBC client connection
type JDBCConnection struct {
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	database     string
	autoCommit   bool
	connectionID uint32
	closed       bool
	mutex        sync.Mutex
}

// JDBC Protocol Constants
const (
	// Message Types
	JDBC_MSG_CONNECT        = 0x01
	JDBC_MSG_DISCONNECT     = 0x02
	JDBC_MSG_EXECUTE_QUERY  = 0x03
	JDBC_MSG_EXECUTE_UPDATE = 0x04
	JDBC_MSG_PREPARE        = 0x05
	JDBC_MSG_EXECUTE_PREP   = 0x06
	JDBC_MSG_GET_METADATA   = 0x07
	JDBC_MSG_SET_AUTOCOMMIT = 0x08
	JDBC_MSG_COMMIT         = 0x09
	JDBC_MSG_ROLLBACK       = 0x0A

	// Response Types
	JDBC_RESP_OK           = 0x00
	JDBC_RESP_ERROR        = 0x01
	JDBC_RESP_RESULT_SET   = 0x02
	JDBC_RESP_UPDATE_COUNT = 0x03
	JDBC_RESP_METADATA     = 0x04

	// Default values
	DEFAULT_JDBC_PORT = 8089
)

// NewJDBCServer creates a new JDBC server instance
func NewJDBCServer(host string, port int, masterAddr string) (*JDBCServer, error) {
	if port <= 0 {
		port = DEFAULT_JDBC_PORT
	}
	if host == "" {
		host = "localhost"
	}

	// Create SQL engine
	sqlEngine := engine.NewSQLEngine(masterAddr)

	server := &JDBCServer{
		host:        host,
		port:        port,
		masterAddr:  masterAddr,
		sqlEngine:   sqlEngine,
		connections: make(map[net.Conn]*JDBCConnection),
		shutdown:    make(chan struct{}),
	}

	return server, nil
}

// Start begins listening for JDBC connections
func (s *JDBCServer) Start() error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start JDBC server on %s: %v", addr, err)
	}

	s.listener = listener
	glog.Infof("JDBC Server listening on %s", addr)

	s.wg.Add(1)
	go s.acceptConnections()

	return nil
}

// Stop gracefully shuts down the JDBC server
func (s *JDBCServer) Stop() error {
	close(s.shutdown)

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	s.connMutex.Lock()
	for conn, jdbcConn := range s.connections {
		jdbcConn.close()
		conn.Close()
	}
	s.connections = make(map[net.Conn]*JDBCConnection)
	s.connMutex.Unlock()

	s.wg.Wait()
	glog.Infof("JDBC Server stopped")
	return nil
}

// acceptConnections handles incoming JDBC connections
func (s *JDBCServer) acceptConnections() {
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
				glog.Errorf("Failed to accept JDBC connection: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection processes a single JDBC connection
func (s *JDBCServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// Create JDBC connection wrapper
	jdbcConn := &JDBCConnection{
		conn:         conn,
		reader:       bufio.NewReader(conn),
		writer:       bufio.NewWriter(conn),
		database:     "default",
		autoCommit:   true,
		connectionID: s.generateConnectionID(),
	}

	// Register connection
	s.connMutex.Lock()
	s.connections[conn] = jdbcConn
	s.connMutex.Unlock()

	// Clean up on exit
	defer func() {
		s.connMutex.Lock()
		delete(s.connections, conn)
		s.connMutex.Unlock()
	}()

	glog.Infof("New JDBC connection from %s (ID: %d)", conn.RemoteAddr(), jdbcConn.connectionID)

	// Handle connection messages
	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		// Set read timeout
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		err := s.handleMessage(jdbcConn)
		if err != nil {
			if err == io.EOF {
				glog.Infof("JDBC client disconnected (ID: %d)", jdbcConn.connectionID)
			} else {
				glog.Errorf("Error handling JDBC message (ID: %d): %v", jdbcConn.connectionID, err)
			}
			return
		}
	}
}

// handleMessage processes a single JDBC protocol message
func (s *JDBCServer) handleMessage(conn *JDBCConnection) error {
	// Read message header (message type + length)
	header := make([]byte, 5)
	_, err := io.ReadFull(conn.reader, header)
	if err != nil {
		return err
	}

	msgType := header[0]
	msgLength := binary.BigEndian.Uint32(header[1:5])

	// Read message body
	msgBody := make([]byte, msgLength)
	if msgLength > 0 {
		_, err = io.ReadFull(conn.reader, msgBody)
		if err != nil {
			return err
		}
	}

	// Process message based on type
	switch msgType {
	case JDBC_MSG_CONNECT:
		return s.handleConnect(conn, msgBody)
	case JDBC_MSG_DISCONNECT:
		return s.handleDisconnect(conn)
	case JDBC_MSG_EXECUTE_QUERY:
		return s.handleExecuteQuery(conn, msgBody)
	case JDBC_MSG_EXECUTE_UPDATE:
		return s.handleExecuteUpdate(conn, msgBody)
	case JDBC_MSG_GET_METADATA:
		return s.handleGetMetadata(conn, msgBody)
	case JDBC_MSG_SET_AUTOCOMMIT:
		return s.handleSetAutoCommit(conn, msgBody)
	case JDBC_MSG_COMMIT:
		return s.handleCommit(conn)
	case JDBC_MSG_ROLLBACK:
		return s.handleRollback(conn)
	default:
		return s.sendError(conn, fmt.Errorf("unknown message type: %d", msgType))
	}
}

// handleConnect processes JDBC connection request
func (s *JDBCServer) handleConnect(conn *JDBCConnection, msgBody []byte) error {
	// Parse connection string (database name)
	if len(msgBody) > 0 {
		conn.database = string(msgBody)
	}

	glog.Infof("JDBC client connected to database: %s (ID: %d)", conn.database, conn.connectionID)

	// Send OK response
	return s.sendOK(conn, "Connected successfully")
}

// handleDisconnect processes JDBC disconnect request
func (s *JDBCServer) handleDisconnect(conn *JDBCConnection) error {
	glog.Infof("JDBC client disconnecting (ID: %d)", conn.connectionID)
	conn.close()
	return io.EOF // This will cause the connection handler to exit
}

// handleExecuteQuery processes SQL SELECT queries
func (s *JDBCServer) handleExecuteQuery(conn *JDBCConnection, msgBody []byte) error {
	sql := string(msgBody)

	glog.V(2).Infof("Executing query (ID: %d): %s", conn.connectionID, sql)

	// Execute SQL using the query engine
	ctx := context.Background()
	result, err := s.sqlEngine.ExecuteSQL(ctx, sql)
	if err != nil {
		return s.sendError(conn, err)
	}

	if result.Error != nil {
		return s.sendError(conn, result.Error)
	}

	// Send result set
	return s.sendResultSet(conn, result)
}

// handleExecuteUpdate processes SQL UPDATE/INSERT/DELETE queries
func (s *JDBCServer) handleExecuteUpdate(conn *JDBCConnection, msgBody []byte) error {
	sql := string(msgBody)

	glog.V(2).Infof("Executing update (ID: %d): %s", conn.connectionID, sql)

	// For now, treat updates same as queries since SeaweedFS SQL is read-only
	ctx := context.Background()
	result, err := s.sqlEngine.ExecuteSQL(ctx, sql)
	if err != nil {
		return s.sendError(conn, err)
	}

	if result.Error != nil {
		return s.sendError(conn, result.Error)
	}

	// Send update count (0 for read-only operations)
	return s.sendUpdateCount(conn, 0)
}

// handleGetMetadata processes JDBC metadata requests
func (s *JDBCServer) handleGetMetadata(conn *JDBCConnection, msgBody []byte) error {
	metadataType := string(msgBody)

	glog.V(2).Infof("Getting metadata (ID: %d): %s", conn.connectionID, metadataType)

	switch metadataType {
	case "tables":
		return s.sendTablesMetadata(conn)
	case "databases", "schemas":
		return s.sendDatabasesMetadata(conn)
	default:
		return s.sendError(conn, fmt.Errorf("unsupported metadata type: %s", metadataType))
	}
}

// handleSetAutoCommit processes autocommit setting
func (s *JDBCServer) handleSetAutoCommit(conn *JDBCConnection, msgBody []byte) error {
	autoCommit := len(msgBody) > 0 && msgBody[0] == 1
	conn.autoCommit = autoCommit

	glog.V(2).Infof("Setting autocommit (ID: %d): %v", conn.connectionID, autoCommit)

	return s.sendOK(conn, fmt.Sprintf("AutoCommit set to %v", autoCommit))
}

// handleCommit processes transaction commit (no-op for read-only)
func (s *JDBCServer) handleCommit(conn *JDBCConnection) error {
	glog.V(2).Infof("Commit (ID: %d): no-op for read-only", conn.connectionID)
	return s.sendOK(conn, "Commit successful")
}

// handleRollback processes transaction rollback (no-op for read-only)
func (s *JDBCServer) handleRollback(conn *JDBCConnection) error {
	glog.V(2).Infof("Rollback (ID: %d): no-op for read-only", conn.connectionID)
	return s.sendOK(conn, "Rollback successful")
}

// sendOK sends a success response
func (s *JDBCServer) sendOK(conn *JDBCConnection, message string) error {
	return s.sendResponse(conn, JDBC_RESP_OK, []byte(message))
}

// sendError sends an error response
func (s *JDBCServer) sendError(conn *JDBCConnection, err error) error {
	return s.sendResponse(conn, JDBC_RESP_ERROR, []byte(err.Error()))
}

// sendResultSet sends query results
func (s *JDBCServer) sendResultSet(conn *JDBCConnection, result *engine.QueryResult) error {
	// Serialize result set
	data := s.serializeResultSet(result)
	return s.sendResponse(conn, JDBC_RESP_RESULT_SET, data)
}

// sendUpdateCount sends update operation result
func (s *JDBCServer) sendUpdateCount(conn *JDBCConnection, count int) error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(count))
	return s.sendResponse(conn, JDBC_RESP_UPDATE_COUNT, data)
}

// sendTablesMetadata sends table metadata
func (s *JDBCServer) sendTablesMetadata(conn *JDBCConnection) error {
	// For now, return empty metadata - this would need to query the schema catalog
	data := s.serializeTablesMetadata([]string{})
	return s.sendResponse(conn, JDBC_RESP_METADATA, data)
}

// sendDatabasesMetadata sends database/schema metadata
func (s *JDBCServer) sendDatabasesMetadata(conn *JDBCConnection) error {
	// Return default databases
	databases := []string{"default", "test"}
	data := s.serializeDatabasesMetadata(databases)
	return s.sendResponse(conn, JDBC_RESP_METADATA, data)
}

// sendResponse sends a response with the given type and data
func (s *JDBCServer) sendResponse(conn *JDBCConnection, responseType byte, data []byte) error {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	// Write response header
	header := make([]byte, 5)
	header[0] = responseType
	binary.BigEndian.PutUint32(header[1:5], uint32(len(data)))

	_, err := conn.writer.Write(header)
	if err != nil {
		return err
	}

	// Write response data
	if len(data) > 0 {
		_, err = conn.writer.Write(data)
		if err != nil {
			return err
		}
	}

	return conn.writer.Flush()
}

// serializeResultSet converts QueryResult to JDBC wire format
func (s *JDBCServer) serializeResultSet(result *engine.QueryResult) []byte {
	var data []byte

	// Column count
	colCount := make([]byte, 4)
	binary.BigEndian.PutUint32(colCount, uint32(len(result.Columns)))
	data = append(data, colCount...)

	// Column names
	for _, col := range result.Columns {
		colName := []byte(col)
		colLen := make([]byte, 4)
		binary.BigEndian.PutUint32(colLen, uint32(len(colName)))
		data = append(data, colLen...)
		data = append(data, colName...)
	}

	// Row count
	rowCount := make([]byte, 4)
	binary.BigEndian.PutUint32(rowCount, uint32(len(result.Rows)))
	data = append(data, rowCount...)

	// Rows
	for _, row := range result.Rows {
		for _, value := range row {
			// Convert value to string and serialize
			valueStr := s.valueToString(value)
			valueBytes := []byte(valueStr)
			valueLen := make([]byte, 4)
			binary.BigEndian.PutUint32(valueLen, uint32(len(valueBytes)))
			data = append(data, valueLen...)
			data = append(data, valueBytes...)
		}
	}

	return data
}

// serializeTablesMetadata converts table list to wire format
func (s *JDBCServer) serializeTablesMetadata(tables []string) []byte {
	var data []byte

	// Table count
	tableCount := make([]byte, 4)
	binary.BigEndian.PutUint32(tableCount, uint32(len(tables)))
	data = append(data, tableCount...)

	// Table names
	for _, table := range tables {
		tableBytes := []byte(table)
		tableLen := make([]byte, 4)
		binary.BigEndian.PutUint32(tableLen, uint32(len(tableBytes)))
		data = append(data, tableLen...)
		data = append(data, tableBytes...)
	}

	return data
}

// serializeDatabasesMetadata converts database list to wire format
func (s *JDBCServer) serializeDatabasesMetadata(databases []string) []byte {
	var data []byte

	// Database count
	dbCount := make([]byte, 4)
	binary.BigEndian.PutUint32(dbCount, uint32(len(databases)))
	data = append(data, dbCount...)

	// Database names
	for _, db := range databases {
		dbBytes := []byte(db)
		dbLen := make([]byte, 4)
		binary.BigEndian.PutUint32(dbLen, uint32(len(dbBytes)))
		data = append(data, dbLen...)
		data = append(data, dbBytes...)
	}

	return data
}

// valueToString converts a sqltypes.Value to string representation
func (s *JDBCServer) valueToString(value sqltypes.Value) string {
	if value.IsNull() {
		return ""
	}

	return value.ToString()
}

// generateConnectionID generates a unique connection ID
func (s *JDBCServer) generateConnectionID() uint32 {
	return uint32(time.Now().UnixNano() % 1000000)
}

// close marks the connection as closed
func (c *JDBCConnection) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.closed = true
}

// GetAddress returns the server address
func (s *JDBCServer) GetAddress() string {
	return fmt.Sprintf("%s:%d", s.host, s.port)
}
