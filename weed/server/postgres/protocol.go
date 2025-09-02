package postgres

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"github.com/xwb1989/sqlparser"
)

// handleMessage processes a single PostgreSQL protocol message
func (s *PostgreSQLServer) handleMessage(session *PostgreSQLSession) error {
	// Read message type
	msgType := make([]byte, 1)
	_, err := io.ReadFull(session.reader, msgType)
	if err != nil {
		return err
	}

	// Read message length
	length := make([]byte, 4)
	_, err = io.ReadFull(session.reader, length)
	if err != nil {
		return err
	}

	msgLength := binary.BigEndian.Uint32(length) - 4
	msgBody := make([]byte, msgLength)
	if msgLength > 0 {
		_, err = io.ReadFull(session.reader, msgBody)
		if err != nil {
			return err
		}
	}

	// Process message based on type
	switch msgType[0] {
	case PG_MSG_QUERY:
		return s.handleSimpleQuery(session, string(msgBody[:len(msgBody)-1])) // Remove null terminator
	case PG_MSG_PARSE:
		return s.handleParse(session, msgBody)
	case PG_MSG_BIND:
		return s.handleBind(session, msgBody)
	case PG_MSG_EXECUTE:
		return s.handleExecute(session, msgBody)
	case PG_MSG_DESCRIBE:
		return s.handleDescribe(session, msgBody)
	case PG_MSG_CLOSE:
		return s.handleClose(session, msgBody)
	case PG_MSG_FLUSH:
		return s.handleFlush(session)
	case PG_MSG_SYNC:
		return s.handleSync(session)
	case PG_MSG_TERMINATE:
		return io.EOF // Signal connection termination
	default:
		return s.sendError(session, "08P01", fmt.Sprintf("unknown message type: %c", msgType[0]))
	}
}

// handleSimpleQuery processes a simple query message
func (s *PostgreSQLServer) handleSimpleQuery(session *PostgreSQLSession, query string) error {
	glog.V(2).Infof("PostgreSQL Query (ID: %d): %s", session.processID, query)

	// Handle USE database commands for session context
	queryUpper := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(queryUpper, "USE ") {
		parts := strings.Fields(query)
		if len(parts) >= 2 {
			newDatabase := strings.TrimSpace(parts[1])
			session.database = newDatabase
			s.sqlEngine.GetCatalog().SetCurrentDatabase(newDatabase)

			// Send command complete for USE
			err := s.sendCommandComplete(session, "USE")
			if err != nil {
				return err
			}
			return s.sendReadyForQuery(session)
		}
	}

	// Set database context in SQL engine if session database is different from current
	if session.database != "" && session.database != s.sqlEngine.GetCatalog().GetCurrentDatabase() {
		s.sqlEngine.GetCatalog().SetCurrentDatabase(session.database)
	}

	// Split query string into individual statements to handle multi-statement queries
	queries, err := sqlparser.SplitStatementToPieces(query)
	if err != nil {
		// If split fails, fall back to single query processing
		queries = []string{strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))}
	}

	// Execute each statement sequentially
	for _, singleQuery := range queries {
		cleanQuery := strings.TrimSpace(singleQuery)
		if cleanQuery == "" {
			continue // Skip empty statements
		}

		// Handle PostgreSQL-specific system queries directly
		if systemResult := s.handleSystemQuery(session, cleanQuery); systemResult != nil {
			err := s.sendSystemQueryResult(session, systemResult, cleanQuery)
			if err != nil {
				return err
			}
			continue // Continue with next statement
		}

		// Execute using SQL engine directly
		ctx := context.Background()
		result, err := s.sqlEngine.ExecuteSQL(ctx, cleanQuery)
		if err != nil {
			// Send error message but keep connection alive
			sendErr := s.sendError(session, "42000", err.Error())
			if sendErr != nil {
				return sendErr
			}
			// Send ReadyForQuery to keep connection alive
			return s.sendReadyForQuery(session)
		}

		if result.Error != nil {
			// Send error message but keep connection alive
			sendErr := s.sendError(session, "42000", result.Error.Error())
			if sendErr != nil {
				return sendErr
			}
			// Send ReadyForQuery to keep connection alive
			return s.sendReadyForQuery(session)
		}

		// Send results for this statement
		if len(result.Columns) > 0 {
			// Send row description
			err = s.sendRowDescription(session, result.Columns, result.Rows)
			if err != nil {
				return err
			}

			// Send data rows
			for _, row := range result.Rows {
				err = s.sendDataRow(session, row)
				if err != nil {
					return err
				}
			}
		}

		// Send command complete for this statement
		tag := s.getCommandTag(cleanQuery, len(result.Rows))
		err = s.sendCommandComplete(session, tag)
		if err != nil {
			return err
		}
	}

	// Send ready for query after all statements are processed
	return s.sendReadyForQuery(session)
}

// SystemQueryResult represents the result of a system query
type SystemQueryResult struct {
	Columns []string
	Rows    [][]string
}

// handleSystemQuery handles PostgreSQL system queries directly
func (s *PostgreSQLServer) handleSystemQuery(session *PostgreSQLSession, query string) *SystemQueryResult {
	// Trim and normalize query
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")
	queryLower := strings.ToLower(query)

	// Handle essential PostgreSQL system queries
	switch queryLower {
	case "select version()":
		return &SystemQueryResult{
			Columns: []string{"version"},
			Rows:    [][]string{{fmt.Sprintf("SeaweedFS %s (PostgreSQL 14.0 compatible)", version.VERSION_NUMBER)}},
		}
	case "select current_database()":
		return &SystemQueryResult{
			Columns: []string{"current_database"},
			Rows:    [][]string{{s.config.Database}},
		}
	case "select current_user":
		return &SystemQueryResult{
			Columns: []string{"current_user"},
			Rows:    [][]string{{"seaweedfs"}},
		}
	case "select current_setting('server_version')":
		return &SystemQueryResult{
			Columns: []string{"server_version"},
			Rows:    [][]string{{fmt.Sprintf("%s (SeaweedFS)", version.VERSION_NUMBER)}},
		}
	case "select current_setting('server_encoding')":
		return &SystemQueryResult{
			Columns: []string{"server_encoding"},
			Rows:    [][]string{{"UTF8"}},
		}
	case "select current_setting('client_encoding')":
		return &SystemQueryResult{
			Columns: []string{"client_encoding"},
			Rows:    [][]string{{"UTF8"}},
		}
	}

	// Handle transaction commands (no-op for read-only)
	switch queryLower {
	case "begin", "start transaction":
		return &SystemQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"BEGIN"}},
		}
	case "commit":
		return &SystemQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"COMMIT"}},
		}
	case "rollback":
		return &SystemQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"ROLLBACK"}},
		}
	}

	// If starts with SET, return a no-op
	if strings.HasPrefix(queryLower, "set ") {
		return &SystemQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"SET"}},
		}
	}

	// Return nil to use SQL engine
	return nil
}

// sendSystemQueryResult sends the result of a system query
func (s *PostgreSQLServer) sendSystemQueryResult(session *PostgreSQLSession, result *SystemQueryResult, query string) error {
	// Create column descriptions for system query results
	columns := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		columns[i] = col
	}

	// Convert to sqltypes.Value format
	var sqlRows [][]sqltypes.Value
	for _, row := range result.Rows {
		sqlRow := make([]sqltypes.Value, len(row))
		for i, cell := range row {
			sqlRow[i] = sqltypes.NewVarChar(cell)
		}
		sqlRows = append(sqlRows, sqlRow)
	}

	// Send row description
	err := s.sendRowDescription(session, columns, sqlRows)
	if err != nil {
		return err
	}

	// Send data rows
	for _, row := range sqlRows {
		err = s.sendDataRow(session, row)
		if err != nil {
			return err
		}
	}

	// Send command complete
	tag := s.getCommandTag(query, len(result.Rows))
	err = s.sendCommandComplete(session, tag)
	if err != nil {
		return err
	}

	// Send ready for query
	return s.sendReadyForQuery(session)
}

// handleParse processes a Parse message (prepared statement)
func (s *PostgreSQLServer) handleParse(session *PostgreSQLSession, msgBody []byte) error {
	// Parse message format: statement_name\0query\0param_count(int16)[param_type(int32)...]
	parts := strings.Split(string(msgBody), "\x00")
	if len(parts) < 2 {
		return s.sendError(session, "08P01", "invalid Parse message format")
	}

	stmtName := parts[0]
	query := parts[1]

	// Create prepared statement
	stmt := &PreparedStatement{
		Name:       stmtName,
		Query:      query,
		ParamTypes: []uint32{},
		Fields:     []FieldDescription{},
	}

	session.preparedStmts[stmtName] = stmt

	// Send parse complete
	return s.sendParseComplete(session)
}

// handleBind processes a Bind message
func (s *PostgreSQLServer) handleBind(session *PostgreSQLSession, msgBody []byte) error {
	// For now, simple implementation
	// In full implementation, would parse parameters and create portal

	// Send bind complete
	return s.sendBindComplete(session)
}

// handleExecute processes an Execute message
func (s *PostgreSQLServer) handleExecute(session *PostgreSQLSession, msgBody []byte) error {
	// Parse portal name
	parts := strings.Split(string(msgBody), "\x00")
	if len(parts) == 0 {
		return s.sendError(session, "08P01", "invalid Execute message format")
	}

	portalName := parts[0]

	// For now, execute as simple query
	// In full implementation, would use portal with parameters
	glog.V(2).Infof("PostgreSQL Execute portal (ID: %d): %s", session.processID, portalName)

	// Send command complete
	err := s.sendCommandComplete(session, "SELECT 0")
	if err != nil {
		return err
	}

	return nil
}

// handleDescribe processes a Describe message
func (s *PostgreSQLServer) handleDescribe(session *PostgreSQLSession, msgBody []byte) error {
	if len(msgBody) < 2 {
		return s.sendError(session, "08P01", "invalid Describe message format")
	}

	objectType := msgBody[0] // 'S' for statement, 'P' for portal
	objectName := string(msgBody[1:])

	glog.V(2).Infof("PostgreSQL Describe %c (ID: %d): %s", objectType, session.processID, objectName)

	// For now, send empty row description
	return s.sendRowDescription(session, []string{}, [][]sqltypes.Value{})
}

// handleClose processes a Close message
func (s *PostgreSQLServer) handleClose(session *PostgreSQLSession, msgBody []byte) error {
	if len(msgBody) < 2 {
		return s.sendError(session, "08P01", "invalid Close message format")
	}

	objectType := msgBody[0] // 'S' for statement, 'P' for portal
	objectName := string(msgBody[1:])

	switch objectType {
	case 'S':
		delete(session.preparedStmts, objectName)
	case 'P':
		delete(session.portals, objectName)
	}

	// Send close complete
	return s.sendCloseComplete(session)
}

// handleFlush processes a Flush message
func (s *PostgreSQLServer) handleFlush(session *PostgreSQLSession) error {
	return session.writer.Flush()
}

// handleSync processes a Sync message
func (s *PostgreSQLServer) handleSync(session *PostgreSQLSession) error {
	// Reset transaction state if needed
	session.transactionState = PG_TRANS_IDLE

	// Send ready for query
	return s.sendReadyForQuery(session)
}

// sendParameterStatus sends a parameter status message
func (s *PostgreSQLServer) sendParameterStatus(session *PostgreSQLSession, name, value string) error {
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_PARAMETER)

	// Calculate length
	length := 4 + len(name) + 1 + len(value) + 1
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)

	// Add name and value
	msg = append(msg, []byte(name)...)
	msg = append(msg, 0) // null terminator
	msg = append(msg, []byte(value)...)
	msg = append(msg, 0) // null terminator

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendBackendKeyData sends backend key data
func (s *PostgreSQLServer) sendBackendKeyData(session *PostgreSQLSession) error {
	msg := make([]byte, 13)
	msg[0] = PG_RESP_BACKEND_KEY
	binary.BigEndian.PutUint32(msg[1:5], 12)
	binary.BigEndian.PutUint32(msg[5:9], session.processID)
	binary.BigEndian.PutUint32(msg[9:13], session.secretKey)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendReadyForQuery sends ready for query message
func (s *PostgreSQLServer) sendReadyForQuery(session *PostgreSQLSession) error {
	msg := make([]byte, 6)
	msg[0] = PG_RESP_READY
	binary.BigEndian.PutUint32(msg[1:5], 5)
	msg[5] = session.transactionState

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendRowDescription sends row description message
func (s *PostgreSQLServer) sendRowDescription(session *PostgreSQLSession, columns []string, rows [][]sqltypes.Value) error {
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_ROW_DESC)

	// Calculate message length
	length := 4 + 2 // length + field count
	for _, col := range columns {
		length += len(col) + 1 + 4 + 2 + 4 + 2 + 4 + 2 // name + null + tableOID + attrNum + typeOID + typeSize + typeMod + format
	}

	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)

	// Field count
	fieldCountBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldCountBytes, uint16(len(columns)))
	msg = append(msg, fieldCountBytes...)

	// Field descriptions
	for i, col := range columns {
		// Field name
		msg = append(msg, []byte(col)...)
		msg = append(msg, 0) // null terminator

		// Table OID (0 for no table)
		tableOID := make([]byte, 4)
		binary.BigEndian.PutUint32(tableOID, 0)
		msg = append(msg, tableOID...)

		// Attribute number
		attrNum := make([]byte, 2)
		binary.BigEndian.PutUint16(attrNum, uint16(i+1))
		msg = append(msg, attrNum...)

		// Type OID (determine from data)
		typeOID := s.getPostgreSQLType(columns, rows, i)
		typeOIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(typeOIDBytes, typeOID)
		msg = append(msg, typeOIDBytes...)

		// Type size (-1 for variable length)
		typeSize := make([]byte, 2)
		binary.BigEndian.PutUint16(typeSize, 0xFFFF) // -1 as uint16
		msg = append(msg, typeSize...)

		// Type modifier (-1 for default)
		typeMod := make([]byte, 4)
		binary.BigEndian.PutUint32(typeMod, 0xFFFFFFFF) // -1 as uint32
		msg = append(msg, typeMod...)

		// Format (0 for text)
		format := make([]byte, 2)
		binary.BigEndian.PutUint16(format, 0)
		msg = append(msg, format...)
	}

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendDataRow sends a data row message
func (s *PostgreSQLServer) sendDataRow(session *PostgreSQLSession, row []sqltypes.Value) error {
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_DATA_ROW)

	// Calculate message length
	length := 4 + 2 // length + field count
	for _, value := range row {
		if value.IsNull() {
			length += 4 // null value length (-1)
		} else {
			valueStr := value.ToString()
			length += 4 + len(valueStr) // field length + data
		}
	}

	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)

	// Field count
	fieldCountBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldCountBytes, uint16(len(row)))
	msg = append(msg, fieldCountBytes...)

	// Field values
	for _, value := range row {
		if value.IsNull() {
			// Null value
			nullLength := make([]byte, 4)
			binary.BigEndian.PutUint32(nullLength, 0xFFFFFFFF) // -1 as uint32
			msg = append(msg, nullLength...)
		} else {
			valueStr := value.ToString()
			valueLength := make([]byte, 4)
			binary.BigEndian.PutUint32(valueLength, uint32(len(valueStr)))
			msg = append(msg, valueLength...)
			msg = append(msg, []byte(valueStr)...)
		}
	}

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendCommandComplete sends command complete message
func (s *PostgreSQLServer) sendCommandComplete(session *PostgreSQLSession, tag string) error {
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_COMMAND)

	length := 4 + len(tag) + 1
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)

	msg = append(msg, []byte(tag)...)
	msg = append(msg, 0) // null terminator

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendParseComplete sends parse complete message
func (s *PostgreSQLServer) sendParseComplete(session *PostgreSQLSession) error {
	msg := make([]byte, 5)
	msg[0] = PG_RESP_PARSE_COMPLETE
	binary.BigEndian.PutUint32(msg[1:5], 4)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendBindComplete sends bind complete message
func (s *PostgreSQLServer) sendBindComplete(session *PostgreSQLSession) error {
	msg := make([]byte, 5)
	msg[0] = PG_RESP_BIND_COMPLETE
	binary.BigEndian.PutUint32(msg[1:5], 4)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendCloseComplete sends close complete message
func (s *PostgreSQLServer) sendCloseComplete(session *PostgreSQLSession) error {
	msg := make([]byte, 5)
	msg[0] = PG_RESP_CLOSE_COMPLETE
	binary.BigEndian.PutUint32(msg[1:5], 4)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendError sends an error message
func (s *PostgreSQLServer) sendError(session *PostgreSQLSession, code, message string) error {
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_ERROR)

	// Build error fields
	fields := fmt.Sprintf("S%s\x00C%s\x00M%s\x00\x00", "ERROR", code, message)
	length := 4 + len(fields)

	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)
	msg = append(msg, []byte(fields)...)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// getCommandTag generates appropriate command tag for query
func (s *PostgreSQLServer) getCommandTag(query string, rowCount int) string {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(queryUpper, "SELECT") {
		return fmt.Sprintf("SELECT %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "INSERT") {
		return fmt.Sprintf("INSERT 0 %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "UPDATE") {
		return fmt.Sprintf("UPDATE %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "DELETE") {
		return fmt.Sprintf("DELETE %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "SHOW") {
		return fmt.Sprintf("SELECT %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "DESCRIBE") || strings.HasPrefix(queryUpper, "DESC") {
		return fmt.Sprintf("SELECT %d", rowCount)
	}

	return "SELECT 0"
}

// getPostgreSQLType determines PostgreSQL type OID from data
func (s *PostgreSQLServer) getPostgreSQLType(columns []string, rows [][]sqltypes.Value, colIndex int) uint32 {
	if len(rows) == 0 || colIndex >= len(rows[0]) {
		return PG_TYPE_TEXT // Default to text
	}

	// Sample first non-null value to determine type
	for _, row := range rows {
		if colIndex < len(row) && !row[colIndex].IsNull() {
			value := row[colIndex]
			switch value.Type() {
			case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32:
				return PG_TYPE_INT4
			case sqltypes.Int64:
				return PG_TYPE_INT8
			case sqltypes.Float32, sqltypes.Float64:
				return PG_TYPE_FLOAT8
			case sqltypes.Bit:
				return PG_TYPE_BOOL
			case sqltypes.Timestamp, sqltypes.Datetime:
				return PG_TYPE_TIMESTAMP
			default:
				// Try to infer from string content
				valueStr := value.ToString()
				if _, err := strconv.ParseInt(valueStr, 10, 32); err == nil {
					return PG_TYPE_INT4
				}
				if _, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
					return PG_TYPE_INT8
				}
				if _, err := strconv.ParseFloat(valueStr, 64); err == nil {
					return PG_TYPE_FLOAT8
				}
				if valueStr == "true" || valueStr == "false" {
					return PG_TYPE_BOOL
				}
				return PG_TYPE_TEXT
			}
		}
	}

	return PG_TYPE_TEXT // Default to text
}
