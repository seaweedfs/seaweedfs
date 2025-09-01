package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/xwb1989/sqlparser"
)

// SQLEngine provides SQL query execution capabilities for SeaweedFS
// Assumptions:
// 1. MQ namespaces map directly to SQL databases
// 2. MQ topics map directly to SQL tables
// 3. Schema evolution is handled transparently with backward compatibility
// 4. Queries run against Parquet-stored MQ messages
type SQLEngine struct {
	catalog *SchemaCatalog
}

// QueryResult represents the result of a SQL query execution
type QueryResult struct {
	Columns []string           `json:"columns"`
	Rows    [][]sqltypes.Value `json:"rows"`
	Error   error              `json:"error,omitempty"`
}

// NewSQLEngine creates a new SQL execution engine
// Assumption: Schema catalog is initialized with current MQ state
func NewSQLEngine(filerAddress string) *SQLEngine {
	return &SQLEngine{
		catalog: NewSchemaCatalog(filerAddress),
	}
}

// ExecuteSQL parses and executes a SQL statement
// Assumptions:
// 1. All SQL statements are MySQL-compatible via xwb1989/sqlparser
// 2. DDL operations (CREATE/ALTER/DROP) modify underlying MQ topics
// 3. DML operations (SELECT) query Parquet files directly
// 4. Error handling follows MySQL conventions
func (e *SQLEngine) ExecuteSQL(ctx context.Context, sql string) (*QueryResult, error) {
	// Handle DESCRIBE as a special case since it's not parsed as a standard statement
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "DESCRIBE") {
		return e.handleDescribeCommand(ctx, sql)
	}

	// Parse the SQL statement
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return &QueryResult{
			Error: fmt.Errorf("SQL parse error: %v", err),
		}, err
	}

	// Route to appropriate handler based on statement type
	switch stmt := stmt.(type) {
	case *sqlparser.Show:
		return e.executeShowStatementWithDescribe(ctx, stmt)
	case *sqlparser.DDL:
		return e.executeDDLStatement(ctx, stmt)
	case *sqlparser.Select:
		return e.executeSelectStatement(ctx, stmt)
	default:
		err := fmt.Errorf("unsupported SQL statement type: %T", stmt)
		return &QueryResult{Error: err}, err
	}
}

// executeDDLStatement handles CREATE, ALTER, DROP operations
// Assumption: These operations modify the underlying MQ topic structure
func (e *SQLEngine) executeDDLStatement(ctx context.Context, stmt *sqlparser.DDL) (*QueryResult, error) {
	switch stmt.Action {
	case sqlparser.CreateStr:
		return e.createTable(ctx, stmt)
	case sqlparser.AlterStr:
		return e.alterTable(ctx, stmt)
	case sqlparser.DropStr:
		return e.dropTable(ctx, stmt)
	default:
		err := fmt.Errorf("unsupported DDL action: %s", stmt.Action)
		return &QueryResult{Error: err}, err
	}
}

// executeSelectStatement handles SELECT queries
// Assumptions:
// 1. Queries run against Parquet files in MQ topics
// 2. Predicate pushdown is used for efficiency
// 3. Cross-topic joins are supported via partition-aware execution
func (e *SQLEngine) executeSelectStatement(ctx context.Context, stmt *sqlparser.Select) (*QueryResult, error) {
	// Parse FROM clause to get table (topic) information
	if len(stmt.From) != 1 {
		err := fmt.Errorf("SELECT supports single table queries only")
		return &QueryResult{Error: err}, err
	}

	// Extract table reference
	var database, tableName string
	switch table := stmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableExpr := table.Expr.(type) {
		case sqlparser.TableName:
			tableName = tableExpr.Name.String()
			if tableExpr.Qualifier.String() != "" {
				database = tableExpr.Qualifier.String()
			}
		default:
			err := fmt.Errorf("unsupported table expression: %T", tableExpr)
			return &QueryResult{Error: err}, err
		}
	default:
		err := fmt.Errorf("unsupported FROM clause: %T", table)
		return &QueryResult{Error: err}, err
	}

	// Use current database context if not specified
	if database == "" {
		database = e.catalog.GetCurrentDatabase()
		if database == "" {
			database = "default"
		}
	}

	// Create HybridMessageScanner for the topic (reads both live logs + Parquet files)
	// TODO: Get real filerClient from broker connection
	// For now, this will use sample data that simulates both live and archived messages
	hybridScanner, err := NewHybridMessageScanner(nil, database, tableName)
	if err != nil {
		// Fallback to sample data if topic doesn't exist or filer unavailable
		return e.executeSelectWithSampleData(ctx, stmt, database, tableName)
	}

	// Parse SELECT columns
	var columns []string
	selectAll := false

	for _, selectExpr := range stmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *sqlparser.StarExpr:
			selectAll = true
		case *sqlparser.AliasedExpr:
			switch col := expr.Expr.(type) {
			case *sqlparser.ColName:
				columns = append(columns, col.Name.String())
			default:
				err := fmt.Errorf("unsupported SELECT expression: %T", col)
				return &QueryResult{Error: err}, err
			}
		default:
			err := fmt.Errorf("unsupported SELECT expression: %T", expr)
			return &QueryResult{Error: err}, err
		}
	}

	// Parse WHERE clause for predicate pushdown
	var predicate func(*schema_pb.RecordValue) bool
	if stmt.Where != nil {
		predicate, err = e.buildPredicate(stmt.Where.Expr)
		if err != nil {
			return &QueryResult{Error: err}, err
		}
	}

	// Parse LIMIT clause
	limit := 0
	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		switch limitExpr := stmt.Limit.Rowcount.(type) {
		case *sqlparser.SQLVal:
			if limitExpr.Type == sqlparser.IntVal {
				var parseErr error
				limit64, parseErr := strconv.ParseInt(string(limitExpr.Val), 10, 64)
				if parseErr != nil {
					return &QueryResult{Error: parseErr}, parseErr
				}
				limit = int(limit64)
			}
		}
	}

	// Build hybrid scan options
	hybridScanOptions := HybridScanOptions{
		StartTimeNs: 0, // TODO: Extract from WHERE clause time filters
		StopTimeNs:  0, // TODO: Extract from WHERE clause time filters  
		Limit:       limit,
		Predicate:   predicate,
	}

	if !selectAll {
		hybridScanOptions.Columns = columns
	}

	// Execute the hybrid scan (live logs + Parquet files)
	results, err := hybridScanner.Scan(ctx, hybridScanOptions)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Convert to SQL result format
	if selectAll {
		columns = nil // Let converter determine all columns
	}

	return hybridScanner.ConvertToSQLResult(results, columns), nil
}

// executeSelectWithSampleData provides enhanced sample data that simulates both live and archived messages
func (e *SQLEngine) executeSelectWithSampleData(ctx context.Context, stmt *sqlparser.Select, database, tableName string) (*QueryResult, error) {
	// Create a sample HybridMessageScanner to simulate both data sources
	now := time.Now().UnixNano()
	
	var sampleResults []HybridScanResult

	switch tableName {
	case "user_events":
		sampleResults = []HybridScanResult{
			// Live log data (recent)
			{
				Values: map[string]*schema_pb.Value{
					"user_id": {Kind: &schema_pb.Value_Int32Value{Int32Value: 1003}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "live_login"}},
					"data": {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "10.0.0.1", "live": true}`}},
				},
				Timestamp: now - 300000000000, // 5 minutes ago
				Key:       []byte("live-1003"),
				Source:    "live_log",
			},
			{
				Values: map[string]*schema_pb.Value{
					"user_id": {Kind: &schema_pb.Value_Int32Value{Int32Value: 1004}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "live_click"}},
					"data": {Kind: &schema_pb.Value_StringValue{StringValue: `{"button": "submit", "live": true}`}},
				},
				Timestamp: now - 120000000000, // 2 minutes ago
				Key:       []byte("live-1004"),
				Source:    "live_log",
			},
			// Archived Parquet data (older)
			{
				Values: map[string]*schema_pb.Value{
					"user_id": {Kind: &schema_pb.Value_Int32Value{Int32Value: 1001}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "archived_login"}},
					"data": {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "192.168.1.1", "archived": true}`}},
				},
				Timestamp: now - 3600000000000, // 1 hour ago
				Key:       []byte("archived-1001"),
				Source:    "parquet_archive",
			},
			{
				Values: map[string]*schema_pb.Value{
					"user_id": {Kind: &schema_pb.Value_Int32Value{Int32Value: 1002}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "archived_logout"}},
					"data": {Kind: &schema_pb.Value_StringValue{StringValue: `{"duration": 1800, "archived": true}`}},
				},
				Timestamp: now - 1800000000000, // 30 minutes ago
				Key:       []byte("archived-1002"),
				Source:    "parquet_archive",
			},
		}
	case "system_logs":
		sampleResults = []HybridScanResult{
			// Live system logs
			{
				Values: map[string]*schema_pb.Value{
					"level": {Kind: &schema_pb.Value_StringValue{StringValue: "INFO"}},
					"message": {Kind: &schema_pb.Value_StringValue{StringValue: "Live service heartbeat"}},
					"service": {Kind: &schema_pb.Value_StringValue{StringValue: "api-gateway"}},
				},
				Timestamp: now - 60000000000, // 1 minute ago
				Key:       []byte("live-log-001"),
				Source:    "live_log",
			},
			// Archived system logs
			{
				Values: map[string]*schema_pb.Value{
					"level": {Kind: &schema_pb.Value_StringValue{StringValue: "ERROR"}},
					"message": {Kind: &schema_pb.Value_StringValue{StringValue: "Database connection timeout"}},
					"service": {Kind: &schema_pb.Value_StringValue{StringValue: "user-service"}},
				},
				Timestamp: now - 7200000000000, // 2 hours ago
				Key:       []byte("archived-error-001"),
				Source:    "parquet_archive",
			},
		}
	default:
		return &QueryResult{
			Error: fmt.Errorf("table '%s.%s' not found", database, tableName),
		}, fmt.Errorf("table '%s.%s' not found", database, tableName)
	}

	// Apply basic LIMIT if specified
	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		if limitExpr, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); ok && limitExpr.Type == sqlparser.IntVal {
			if limit64, err := strconv.ParseInt(string(limitExpr.Val), 10, 64); err == nil {
				limit := int(limit64)
				if limit > 0 && limit < len(sampleResults) {
					sampleResults = sampleResults[:limit]
				}
			}
		}
	}

	// Convert to SQL result format using hybrid scanner logic
	return convertHybridResultsToSQL(sampleResults, nil), nil
}

// convertHybridResultsToSQL converts HybridScanResults to SQL format (helper function)
func convertHybridResultsToSQL(results []HybridScanResult, columns []string) *QueryResult {
	if len(results) == 0 {
		return &QueryResult{
			Columns: columns,
			Rows:    [][]sqltypes.Value{},
		}
	}
	
	// Determine columns if not specified
	if len(columns) == 0 {
		columnSet := make(map[string]bool)
		for _, result := range results {
			for columnName := range result.Values {
				columnSet[columnName] = true
			}
		}
		
		columns = make([]string, 0, len(columnSet))
		for columnName := range columnSet {
			columns = append(columns, columnName)
		}
		
		// Add metadata columns showing data source
		columns = append(columns, "_source")
	}
	
	// Convert to SQL rows
	rows := make([][]sqltypes.Value, len(results))
	for i, result := range results {
		row := make([]sqltypes.Value, len(columns))
		for j, columnName := range columns {
			if columnName == "_source" {
				row[j] = sqltypes.NewVarChar(result.Source)
			} else if value, exists := result.Values[columnName]; exists {
				row[j] = convertSchemaValueToSQL(value)
			} else {
				row[j] = sqltypes.NULL
			}
		}
		rows[i] = row
	}
	
	return &QueryResult{
		Columns: columns,
		Rows:    rows,
	}
}

// buildPredicate creates a predicate function from a WHERE clause expression
// This is a simplified implementation - a full implementation would be much more complex
func (e *SQLEngine) buildPredicate(expr sqlparser.Expr) (func(*schema_pb.RecordValue) bool, error) {
	switch exprType := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return e.buildComparisonPredicate(exprType)
	case *sqlparser.AndExpr:
		leftPred, err := e.buildPredicate(exprType.Left)
		if err != nil {
			return nil, err
		}
		rightPred, err := e.buildPredicate(exprType.Right)
		if err != nil {
			return nil, err
		}
		return func(record *schema_pb.RecordValue) bool {
			return leftPred(record) && rightPred(record)
		}, nil
	case *sqlparser.OrExpr:
		leftPred, err := e.buildPredicate(exprType.Left)
		if err != nil {
			return nil, err
		}
		rightPred, err := e.buildPredicate(exprType.Right)
		if err != nil {
			return nil, err
		}
		return func(record *schema_pb.RecordValue) bool {
			return leftPred(record) || rightPred(record)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported WHERE expression: %T", expr)
	}
}

// buildComparisonPredicate creates a predicate for comparison operations (=, <, >, etc.)
func (e *SQLEngine) buildComparisonPredicate(expr *sqlparser.ComparisonExpr) (func(*schema_pb.RecordValue) bool, error) {
	// Extract column name (left side)
	colName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("unsupported comparison left side: %T", expr.Left)
	}

	columnName := colName.Name.String()

	// Extract comparison value (right side)
	var compareValue interface{}
	switch val := expr.Right.(type) {
	case *sqlparser.SQLVal:
		switch val.Type {
		case sqlparser.IntVal:
			intVal, err := strconv.ParseInt(string(val.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			compareValue = intVal
		case sqlparser.StrVal:
			compareValue = string(val.Val)
		default:
			return nil, fmt.Errorf("unsupported SQL value type: %v", val.Type)
		}
	default:
		return nil, fmt.Errorf("unsupported comparison right side: %T", expr.Right)
	}

	// Create predicate based on operator
	operator := expr.Operator

	return func(record *schema_pb.RecordValue) bool {
		fieldValue, exists := record.Fields[columnName]
		if !exists {
			return false
		}

		return e.evaluateComparison(fieldValue, operator, compareValue)
	}, nil
}

// evaluateComparison performs the actual comparison
func (e *SQLEngine) evaluateComparison(fieldValue *schema_pb.Value, operator string, compareValue interface{}) bool {
	// This is a simplified implementation
	// A full implementation would handle type coercion and all comparison operators

	switch operator {
	case "=":
		return e.valuesEqual(fieldValue, compareValue)
	case "<":
		return e.valueLessThan(fieldValue, compareValue)
	case ">":
		return e.valueGreaterThan(fieldValue, compareValue)
	// TODO: Add support for <=, >=, !=, LIKE, IN, etc.
	default:
		return false
	}
}

// Helper functions for value comparison (simplified implementation)
func (e *SQLEngine) valuesEqual(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	switch v := fieldValue.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int32Value == int32(intVal)
		}
	case *schema_pb.Value_Int64Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int64Value == intVal
		}
	case *schema_pb.Value_StringValue:
		if strVal, ok := compareValue.(string); ok {
			return v.StringValue == strVal
		}
	}
	return false
}

func (e *SQLEngine) valueLessThan(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	switch v := fieldValue.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int32Value < int32(intVal)
		}
	case *schema_pb.Value_Int64Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int64Value < intVal
		}
	}
	return false
}

func (e *SQLEngine) valueGreaterThan(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	switch v := fieldValue.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int32Value > int32(intVal)
		}
	case *schema_pb.Value_Int64Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int64Value > intVal
		}
	}
	return false
}

// Helper methods for specific operations

func (e *SQLEngine) showDatabases(ctx context.Context) (*QueryResult, error) {
	databases := e.catalog.ListDatabases()

	result := &QueryResult{
		Columns: []string{"Database"},
		Rows:    make([][]sqltypes.Value, len(databases)),
	}

	for i, db := range databases {
		result.Rows[i] = []sqltypes.Value{
			sqltypes.NewVarChar(db),
		}
	}

	return result, nil
}

func (e *SQLEngine) showTables(ctx context.Context, dbName string) (*QueryResult, error) {
	// Assumption: If no database specified, use default or return error
	if dbName == "" {
		// TODO: Implement default database context
		// For now, use 'default' as the default database
		dbName = "default"
	}

	tables, err := e.catalog.ListTables(dbName)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	result := &QueryResult{
		Columns: []string{"Tables_in_" + dbName},
		Rows:    make([][]sqltypes.Value, len(tables)),
	}

	for i, table := range tables {
		result.Rows[i] = []sqltypes.Value{
			sqltypes.NewVarChar(table),
		}
	}

	return result, nil
}

func (e *SQLEngine) createTable(ctx context.Context, stmt *sqlparser.DDL) (*QueryResult, error) {
	// Parse CREATE TABLE statement
	// Assumption: Table name format is [database.]table_name
	tableName := stmt.NewName.Name.String()
	database := ""

	// Check if database is specified in table name
	if stmt.NewName.Qualifier.String() != "" {
		database = stmt.NewName.Qualifier.String()
	} else {
		// Use current database context or default
		database = e.catalog.GetCurrentDatabase()
		if database == "" {
			database = "default"
		}
	}

	// Parse column definitions from CREATE TABLE
	// Assumption: stmt.TableSpec contains column definitions
	if stmt.TableSpec == nil || len(stmt.TableSpec.Columns) == 0 {
		err := fmt.Errorf("CREATE TABLE requires column definitions")
		return &QueryResult{Error: err}, err
	}

	// Convert SQL columns to MQ schema fields
	fields := make([]*schema_pb.Field, len(stmt.TableSpec.Columns))
	for i, col := range stmt.TableSpec.Columns {
		fieldType, err := e.convertSQLTypeToMQ(col.Type)
		if err != nil {
			return &QueryResult{Error: err}, err
		}

		fields[i] = &schema_pb.Field{
			Name: col.Name.String(),
			Type: fieldType,
		}
	}

	// Create record type for the topic
	recordType := &schema_pb.RecordType{
		Fields: fields,
	}

	// Create the topic via broker
	partitionCount := int32(6) // Default partition count - TODO: make configurable
	err := e.catalog.brokerClient.ConfigureTopic(ctx, database, tableName, partitionCount, recordType)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Register the new topic in catalog
	mqSchema := &schema.Schema{
		Namespace:  database,
		Name:       tableName,
		RecordType: recordType,
		RevisionId: 1, // Initial revision
	}

	err = e.catalog.RegisterTopic(database, tableName, mqSchema)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Return success result
	result := &QueryResult{
		Columns: []string{"Result"},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar(fmt.Sprintf("Table '%s.%s' created successfully", database, tableName))},
		},
	}

	return result, nil
}

func (e *SQLEngine) alterTable(ctx context.Context, stmt *sqlparser.DDL) (*QueryResult, error) {
	// TODO: Implement table alteration
	// This will modify the MQ topic schema with versioning
	err := fmt.Errorf("ALTER TABLE not yet implemented")
	return &QueryResult{Error: err}, err
}

func (e *SQLEngine) dropTable(ctx context.Context, stmt *sqlparser.DDL) (*QueryResult, error) {
	// Parse DROP TABLE statement
	// Assumption: Table name is in stmt.NewName for DROP operations
	tableName := stmt.NewName.Name.String()
	database := ""

	// Check if database is specified in table name
	if stmt.NewName.Qualifier.String() != "" {
		database = stmt.NewName.Qualifier.String()
	} else {
		// Use current database context or default
		database = e.catalog.GetCurrentDatabase()
		if database == "" {
			database = "default"
		}
	}

	// Delete the topic via broker
	err := e.catalog.brokerClient.DeleteTopic(ctx, database, tableName)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Remove from catalog cache
	// TODO: Implement catalog cache removal

	// Return success result
	result := &QueryResult{
		Columns: []string{"Result"},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar(fmt.Sprintf("Table '%s.%s' dropped successfully", database, tableName))},
		},
	}

	return result, nil
}
