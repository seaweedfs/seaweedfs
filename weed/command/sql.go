package command

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/peterh/liner"
	"github.com/seaweedfs/seaweedfs/weed/query/engine"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/sqlutil"
)

func init() {
	cmdSql.Run = runSql
}

var cmdSql = &Command{
	UsageLine: "sql [-master=localhost:9333] [-interactive] [-file=query.sql] [-output=table|json|csv] [-database=dbname] [-query=\"SQL\"]",
	Short:     "advanced SQL query interface for SeaweedFS MQ topics with multiple execution modes",
	Long: `Enhanced SQL interface for SeaweedFS Message Queue topics with multiple execution modes.

Execution Modes:
- Interactive shell (default): weed sql -interactive
- Single query: weed sql -query "SELECT * FROM user_events"  
- Batch from file: weed sql -file queries.sql
- Context switching: weed sql -database analytics -interactive

Output Formats:
- table: ASCII table format (default for interactive)
- json: JSON format (default for non-interactive) 
- csv: Comma-separated values

Features:
- Full WHERE clause support (=, <, >, <=, >=, !=, LIKE, IN)
- Advanced pattern matching with LIKE wildcards (%, _)
- Multi-value filtering with IN operator
- Real MQ namespace and topic discovery
- Database context switching

Examples:
  weed sql -interactive
  weed sql -query "SHOW DATABASES" -output json
  weed sql -file batch_queries.sql -output csv
  weed sql -database analytics -query "SELECT COUNT(*) FROM metrics"
  weed sql -master broker1:9333 -interactive
`,
}

var (
	sqlMaster      = cmdSql.Flag.String("master", "localhost:9333", "SeaweedFS master server HTTP address")
	sqlInteractive = cmdSql.Flag.Bool("interactive", false, "start interactive shell mode")
	sqlFile        = cmdSql.Flag.String("file", "", "execute SQL queries from file")
	sqlOutput      = cmdSql.Flag.String("output", "", "output format: table, json, csv (auto-detected if not specified)")
	sqlDatabase    = cmdSql.Flag.String("database", "", "default database context")
	sqlQuery       = cmdSql.Flag.String("query", "", "execute single SQL query")
)

// OutputFormat represents different output formatting options
type OutputFormat string

const (
	OutputTable OutputFormat = "table"
	OutputJSON  OutputFormat = "json"
	OutputCSV   OutputFormat = "csv"
)

// SQLContext holds the execution context for SQL operations
type SQLContext struct {
	engine          *engine.SQLEngine
	currentDatabase string
	outputFormat    OutputFormat
	interactive     bool
}

func runSql(command *Command, args []string) bool {
	// Initialize SQL engine with master address for service discovery
	sqlEngine := engine.NewSQLEngine(*sqlMaster)

	// Determine execution mode and output format
	interactive := *sqlInteractive || (*sqlQuery == "" && *sqlFile == "")
	outputFormat := determineOutputFormat(*sqlOutput, interactive)

	// Create SQL context
	ctx := &SQLContext{
		engine:          sqlEngine,
		currentDatabase: *sqlDatabase,
		outputFormat:    outputFormat,
		interactive:     interactive,
	}

	// Set current database in SQL engine if specified via command line
	if *sqlDatabase != "" {
		ctx.engine.GetCatalog().SetCurrentDatabase(*sqlDatabase)
	}

	// Execute based on mode
	switch {
	case *sqlQuery != "":
		// Single query mode
		return executeSingleQuery(ctx, *sqlQuery)
	case *sqlFile != "":
		// Batch file mode
		return executeFileQueries(ctx, *sqlFile)
	default:
		// Interactive mode
		return runInteractiveShell(ctx)
	}
}

// determineOutputFormat selects the appropriate output format
func determineOutputFormat(specified string, interactive bool) OutputFormat {
	switch strings.ToLower(specified) {
	case "table":
		return OutputTable
	case "json":
		return OutputJSON
	case "csv":
		return OutputCSV
	default:
		// Auto-detect based on mode
		if interactive {
			return OutputTable
		}
		return OutputJSON
	}
}

// executeSingleQuery executes a single query and outputs the result
func executeSingleQuery(ctx *SQLContext, query string) bool {
	if ctx.outputFormat != OutputTable {
		// Suppress banner for non-interactive output
		return executeAndDisplay(ctx, query, false)
	}

	fmt.Printf("Executing query against %s...\n", *sqlMaster)
	return executeAndDisplay(ctx, query, true)
}

// executeFileQueries processes SQL queries from a file
func executeFileQueries(ctx *SQLContext, filename string) bool {
	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("Error reading file %s: %v\n", filename, err)
		return false
	}

	if ctx.outputFormat == OutputTable && ctx.interactive {
		fmt.Printf("Executing queries from %s against %s...\n", filename, *sqlMaster)
	}

	// Split file content into individual queries (robust approach)
	queries := sqlutil.SplitStatements(string(content))

	for i, query := range queries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}

		if ctx.outputFormat == OutputTable && len(queries) > 1 {
			fmt.Printf("\n--- Query %d ---\n", i+1)
		}

		if !executeAndDisplay(ctx, query, ctx.outputFormat == OutputTable) {
			return false
		}
	}

	return true
}

// runInteractiveShell starts the enhanced interactive shell with readline support
func runInteractiveShell(ctx *SQLContext) bool {
	fmt.Println("SeaweedFS Enhanced SQL Interface")
	fmt.Println("Type 'help;' for help, 'exit;' to quit")
	fmt.Printf("Connected to master: %s\n", *sqlMaster)
	if ctx.currentDatabase != "" {
		fmt.Printf("Current database: %s\n", ctx.currentDatabase)
	}
	fmt.Println("Advanced WHERE operators supported: <=, >=, !=, LIKE, IN")
	fmt.Println("Use up/down arrows for command history")
	fmt.Println()

	// Initialize liner for readline functionality
	line := liner.NewLiner()
	defer line.Close()

	// Handle Ctrl+C gracefully
	line.SetCtrlCAborts(true)
	grace.OnInterrupt(func() {
		line.Close()
	})

	// Load command history
	historyPath := path.Join(os.TempDir(), "weed-sql-history")
	if f, err := os.Open(historyPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	// Save history on exit
	defer func() {
		if f, err := os.Create(historyPath); err == nil {
			line.WriteHistory(f)
			f.Close()
		}
	}()

	var queryBuffer strings.Builder

	for {
		// Show prompt with current database context
		var prompt string
		if queryBuffer.Len() == 0 {
			if ctx.currentDatabase != "" {
				prompt = fmt.Sprintf("seaweedfs:%s> ", ctx.currentDatabase)
			} else {
				prompt = "seaweedfs> "
			}
		} else {
			prompt = "    -> " // Continuation prompt
		}

		// Read line with readline support
		input, err := line.Prompt(prompt)
		if err != nil {
			if err == liner.ErrPromptAborted {
				fmt.Println("Query cancelled")
				queryBuffer.Reset()
				continue
			}
			if err != io.EOF {
				fmt.Printf("Input error: %v\n", err)
			}
			break
		}

		lineStr := strings.TrimSpace(input)

		// Handle empty lines
		if lineStr == "" {
			continue
		}

		// Accumulate lines in query buffer
		if queryBuffer.Len() > 0 {
			queryBuffer.WriteString(" ")
		}
		queryBuffer.WriteString(lineStr)

		// Check if we have a complete statement (ends with semicolon or special command)
		fullQuery := strings.TrimSpace(queryBuffer.String())
		isComplete := strings.HasSuffix(lineStr, ";") ||
			isSpecialCommand(fullQuery)

		if !isComplete {
			continue // Continue reading more lines
		}

		// Add completed command to history
		line.AppendHistory(fullQuery)

		// Handle special commands (with or without semicolon)
		cleanQuery := strings.TrimSuffix(fullQuery, ";")
		cleanQuery = strings.TrimSpace(cleanQuery)

		if cleanQuery == "exit" || cleanQuery == "quit" || cleanQuery == "\\q" {
			fmt.Println("Goodbye!")
			break
		}

		if cleanQuery == "help" {
			showEnhancedHelp()
			queryBuffer.Reset()
			continue
		}

		// Handle database switching - use proper SQL parser instead of manual parsing
		if strings.HasPrefix(strings.ToUpper(cleanQuery), "USE ") {
			// Execute USE statement through the SQL engine for proper parsing
			result, err := ctx.engine.ExecuteSQL(context.Background(), cleanQuery)
			if err != nil {
				fmt.Printf("Error: %v\n\n", err)
			} else if result.Error != nil {
				fmt.Printf("Error: %v\n\n", result.Error)
			} else {
				// Extract the database name from the result message for CLI context
				if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
					message := result.Rows[0][0].ToString()
					// Extract database name from "Database changed to: dbname"
					if strings.HasPrefix(message, "Database changed to: ") {
						ctx.currentDatabase = strings.TrimPrefix(message, "Database changed to: ")
					}
					fmt.Printf("%s\n\n", message)
				}
			}
			queryBuffer.Reset()
			continue
		}

		// Handle output format switching
		if strings.HasPrefix(strings.ToUpper(cleanQuery), "\\FORMAT ") {
			format := strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(cleanQuery), "\\FORMAT "))
			switch format {
			case "TABLE":
				ctx.outputFormat = OutputTable
				fmt.Println("Output format set to: table")
			case "JSON":
				ctx.outputFormat = OutputJSON
				fmt.Println("Output format set to: json")
			case "CSV":
				ctx.outputFormat = OutputCSV
				fmt.Println("Output format set to: csv")
			default:
				fmt.Printf("Invalid format: %s. Supported: table, json, csv\n", format)
			}
			queryBuffer.Reset()
			continue
		}

		// Execute SQL query (without semicolon)
		executeAndDisplay(ctx, cleanQuery, true)

		// Reset buffer for next query
		queryBuffer.Reset()
	}

	return true
}

// isSpecialCommand checks if a command is a special command that doesn't require semicolon
func isSpecialCommand(query string) bool {
	cleanQuery := strings.TrimSuffix(strings.TrimSpace(query), ";")
	cleanQuery = strings.ToLower(cleanQuery)

	// Special commands that work with or without semicolon
	specialCommands := []string{
		"exit", "quit", "\\q", "help",
	}

	for _, cmd := range specialCommands {
		if cleanQuery == cmd {
			return true
		}
	}

	// Commands that are exactly specific commands (not just prefixes)
	parts := strings.Fields(strings.ToUpper(cleanQuery))
	if len(parts) == 0 {
		return false
	}
	return (parts[0] == "USE" && len(parts) >= 2) ||
		strings.HasPrefix(strings.ToUpper(cleanQuery), "\\FORMAT ")
}

// executeAndDisplay executes a query and displays the result in the specified format
func executeAndDisplay(ctx *SQLContext, query string, showTiming bool) bool {
	startTime := time.Now()

	// Execute the query
	execCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := ctx.engine.ExecuteSQL(execCtx, query)
	if err != nil {
		if ctx.outputFormat == OutputJSON {
			errorResult := map[string]interface{}{
				"error": err.Error(),
				"query": query,
			}
			jsonBytes, _ := json.MarshalIndent(errorResult, "", "  ")
			fmt.Println(string(jsonBytes))
		} else {
			fmt.Printf("Error: %v\n", err)
		}
		return false
	}

	if result.Error != nil {
		if ctx.outputFormat == OutputJSON {
			errorResult := map[string]interface{}{
				"error": result.Error.Error(),
				"query": query,
			}
			jsonBytes, _ := json.MarshalIndent(errorResult, "", "  ")
			fmt.Println(string(jsonBytes))
		} else {
			fmt.Printf("Query Error: %v\n", result.Error)
		}
		return false
	}

	// Display results in the specified format
	switch ctx.outputFormat {
	case OutputTable:
		displayTableResult(result)
	case OutputJSON:
		displayJSONResult(result)
	case OutputCSV:
		displayCSVResult(result)
	}

	// Show execution time for interactive/table mode
	// Only show timing if there are columns or if result is truly empty
	if showTiming && ctx.outputFormat == OutputTable && (len(result.Columns) > 0 || len(result.Rows) == 0) {
		elapsed := time.Since(startTime)
		fmt.Printf("\n(%d rows in set, %.3f sec)\n\n", len(result.Rows), elapsed.Seconds())
	}

	return true
}

// displayTableResult formats and displays query results in ASCII table format
func displayTableResult(result *engine.QueryResult) {
	if len(result.Columns) == 0 {
		fmt.Println("Empty result set")
		return
	}

	// Calculate column widths for formatting
	colWidths := make([]int, len(result.Columns))
	for i, col := range result.Columns {
		colWidths[i] = len(col)
	}

	// Check data for wider columns
	for _, row := range result.Rows {
		for i, val := range row {
			if i < len(colWidths) {
				valStr := val.ToString()
				if len(valStr) > colWidths[i] {
					colWidths[i] = len(valStr)
				}
			}
		}
	}

	// Print header separator
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2) + "+")
	}
	fmt.Println()

	// Print column headers
	fmt.Print("|")
	for i, col := range result.Columns {
		fmt.Printf(" %-*s |", colWidths[i], col)
	}
	fmt.Println()

	// Print separator
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2) + "+")
	}
	fmt.Println()

	// Print data rows
	for _, row := range result.Rows {
		fmt.Print("|")
		for i, val := range row {
			if i < len(colWidths) {
				fmt.Printf(" %-*s |", colWidths[i], val.ToString())
			}
		}
		fmt.Println()
	}

	// Print bottom separator
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2) + "+")
	}
	fmt.Println()
}

// displayJSONResult outputs query results in JSON format
func displayJSONResult(result *engine.QueryResult) {
	// Convert result to JSON-friendly format
	jsonResult := map[string]interface{}{
		"columns": result.Columns,
		"rows":    make([]map[string]interface{}, len(result.Rows)),
		"count":   len(result.Rows),
	}

	// Convert rows to JSON objects
	for i, row := range result.Rows {
		rowObj := make(map[string]interface{})
		for j, val := range row {
			if j < len(result.Columns) {
				rowObj[result.Columns[j]] = val.ToString()
			}
		}
		jsonResult["rows"].([]map[string]interface{})[i] = rowObj
	}

	// Marshal and print JSON
	jsonBytes, err := json.MarshalIndent(jsonResult, "", "  ")
	if err != nil {
		fmt.Printf("Error formatting JSON: %v\n", err)
		return
	}

	fmt.Println(string(jsonBytes))
}

// displayCSVResult outputs query results in CSV format
func displayCSVResult(result *engine.QueryResult) {
	// Handle execution plan results specially to avoid CSV quoting issues
	if len(result.Columns) == 1 && result.Columns[0] == "Query Execution Plan" {
		// For execution plans, output directly without CSV encoding to avoid quotes
		for _, row := range result.Rows {
			if len(row) > 0 {
				fmt.Println(row[0].ToString())
			}
		}
		return
	}

	// Standard CSV output for regular query results
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// Write headers
	if err := writer.Write(result.Columns); err != nil {
		fmt.Printf("Error writing CSV headers: %v\n", err)
		return
	}

	// Write data rows
	for _, row := range result.Rows {
		csvRow := make([]string, len(row))
		for i, val := range row {
			csvRow[i] = val.ToString()
		}
		if err := writer.Write(csvRow); err != nil {
			fmt.Printf("Error writing CSV row: %v\n", err)
			return
		}
	}
}

func showEnhancedHelp() {
	fmt.Println(`SeaweedFS Enhanced SQL Interface Help:

METADATA OPERATIONS:
  SHOW DATABASES;              - List all MQ namespaces
  SHOW TABLES;                 - List all topics in current namespace  
  SHOW TABLES FROM database;   - List topics in specific namespace
  DESCRIBE table_name;         - Show table schema

ADVANCED QUERYING:
  SELECT * FROM table_name;                    - Query all data
  SELECT col1, col2 FROM table WHERE ...;     - Column projection
  SELECT * FROM table WHERE id <= 100;        - Range filtering
  SELECT * FROM table WHERE name LIKE 'admin%'; - Pattern matching
  SELECT * FROM table WHERE status IN ('active', 'pending'); - Multi-value
  SELECT COUNT(*), MAX(id), MIN(id) FROM ...;  - Aggregation functions

QUERY ANALYSIS:
  EXPLAIN SELECT ...;                          - Show hierarchical execution plan
                                                 (data sources, optimizations, timing)

DDL OPERATIONS:
  CREATE TABLE topic (field1 INT, field2 STRING); - Create topic
  Note: ALTER TABLE and DROP TABLE are not supported

SPECIAL COMMANDS:
  USE database_name;           - Switch database context
  \format table|json|csv       - Change output format
  help;                        - Show this help
  exit; or quit; or \q        - Exit interface

EXTENDED WHERE OPERATORS:
  =, <, >, <=, >=             - Comparison operators
  !=, <>                      - Not equal operators  
  LIKE 'pattern%'             - Pattern matching (% = any chars, _ = single char)
  IN (value1, value2, ...)    - Multi-value matching
  AND, OR                     - Logical operators

EXAMPLES:
  SELECT * FROM user_events WHERE user_id >= 10 AND status != 'deleted';
  SELECT username FROM users WHERE email LIKE '%@company.com';
  SELECT * FROM logs WHERE level IN ('error', 'warning') AND timestamp >= '2023-01-01';
  EXPLAIN SELECT MAX(id) FROM events;  -- View execution plan

Current Status: Full WHERE clause support + Real MQ integration`)
}
