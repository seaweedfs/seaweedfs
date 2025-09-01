package command

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/query/engine"
)

func init() {
	cmdSql.Run = runSql
}

var cmdSql = &Command{
	UsageLine: "sql [-server=localhost:8888] [-interactive] [-file=query.sql] [-output=table|json|csv] [-database=dbname] [-query=\"SQL\"]",
	Short:     "advanced SQL query interface for SeaweedFS MQ topics with multiple execution modes",
	Long: `Enhanced SQL interface for SeaweedFS Message Queue topics with multiple execution modes.

Execution Modes:
- Interactive shell (default): weed sql --interactive
- Single query: weed sql --query "SELECT * FROM user_events"  
- Batch from file: weed sql --file queries.sql
- Context switching: weed sql --database analytics --interactive

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
  weed sql --interactive
  weed sql --query "SHOW DATABASES" --output json
  weed sql --file batch_queries.sql --output csv
  weed sql --database analytics --query "SELECT COUNT(*) FROM metrics"
  weed sql --server broker1:8888 --interactive
`,
}

var (
	sqlServer     = cmdSql.Flag.String("server", "localhost:8888", "SeaweedFS server address")
	sqlInteractive = cmdSql.Flag.Bool("interactive", false, "start interactive shell mode")
	sqlFile       = cmdSql.Flag.String("file", "", "execute SQL queries from file")
	sqlOutput     = cmdSql.Flag.String("output", "", "output format: table, json, csv (auto-detected if not specified)")
	sqlDatabase   = cmdSql.Flag.String("database", "", "default database context")
	sqlQuery      = cmdSql.Flag.String("query", "", "execute single SQL query")
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
	// Initialize SQL engine
	sqlEngine := engine.NewSQLEngine(*sqlServer)
	
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
	
	fmt.Printf("Executing query against %s...\n", *sqlServer)
	return executeAndDisplay(ctx, query, true)
}

// executeFileQueries processes SQL queries from a file
func executeFileQueries(ctx *SQLContext, filename string) bool {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("Error reading file %s: %v\n", filename, err)
		return false
	}

	if ctx.outputFormat == OutputTable && ctx.interactive {
		fmt.Printf("Executing queries from %s against %s...\n", filename, *sqlServer)
	}

	// Split file content into individual queries (simple approach)
	queries := strings.Split(string(content), ";")
	
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

// runInteractiveShell starts the enhanced interactive shell
func runInteractiveShell(ctx *SQLContext) bool {
	fmt.Println("🚀 SeaweedFS Enhanced SQL Interface")
	fmt.Println("Type 'help;' for help, 'exit;' to quit")
	fmt.Printf("Connected to: %s\n", *sqlServer)
	if ctx.currentDatabase != "" {
		fmt.Printf("Current database: %s\n", ctx.currentDatabase)
	}
	fmt.Println("✨ Advanced WHERE operators supported: <=, >=, !=, LIKE, IN")
	fmt.Println()

	// Interactive shell with command history (basic implementation)
	scanner := bufio.NewScanner(os.Stdin)
	var queryBuffer strings.Builder
	queryHistory := make([]string, 0)

	for {
		// Show prompt with current database context
		if queryBuffer.Len() == 0 {
			if ctx.currentDatabase != "" {
				fmt.Printf("seaweedfs:%s> ", ctx.currentDatabase)
			} else {
				fmt.Print("seaweedfs> ")
			}
		} else {
			fmt.Print("    -> ") // Continuation prompt
		}

		// Read line
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())

		// Handle special commands
		if line == "exit;" || line == "quit;" || line == "\\q" {
			fmt.Println("Goodbye! 👋")
			break
		}

		if line == "help;" {
			showEnhancedHelp()
			continue
		}

		// Handle database switching
		if strings.HasPrefix(strings.ToUpper(line), "USE ") {
			dbName := strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(line), "USE "))
			dbName = strings.TrimSuffix(dbName, ";")
			ctx.currentDatabase = dbName
			fmt.Printf("Database changed to: %s\n\n", dbName)
			continue
		}

		// Handle output format switching
		if strings.HasPrefix(strings.ToUpper(line), "\\FORMAT ") {
			format := strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(line), "\\FORMAT "))
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
			continue
		}

		if line == "" {
			continue
		}

		// Accumulate multi-line queries
		queryBuffer.WriteString(line)
		queryBuffer.WriteString(" ")

		// Execute when query ends with semicolon
		if strings.HasSuffix(line, ";") {
			query := strings.TrimSpace(queryBuffer.String())
			query = strings.TrimSuffix(query, ";") // Remove trailing semicolon

			// Add to history
			queryHistory = append(queryHistory, query)

			// Execute query
			executeAndDisplay(ctx, query, true)

			// Reset buffer for next query
			queryBuffer.Reset()
		}
	}

	return true
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
	if showTiming && ctx.outputFormat == OutputTable {
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
	fmt.Println(`🚀 SeaweedFS Enhanced SQL Interface Help:

📊 METADATA OPERATIONS:
  SHOW DATABASES;              - List all MQ namespaces
  SHOW TABLES;                 - List all topics in current namespace  
  SHOW TABLES FROM database;   - List topics in specific namespace
  DESCRIBE table_name;         - Show table schema

🔍 ADVANCED QUERYING:
  SELECT * FROM table_name;                    - Query all data
  SELECT col1, col2 FROM table WHERE ...;     - Column projection
  SELECT * FROM table WHERE id <= 100;        - Range filtering
  SELECT * FROM table WHERE name LIKE 'admin%'; - Pattern matching
  SELECT * FROM table WHERE status IN ('active', 'pending'); - Multi-value

📝 DDL OPERATIONS:
  CREATE TABLE topic (field1 INT, field2 STRING); - Create topic
  DROP TABLE table_name;                           - Delete topic

⚙️  SPECIAL COMMANDS:
  USE database_name;           - Switch database context
  \format table|json|csv       - Change output format
  help;                        - Show this help
  exit; or quit; or \q        - Exit interface

🎯 EXTENDED WHERE OPERATORS:
  =, <, >, <=, >=             - Comparison operators
  !=, <>                      - Not equal operators  
  LIKE 'pattern%'             - Pattern matching (% = any chars, _ = single char)
  IN (value1, value2, ...)    - Multi-value matching
  AND, OR                     - Logical operators

💡 EXAMPLES:
  SELECT * FROM user_events WHERE user_id >= 10 AND status != 'deleted';
  SELECT username FROM users WHERE email LIKE '%@company.com';
  SELECT * FROM logs WHERE level IN ('error', 'warning') AND timestamp >= '2023-01-01';

🌟 Current Status: Full WHERE clause support + Real MQ integration`)
}
