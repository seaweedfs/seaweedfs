package command

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/query/engine"
)

func init() {
	cmdSql.Run = runSql
}

var cmdSql = &Command{
	UsageLine: "sql [-server=localhost:8888]",
	Short:     "start a SQL query interface for SeaweedFS MQ topics",
	Long: `Start an interactive SQL shell to query SeaweedFS Message Queue topics as tables.

Features:
- SHOW DATABASES: List all MQ namespaces  
- SHOW TABLES: List topics in current namespace
- DESCRIBE table: Show table schema
- SELECT queries (coming soon)
- CREATE/ALTER/DROP TABLE operations (coming soon)

Assumptions:
- MQ namespaces map to SQL databases
- MQ topics map to SQL tables
- Messages are stored in Parquet format for efficient querying

Examples:
  weed sql
  weed sql -server=broker1:8888
`,
}

var (
	sqlServer = cmdSql.Flag.String("server", "localhost:8888", "SeaweedFS server address")
)

func runSql(command *Command, args []string) bool {
	fmt.Println("SeaweedFS SQL Interface")
	fmt.Println("Type 'help;' for help, 'exit;' to quit")
	fmt.Printf("Connected to: %s\n", *sqlServer)
	fmt.Println()

	// Initialize SQL engine
	// Assumption: Engine will connect to MQ broker on demand
	sqlEngine := engine.NewSQLEngine(*sqlServer)

	// Interactive shell loop
	scanner := bufio.NewScanner(os.Stdin)
	var queryBuffer strings.Builder
	
	for {
		// Show prompt
		if queryBuffer.Len() == 0 {
			fmt.Print("seaweedfs> ")
		} else {
			fmt.Print("    -> ")  // Continuation prompt
		}
		
		// Read line
		if !scanner.Scan() {
			break
		}
		
		line := strings.TrimSpace(scanner.Text())
		
		// Handle special commands
		if line == "exit;" || line == "quit;" || line == "\\q" {
			fmt.Println("Goodbye!")
			break
		}
		
		if line == "help;" {
			showHelp()
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
			
			executeQuery(sqlEngine, query)
			
			// Reset buffer for next query
			queryBuffer.Reset()
		}
	}
	
	return true
}

// executeQuery runs a SQL query and displays results
// Assumption: All queries are executed synchronously for simplicity
func executeQuery(engine *engine.SQLEngine, query string) {
	startTime := time.Now()
	
	// Execute the query
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	result, err := engine.ExecuteSQL(ctx, query)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	if result.Error != nil {
		fmt.Printf("Query Error: %v\n", result.Error)
		return
	}
	
	// Display results
	displayQueryResult(result)
	
	// Show execution time
	elapsed := time.Since(startTime)
	fmt.Printf("\n(%d rows in set, %.2f sec)\n\n", len(result.Rows), elapsed.Seconds())
}

// displayQueryResult formats and displays query results in table format
// Assumption: Results fit in terminal width (simple formatting for now)
func displayQueryResult(result *engine.QueryResult) {
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

func showHelp() {
	fmt.Println(`
SeaweedFS SQL Interface Help:

Available Commands:
  SHOW DATABASES;              - List all MQ namespaces
  SHOW TABLES;                 - List all topics in current namespace  
  SHOW TABLES FROM database;   - List topics in specific namespace
  DESCRIBE table_name;         - Show table schema (coming soon)
  
  SELECT * FROM table_name;    - Query table data (coming soon)
  CREATE TABLE ...;            - Create new topic (coming soon)
  ALTER TABLE ...;             - Modify topic schema (coming soon)
  DROP TABLE table_name;       - Delete topic (coming soon)

Special Commands:
  help;                        - Show this help
  exit; or quit; or \q        - Exit the SQL interface

Notes:
- MQ namespaces appear as SQL databases
- MQ topics appear as SQL tables
- All queries must end with semicolon (;)
- Multi-line queries are supported

Current Status: Basic metadata operations implemented
`)
}
