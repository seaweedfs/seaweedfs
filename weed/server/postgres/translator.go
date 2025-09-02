package postgres

import (
	"fmt"
	"regexp"
	"strings"
)

// PostgreSQL to SeaweedFS SQL translator
type PostgreSQLTranslator struct {
	systemQueries map[string]string
	patterns      map[*regexp.Regexp]string
}

// initSystemQueries initializes the system query mappings
func (t *PostgreSQLTranslator) initSystemQueries() {
	t.systemQueries = map[string]string{
		// Version queries
		"SELECT version()":            "SELECT 'SeaweedFS 1.0 (PostgreSQL 14.0 compatible)' as version",
		"SELECT version() AS version": "SELECT 'SeaweedFS 1.0 (PostgreSQL 14.0 compatible)' as version",
		"select version()":            "SELECT 'SeaweedFS 1.0 (PostgreSQL 14.0 compatible)' as version",

		// Current database
		"SELECT current_database()":                     "SELECT 'default' as current_database",
		"select current_database()":                     "SELECT 'default' as current_database",
		"SELECT current_database() AS current_database": "SELECT 'default' as current_database",

		// Current user
		"SELECT current_user":                 "SELECT 'seaweedfs' as current_user",
		"select current_user":                 "SELECT 'seaweedfs' as current_user",
		"SELECT current_user AS current_user": "SELECT 'seaweedfs' as current_user",
		"SELECT user":                         "SELECT 'seaweedfs' as user",

		// Session info
		"SELECT session_user":                       "SELECT 'seaweedfs' as session_user",
		"SELECT current_setting('server_version')":  "SELECT '14.0' as server_version",
		"SELECT current_setting('server_encoding')": "SELECT 'UTF8' as server_encoding",
		"SELECT current_setting('client_encoding')": "SELECT 'UTF8' as client_encoding",

		// Simple system queries
		"SELECT 1":         "SELECT 1",
		"select 1":         "SELECT 1",
		"SELECT 1 AS test": "SELECT 1 AS test",

		// Database listing
		"SELECT datname FROM pg_database":                  "SHOW DATABASES",
		"SELECT datname FROM pg_database ORDER BY datname": "SHOW DATABASES",

		// Table listing
		"SELECT tablename FROM pg_tables":                                                "SHOW TABLES",
		"SELECT schemaname, tablename FROM pg_tables":                                    "SHOW TABLES",
		"SELECT table_name FROM information_schema.tables":                               "SHOW TABLES",
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'": "SHOW TABLES",

		// Schema queries
		"SELECT schema_name FROM information_schema.schemata": "SELECT 'public' as schema_name",
		"SELECT nspname FROM pg_namespace":                    "SELECT 'public' as nspname",

		// Connection info
		"SELECT inet_client_addr()": "SELECT '127.0.0.1' as inet_client_addr",
		"SELECT inet_client_port()": "SELECT 0 as inet_client_port",
		"SELECT pg_backend_pid()":   "SELECT 1 as pg_backend_pid",

		// Transaction info
		"SELECT txid_current()":      "SELECT 1 as txid_current",
		"SELECT pg_is_in_recovery()": "SELECT false as pg_is_in_recovery",

		// Statistics
		"SELECT COUNT(*) FROM pg_stat_user_tables": "SELECT 0 as count",

		// Empty system tables
		"SELECT * FROM pg_settings LIMIT 0": "SELECT 'name' as name, 'setting' as setting, 'unit' as unit, 'category' as category, 'short_desc' as short_desc, 'extra_desc' as extra_desc, 'context' as context, 'vartype' as vartype, 'source' as source, 'min_val' as min_val, 'max_val' as max_val, 'enumvals' as enumvals, 'boot_val' as boot_val, 'reset_val' as reset_val, 'sourcefile' as sourcefile, 'sourceline' as sourceline, 'pending_restart' as pending_restart WHERE 1=0",

		"SELECT * FROM pg_type LIMIT 0": "SELECT 'oid' as oid, 'typname' as typname, 'typlen' as typlen WHERE 1=0",

		"SELECT * FROM pg_class LIMIT 0": "SELECT 'oid' as oid, 'relname' as relname, 'relkind' as relkind WHERE 1=0",
	}

	// Initialize regex patterns for more complex queries
	t.patterns = map[*regexp.Regexp]string{
		// \d commands (psql describe commands)
		regexp.MustCompile(`(?i)\\d\+?\s*$`):       "SHOW TABLES",
		regexp.MustCompile(`(?i)\\dt\+?\s*$`):      "SHOW TABLES",
		regexp.MustCompile(`(?i)\\dn\+?\s*$`):      "SELECT 'public' as name, 'seaweedfs' as owner",
		regexp.MustCompile(`(?i)\\l\+?\s*$`):       "SHOW DATABASES",
		regexp.MustCompile(`(?i)\\d\+?\s+(\w+)$`):  "DESCRIBE $1",
		regexp.MustCompile(`(?i)\\dt\+?\s+(\w+)$`): "DESCRIBE $1",

		// pg_catalog queries
		regexp.MustCompile(`(?i)SELECT\s+.*\s+FROM\s+pg_catalog\.pg_tables`): "SHOW TABLES",
		regexp.MustCompile(`(?i)SELECT\s+.*\s+FROM\s+pg_tables`):             "SHOW TABLES",
		regexp.MustCompile(`(?i)SELECT\s+.*\s+FROM\s+pg_database`):           "SHOW DATABASES",

		// SHOW commands (already supported but normalize)
		regexp.MustCompile(`(?i)SHOW\s+DATABASES?\s*;?\s*$`): "SHOW DATABASES",
		regexp.MustCompile(`(?i)SHOW\s+TABLES?\s*;?\s*$`):    "SHOW TABLES",
		regexp.MustCompile(`(?i)SHOW\s+SCHEMAS?\s*;?\s*$`):   "SELECT 'public' as schema_name",

		// BEGIN/COMMIT/ROLLBACK (no-op for read-only)
		regexp.MustCompile(`(?i)BEGIN\s*;?\s*$`):               "SELECT 'BEGIN' as status",
		regexp.MustCompile(`(?i)START\s+TRANSACTION\s*;?\s*$`): "SELECT 'BEGIN' as status",
		regexp.MustCompile(`(?i)COMMIT\s*;?\s*$`):              "SELECT 'COMMIT' as status",
		regexp.MustCompile(`(?i)ROLLBACK\s*;?\s*$`):            "SELECT 'ROLLBACK' as status",

		// SET commands (mostly no-op)
		regexp.MustCompile(`(?i)SET\s+.*\s*;?\s*$`): "SELECT 'SET' as status",

		// Column information queries
		regexp.MustCompile(`(?i)SELECT\s+.*\s+FROM\s+information_schema\.columns\s+WHERE\s+table_name\s*=\s*'(\w+)'`): "DESCRIBE $1",
	}
}

// TranslateQuery translates a PostgreSQL query to SeaweedFS SQL
func (t *PostgreSQLTranslator) TranslateQuery(pgSQL string) (string, error) {
	// Trim whitespace and semicolons
	query := strings.TrimSpace(pgSQL)
	query = strings.TrimSuffix(query, ";")

	// Check for exact matches first
	if seaweedSQL, exists := t.systemQueries[query]; exists {
		return seaweedSQL, nil
	}

	// Check case-insensitive exact matches
	queryLower := strings.ToLower(query)
	for pgQuery, seaweedSQL := range t.systemQueries {
		if strings.ToLower(pgQuery) == queryLower {
			return seaweedSQL, nil
		}
	}

	// Check regex patterns
	for pattern, replacement := range t.patterns {
		if pattern.MatchString(query) {
			// Handle replacements with capture groups
			if strings.Contains(replacement, "$") {
				return pattern.ReplaceAllString(query, replacement), nil
			}
			return replacement, nil
		}
	}

	// Handle psql meta-commands
	if strings.HasPrefix(query, "\\") {
		return t.translateMetaCommand(query)
	}

	// Handle information_schema queries
	if strings.Contains(strings.ToLower(query), "information_schema") {
		return t.translateInformationSchema(query)
	}

	// Handle pg_catalog queries
	if strings.Contains(strings.ToLower(query), "pg_catalog") || strings.Contains(strings.ToLower(query), "pg_") {
		return t.translatePgCatalog(query)
	}

	// For regular queries, pass through as-is
	// The SeaweedFS SQL engine will handle standard SQL
	return query, nil
}

// translateMetaCommand translates psql meta-commands
func (t *PostgreSQLTranslator) translateMetaCommand(cmd string) (string, error) {
	cmd = strings.TrimSpace(cmd)

	switch {
	case cmd == "\\d" || cmd == "\\dt":
		return "SHOW TABLES", nil
	case cmd == "\\l":
		return "SHOW DATABASES", nil
	case cmd == "\\dn":
		return "SELECT 'public' as schema_name, 'seaweedfs' as owner", nil
	case cmd == "\\du":
		return "SELECT 'seaweedfs' as rolname, true as rolsuper, true as rolcreaterole, true as rolcreatedb", nil
	case strings.HasPrefix(cmd, "\\d "):
		// Describe table
		tableName := strings.TrimSpace(cmd[3:])
		return fmt.Sprintf("DESCRIBE %s", tableName), nil
	case strings.HasPrefix(cmd, "\\dt "):
		// Describe table (table-specific)
		tableName := strings.TrimSpace(cmd[4:])
		return fmt.Sprintf("DESCRIBE %s", tableName), nil
	case cmd == "\\q":
		return "SELECT 'quit' as status", fmt.Errorf("client requested quit")
	case cmd == "\\h" || cmd == "\\help":
		return "SELECT 'SeaweedFS PostgreSQL Interface - Limited command support' as help", nil
	case cmd == "\\?":
		return "SELECT 'Available: \\d (tables), \\l (databases), \\q (quit)' as commands", nil
	default:
		return "SELECT 'Unsupported meta-command' as error", fmt.Errorf("unsupported meta-command: %s", cmd)
	}
}

// translateInformationSchema translates INFORMATION_SCHEMA queries
func (t *PostgreSQLTranslator) translateInformationSchema(query string) (string, error) {
	queryLower := strings.ToLower(query)

	if strings.Contains(queryLower, "information_schema.tables") {
		return "SHOW TABLES", nil
	}

	if strings.Contains(queryLower, "information_schema.columns") {
		// Extract table name if present
		re := regexp.MustCompile(`(?i)table_name\s*=\s*'(\w+)'`)
		matches := re.FindStringSubmatch(query)
		if len(matches) > 1 {
			return fmt.Sprintf("DESCRIBE %s", matches[1]), nil
		}
		return "SHOW TABLES", nil // Return tables if no specific table
	}

	if strings.Contains(queryLower, "information_schema.schemata") {
		return "SELECT 'public' as schema_name, 'seaweedfs' as schema_owner", nil
	}

	// Default fallback
	return "SELECT 'information_schema query not supported' as error", nil
}

// translatePgCatalog translates PostgreSQL catalog queries
func (t *PostgreSQLTranslator) translatePgCatalog(query string) (string, error) {
	queryLower := strings.ToLower(query)

	// pg_tables
	if strings.Contains(queryLower, "pg_tables") {
		return "SHOW TABLES", nil
	}

	// pg_database
	if strings.Contains(queryLower, "pg_database") {
		return "SHOW DATABASES", nil
	}

	// pg_namespace
	if strings.Contains(queryLower, "pg_namespace") {
		return "SELECT 'public' as nspname, 2200 as oid", nil
	}

	// pg_class (tables, indexes, etc.)
	if strings.Contains(queryLower, "pg_class") {
		return "SHOW TABLES", nil
	}

	// pg_type (data types)
	if strings.Contains(queryLower, "pg_type") {
		return t.generatePgTypeResult(), nil
	}

	// pg_attribute (column info)
	if strings.Contains(queryLower, "pg_attribute") {
		return "SELECT 'attname' as attname, 'atttypid' as atttypid, 'attnum' as attnum WHERE 1=0", nil
	}

	// pg_settings
	if strings.Contains(queryLower, "pg_settings") {
		return t.generatePgSettingsResult(), nil
	}

	// pg_stat_* tables
	if strings.Contains(queryLower, "pg_stat_") {
		return "SELECT 0 as count", nil
	}

	// Default: return empty result for unknown pg_ queries
	return "SELECT 'pg_catalog query not fully supported' as notice", nil
}

// generatePgTypeResult generates a basic pg_type result
func (t *PostgreSQLTranslator) generatePgTypeResult() string {
	return `
	SELECT * FROM (
		SELECT 16 as oid, 'bool' as typname, 1 as typlen, 'b' as typtype
		UNION ALL
		SELECT 20 as oid, 'int8' as typname, 8 as typlen, 'b' as typtype  
		UNION ALL
		SELECT 23 as oid, 'int4' as typname, 4 as typlen, 'b' as typtype
		UNION ALL  
		SELECT 25 as oid, 'text' as typname, -1 as typlen, 'b' as typtype
		UNION ALL
		SELECT 701 as oid, 'float8' as typname, 8 as typlen, 'b' as typtype
		UNION ALL
		SELECT 1043 as oid, 'varchar' as typname, -1 as typlen, 'b' as typtype
		UNION ALL
		SELECT 1114 as oid, 'timestamp' as typname, 8 as typlen, 'b' as typtype
	) t WHERE 1=0
	`
}

// generatePgSettingsResult generates a basic pg_settings result
func (t *PostgreSQLTranslator) generatePgSettingsResult() string {
	return `
	SELECT * FROM (
		SELECT 'server_version' as name, '14.0' as setting, NULL as unit, 'Version and Platform Compatibility' as category, 'SeaweedFS version' as short_desc
		UNION ALL
		SELECT 'server_encoding' as name, 'UTF8' as setting, NULL as unit, 'Client Connection Defaults' as category, 'Server encoding' as short_desc
		UNION ALL  
		SELECT 'client_encoding' as name, 'UTF8' as setting, NULL as unit, 'Client Connection Defaults' as category, 'Client encoding' as short_desc
		UNION ALL
		SELECT 'max_connections' as name, '100' as setting, NULL as unit, 'Connections and Authentication' as category, 'Maximum connections' as short_desc
	) s WHERE 1=0
	`
}

// GetDatabaseName returns the appropriate database name for the session
func (t *PostgreSQLTranslator) GetDatabaseName(requestedDB string) string {
	if requestedDB == "" || requestedDB == "postgres" || requestedDB == "template1" {
		return "default"
	}
	return requestedDB
}

// IsSystemQuery checks if a query is a system/meta query that doesn't access actual data
func (t *PostgreSQLTranslator) IsSystemQuery(query string) bool {
	queryLower := strings.ToLower(strings.TrimSpace(query))

	// System function calls
	systemFunctions := []string{
		"version()", "current_database()", "current_user", "session_user",
		"current_setting(", "inet_client_", "pg_backend_pid()", "txid_current()",
		"pg_is_in_recovery()",
	}

	for _, fn := range systemFunctions {
		if strings.Contains(queryLower, fn) {
			return true
		}
	}

	// System table queries
	systemTables := []string{
		"pg_catalog", "pg_tables", "pg_database", "pg_namespace", "pg_class",
		"pg_type", "pg_attribute", "pg_settings", "pg_stat_", "information_schema",
	}

	for _, table := range systemTables {
		if strings.Contains(queryLower, table) {
			return true
		}
	}

	// Meta commands
	if strings.HasPrefix(queryLower, "\\") {
		return true
	}

	// Transaction control
	transactionCommands := []string{"begin", "commit", "rollback", "start transaction", "set "}
	for _, cmd := range transactionCommands {
		if strings.HasPrefix(queryLower, cmd) {
			return true
		}
	}

	return false
}
