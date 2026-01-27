package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestIsSqlState(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		code     string
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			code:     "3D000",
			expected: false,
		},
		{
			name:     "generic error",
			err:      errors.New("some error"),
			code:     "3D000",
			expected: false,
		},
		{
			name:     "matching pg error",
			err:      &pgconn.PgError{Code: "3D000", Message: "database does not exist"},
			code:     "3D000",
			expected: true,
		},
		{
			name:     "mismatching pg error",
			err:      &pgconn.PgError{Code: "42P04", Message: "database already exists"},
			code:     "3D000",
			expected: false,
		},
		{
			name:     "wrapped pg error",
			err:      fmt.Errorf("execute failed: %w", &pgconn.PgError{Code: "3D000"}),
			code:     "3D000",
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isSqlState(tc.err, tc.code)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestBuildUrl(t *testing.T) {
	cases := []struct {
		name                string
		user                string
		password            string
		hostname            string
		port                int
		database            string
		schema              string
		sslmode             string
		sslcert             string
		sslkey              string
		sslrootcert         string
		sslcrl              string
		pgbouncerCompatible bool
		expectedMasked      string
	}{
		{
			name:           "basic",
			user:           "testuser",
			password:       "secretpassword",
			hostname:       "localhost",
			port:           5432,
			database:       "testdb",
			expectedMasked: "password=*****",
		},
		{
			name:           "empty password",
			user:           "testuser",
			password:       "",
			hostname:       "localhost",
			port:           5432,
			database:       "testdb",
			expectedMasked: "", // Should not contain password field if empty
		},
		{
			name:           "all fields",
			user:           "user",
			password:       "secretpass",
			hostname:       "host",
			port:           5432,
			database:       "db",
			schema:         "public",
			sslmode:        "verify-full",
			sslcert:        "cert.pem",
			sslkey:         "key.pem",
			sslrootcert:    "root.pem",
			sslcrl:         "crl.pem",
			expectedMasked: "password=*****",
		},
	}

	store := &PostgresStore{}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, maskedUrl := store.buildUrl(
				tc.user, tc.password, tc.hostname, tc.port,
				tc.database, tc.schema, tc.sslmode, tc.sslcert,
				tc.sslkey, tc.sslrootcert, tc.sslcrl, tc.pgbouncerCompatible,
			)

			// Verify real URL contains password
			if tc.password != "" {
				if !strings.Contains(url, "password="+tc.password) {
					t.Errorf("real url should contain actual password")
				}
				if strings.Contains(maskedUrl, tc.password) {
					t.Errorf("masked url should NOT contain actual password")
				}
				if !strings.Contains(maskedUrl, "password=*****") {
					t.Errorf("masked url should contain masked password")
				}
			} else {
				if strings.Contains(url, "password=") {
					t.Errorf("url should not contain password field if empty")
				}
				if strings.Contains(maskedUrl, "password=") {
					t.Errorf("masked url should not contain password field if empty")
				}
			}

			// Verify other fields exist in both
			if !strings.Contains(url, "user="+tc.user) || !strings.Contains(maskedUrl, "user="+tc.user) {
				t.Errorf("both urls should contain user")
			}
		})
	}
}

func TestGetSchemaChanges(t *testing.T) {
	strPtr := func(i int) *int { return &i }

	tests := []struct {
		name            string
		existingColumns map[string]ColumnInfo
		expectError     bool
		expectedSqlContain []string
		unexpectedSqlContain []string
	}{
		{
			name: "all valid",
			existingColumns: map[string]ColumnInfo{
				"dirhash":   {DataType: "bigint"},
				"name":      {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength)},
				"directory": {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength)},
				"meta":      {DataType: "bytea"},
			},
			expectError: false,
			expectedSqlContain: []string{},
		},
		{
			name: "missing column",
			existingColumns: map[string]ColumnInfo{
				"dirhash": {DataType: "bigint"},
			},
			expectError: false,
			expectedSqlContain: []string{
				"ALTER TABLE filemeta ADD COLUMN name VARCHAR(65535)",
				"ALTER TABLE filemeta ADD COLUMN directory VARCHAR(65535)",
				"ALTER TABLE filemeta ADD COLUMN meta bytea",
			},
		},
		{
			name: "type safe widening (integer -> bigint)",
			existingColumns: map[string]ColumnInfo{
				"dirhash":   {DataType: "integer"}, // should be bigint
				"name":      {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength)},
				"directory": {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength)},
				"meta":      {DataType: "bytea"},
			},
			expectError: false,
			expectedSqlContain: []string{
				"ALTER TABLE filemeta ALTER COLUMN dirhash TYPE bigint",
			},
		},
		{
			name: "type unsafe check",
			existingColumns: map[string]ColumnInfo{
				"dirhash":   {DataType: "text"}, // unsafe to convert to bigint blindly
				"name":      {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength)},
				"directory": {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength)},
				"meta":      {DataType: "bytea"},
			},
			expectError: true,
		},
		{
			name: "varchar too short",
			existingColumns: map[string]ColumnInfo{
				"dirhash":   {DataType: "bigint"},
				"name":      {DataType: "character varying", MaxLength: strPtr(255)}, // too short
				"directory": {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength)},
				"meta":      {DataType: "bytea"},
			},
			expectError: false,
			expectedSqlContain: []string{
				fmt.Sprintf("ALTER TABLE filemeta ALTER COLUMN name TYPE VARCHAR(%d)", MaxVarcharLength),
			},
		},
		{
			name: "varchar long enough",
			existingColumns: map[string]ColumnInfo{
				"dirhash":   {DataType: "bigint"},
				"name":      {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength + 10)},
				"directory": {DataType: "character varying", MaxLength: strPtr(MaxVarcharLength)},
				"meta":      {DataType: "bytea"},
			},
			expectError: false,
			expectedSqlContain: []string{}, // No change needed
		},
	}

	store := &PostgresStore{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqls, err := store.getSchemaChanges(tc.existingColumns)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Validate expected SQLs
			for _, expected := range tc.expectedSqlContain {
				found := false
				for _, sql := range sqls {
					if strings.Contains(sql, expected) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected SQL containing '%s' not found in %v", expected, sqls)
				}
			}
			
			// Validate unexpected SQLs (if any logic required, but mostly expected empty for "all valid")
			if len(tc.expectedSqlContain) == 0 && len(sqls) > 0 {
				t.Errorf("expected no SQL changes, but got: %v", sqls)
			}
		})
	}
}

func TestCheckSchemaTransaction(t *testing.T) {
	// 1. Success Case: Commit
	t.Run("Transaction Commit Success", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		store := &PostgresStore{}
		store.DB = db

		// Expect Query for columns
		// Simulate missing 'name' column
		rows := sqlmock.NewRows([]string{"column_name", "data_type", "character_maximum_length"}).
			AddRow("dirhash", "bigint", nil).
			AddRow("directory", "character varying", MaxVarcharLength).
			AddRow("meta", "bytea", nil)
		mock.ExpectQuery("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns").WillReturnRows(rows)

		// Expect Transaction
		mock.ExpectBegin()
		mock.ExpectExec("ALTER TABLE filemeta ADD COLUMN name VARCHAR\\(65535\\)").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		if err := store.checkSchema(); err != nil {
			t.Errorf("checkSchema() error = %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	// 2. Failure Case: Rollback
	t.Run("Transaction Rollback Failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()

		store := &PostgresStore{}
		store.DB = db

		// Expect Query
		// Simulate missing 'name' and 'directory' columns
		rows := sqlmock.NewRows([]string{"column_name", "data_type", "character_maximum_length"}).
			AddRow("dirhash", "bigint", nil).
			AddRow("meta", "bytea", nil)
		mock.ExpectQuery("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns").WillReturnRows(rows)

		// Expect Transaction
		mock.ExpectBegin()
		// First execution succeeds (name)
		mock.ExpectExec("ALTER TABLE filemeta ADD COLUMN name VARCHAR\\(65535\\)").WillReturnResult(sqlmock.NewResult(1, 1))
		// Second execution fails (directory)
		mock.ExpectExec("ALTER TABLE filemeta ADD COLUMN directory VARCHAR\\(65535\\)").WillReturnError(errors.New("db explosion"))
		// Since second failed, we expect Rollback
		mock.ExpectRollback()

		if err := store.checkSchema(); err == nil {
			t.Error("expected error but got nil")
		} else {
			if !strings.Contains(err.Error(), "execute sql") {
				t.Errorf("unexpected error message: %v", err)
			}
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}
