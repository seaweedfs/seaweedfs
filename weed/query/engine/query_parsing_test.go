package engine

import (
	"testing"
)

func TestParseSQL_COUNT_Functions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(t *testing.T, stmt Statement)
	}{
		{
			name:    "COUNT(*) basic",
			sql:     "SELECT COUNT(*) FROM test_table",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt, ok := stmt.(*SelectStatement)
				if !ok {
					t.Fatalf("Expected *SelectStatement, got %T", stmt)
				}

				if len(selectStmt.SelectExprs) != 1 {
					t.Fatalf("Expected 1 select expression, got %d", len(selectStmt.SelectExprs))
				}

				aliasedExpr, ok := selectStmt.SelectExprs[0].(*AliasedExpr)
				if !ok {
					t.Fatalf("Expected *AliasedExpr, got %T", selectStmt.SelectExprs[0])
				}

				funcExpr, ok := aliasedExpr.Expr.(*FuncExpr)
				if !ok {
					t.Fatalf("Expected *FuncExpr, got %T", aliasedExpr.Expr)
				}

				if funcExpr.Name.String() != "COUNT" {
					t.Errorf("Expected function name 'COUNT', got '%s'", funcExpr.Name.String())
				}

				if len(funcExpr.Exprs) != 1 {
					t.Fatalf("Expected 1 function argument, got %d", len(funcExpr.Exprs))
				}

				starExpr, ok := funcExpr.Exprs[0].(*StarExpr)
				if !ok {
					t.Errorf("Expected *StarExpr argument, got %T", funcExpr.Exprs[0])
				}
				_ = starExpr // Use the variable to avoid unused variable error
			},
		},
		{
			name:    "COUNT(column_name)",
			sql:     "SELECT COUNT(user_id) FROM users",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt, ok := stmt.(*SelectStatement)
				if !ok {
					t.Fatalf("Expected *SelectStatement, got %T", stmt)
				}

				aliasedExpr := selectStmt.SelectExprs[0].(*AliasedExpr)
				funcExpr := aliasedExpr.Expr.(*FuncExpr)

				if funcExpr.Name.String() != "COUNT" {
					t.Errorf("Expected function name 'COUNT', got '%s'", funcExpr.Name.String())
				}

				if len(funcExpr.Exprs) != 1 {
					t.Fatalf("Expected 1 function argument, got %d", len(funcExpr.Exprs))
				}

				argExpr, ok := funcExpr.Exprs[0].(*AliasedExpr)
				if !ok {
					t.Errorf("Expected *AliasedExpr argument, got %T", funcExpr.Exprs[0])
				}

				colName, ok := argExpr.Expr.(*ColName)
				if !ok {
					t.Errorf("Expected *ColName, got %T", argExpr.Expr)
				}

				if colName.Name.String() != "user_id" {
					t.Errorf("Expected column name 'user_id', got '%s'", colName.Name.String())
				}
			},
		},
		{
			name:    "Multiple aggregate functions",
			sql:     "SELECT COUNT(*), SUM(amount), AVG(score) FROM transactions",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt, ok := stmt.(*SelectStatement)
				if !ok {
					t.Fatalf("Expected *SelectStatement, got %T", stmt)
				}

				if len(selectStmt.SelectExprs) != 3 {
					t.Fatalf("Expected 3 select expressions, got %d", len(selectStmt.SelectExprs))
				}

				// Verify COUNT(*)
				countExpr := selectStmt.SelectExprs[0].(*AliasedExpr)
				countFunc := countExpr.Expr.(*FuncExpr)
				if countFunc.Name.String() != "COUNT" {
					t.Errorf("Expected first function to be COUNT, got %s", countFunc.Name.String())
				}

				// Verify SUM(amount)
				sumExpr := selectStmt.SelectExprs[1].(*AliasedExpr)
				sumFunc := sumExpr.Expr.(*FuncExpr)
				if sumFunc.Name.String() != "SUM" {
					t.Errorf("Expected second function to be SUM, got %s", sumFunc.Name.String())
				}

				// Verify AVG(score)
				avgExpr := selectStmt.SelectExprs[2].(*AliasedExpr)
				avgFunc := avgExpr.Expr.(*FuncExpr)
				if avgFunc.Name.String() != "AVG" {
					t.Errorf("Expected third function to be AVG, got %s", avgFunc.Name.String())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.validate != nil {
				tt.validate(t, stmt)
			}
		})
	}
}

func TestParseSQL_SELECT_Expressions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(t *testing.T, stmt Statement)
	}{
		{
			name:    "SELECT * FROM table",
			sql:     "SELECT * FROM users",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if len(selectStmt.SelectExprs) != 1 {
					t.Fatalf("Expected 1 select expression, got %d", len(selectStmt.SelectExprs))
				}

				_, ok := selectStmt.SelectExprs[0].(*StarExpr)
				if !ok {
					t.Errorf("Expected *StarExpr, got %T", selectStmt.SelectExprs[0])
				}
			},
		},
		{
			name:    "SELECT column FROM table",
			sql:     "SELECT user_id FROM users",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if len(selectStmt.SelectExprs) != 1 {
					t.Fatalf("Expected 1 select expression, got %d", len(selectStmt.SelectExprs))
				}

				aliasedExpr, ok := selectStmt.SelectExprs[0].(*AliasedExpr)
				if !ok {
					t.Fatalf("Expected *AliasedExpr, got %T", selectStmt.SelectExprs[0])
				}

				colName, ok := aliasedExpr.Expr.(*ColName)
				if !ok {
					t.Fatalf("Expected *ColName, got %T", aliasedExpr.Expr)
				}

				if colName.Name.String() != "user_id" {
					t.Errorf("Expected column name 'user_id', got '%s'", colName.Name.String())
				}
			},
		},
		{
			name:    "SELECT multiple columns",
			sql:     "SELECT user_id, name, email FROM users",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if len(selectStmt.SelectExprs) != 3 {
					t.Fatalf("Expected 3 select expressions, got %d", len(selectStmt.SelectExprs))
				}

				expectedColumns := []string{"user_id", "name", "email"}
				for i, expected := range expectedColumns {
					aliasedExpr := selectStmt.SelectExprs[i].(*AliasedExpr)
					colName := aliasedExpr.Expr.(*ColName)
					if colName.Name.String() != expected {
						t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, colName.Name.String())
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.validate != nil {
				tt.validate(t, stmt)
			}
		})
	}
}

func TestParseSQL_WHERE_Clauses(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(t *testing.T, stmt Statement)
	}{
		{
			name:    "WHERE with simple comparison",
			sql:     "SELECT * FROM users WHERE age > 18",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if selectStmt.Where == nil {
					t.Fatal("Expected WHERE clause, got nil")
				}

				// Just verify we have a WHERE clause with an expression
				if selectStmt.Where.Expr == nil {
					t.Error("Expected WHERE expression, got nil")
				}
			},
		},
		{
			name:    "WHERE with AND condition",
			sql:     "SELECT * FROM users WHERE age > 18 AND status = 'active'",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if selectStmt.Where == nil {
					t.Fatal("Expected WHERE clause, got nil")
				}

				// Verify we have an AND expression
				andExpr, ok := selectStmt.Where.Expr.(*AndExpr)
				if !ok {
					t.Errorf("Expected *AndExpr, got %T", selectStmt.Where.Expr)
				}
				_ = andExpr // Use variable to avoid unused error
			},
		},
		{
			name:    "WHERE with OR condition",
			sql:     "SELECT * FROM users WHERE age < 18 OR age > 65",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if selectStmt.Where == nil {
					t.Fatal("Expected WHERE clause, got nil")
				}

				// Verify we have an OR expression
				orExpr, ok := selectStmt.Where.Expr.(*OrExpr)
				if !ok {
					t.Errorf("Expected *OrExpr, got %T", selectStmt.Where.Expr)
				}
				_ = orExpr // Use variable to avoid unused error
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.validate != nil {
				tt.validate(t, stmt)
			}
		})
	}
}

func TestParseSQL_LIMIT_Clauses(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(t *testing.T, stmt Statement)
	}{
		{
			name:    "LIMIT with number",
			sql:     "SELECT * FROM users LIMIT 10",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if selectStmt.Limit == nil {
					t.Fatal("Expected LIMIT clause, got nil")
				}

				if selectStmt.Limit.Rowcount == nil {
					t.Error("Expected LIMIT rowcount, got nil")
				}

				// Verify no OFFSET is set
				if selectStmt.Limit.Offset != nil {
					t.Error("Expected OFFSET to be nil for LIMIT-only query")
				}

				sqlVal, ok := selectStmt.Limit.Rowcount.(*SQLVal)
				if !ok {
					t.Errorf("Expected *SQLVal, got %T", selectStmt.Limit.Rowcount)
				}

				if sqlVal.Type != IntVal {
					t.Errorf("Expected IntVal type, got %d", sqlVal.Type)
				}

				if string(sqlVal.Val) != "10" {
					t.Errorf("Expected limit value '10', got '%s'", string(sqlVal.Val))
				}
			},
		},
		{
			name:    "LIMIT with OFFSET",
			sql:     "SELECT * FROM users LIMIT 10 OFFSET 5",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if selectStmt.Limit == nil {
					t.Fatal("Expected LIMIT clause, got nil")
				}

				// Verify LIMIT value
				if selectStmt.Limit.Rowcount == nil {
					t.Error("Expected LIMIT rowcount, got nil")
				}

				limitVal, ok := selectStmt.Limit.Rowcount.(*SQLVal)
				if !ok {
					t.Errorf("Expected *SQLVal for LIMIT, got %T", selectStmt.Limit.Rowcount)
				}

				if limitVal.Type != IntVal {
					t.Errorf("Expected IntVal type for LIMIT, got %d", limitVal.Type)
				}

				if string(limitVal.Val) != "10" {
					t.Errorf("Expected limit value '10', got '%s'", string(limitVal.Val))
				}

				// Verify OFFSET value
				if selectStmt.Limit.Offset == nil {
					t.Fatal("Expected OFFSET clause, got nil")
				}

				offsetVal, ok := selectStmt.Limit.Offset.(*SQLVal)
				if !ok {
					t.Errorf("Expected *SQLVal for OFFSET, got %T", selectStmt.Limit.Offset)
				}

				if offsetVal.Type != IntVal {
					t.Errorf("Expected IntVal type for OFFSET, got %d", offsetVal.Type)
				}

				if string(offsetVal.Val) != "5" {
					t.Errorf("Expected offset value '5', got '%s'", string(offsetVal.Val))
				}
			},
		},
		{
			name:    "LIMIT with OFFSET zero",
			sql:     "SELECT * FROM users LIMIT 5 OFFSET 0",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if selectStmt.Limit == nil {
					t.Fatal("Expected LIMIT clause, got nil")
				}

				// Verify OFFSET is 0
				if selectStmt.Limit.Offset == nil {
					t.Fatal("Expected OFFSET clause, got nil")
				}

				offsetVal, ok := selectStmt.Limit.Offset.(*SQLVal)
				if !ok {
					t.Errorf("Expected *SQLVal for OFFSET, got %T", selectStmt.Limit.Offset)
				}

				if string(offsetVal.Val) != "0" {
					t.Errorf("Expected offset value '0', got '%s'", string(offsetVal.Val))
				}
			},
		},
		{
			name:    "LIMIT with large OFFSET",
			sql:     "SELECT * FROM users LIMIT 100 OFFSET 1000",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				selectStmt := stmt.(*SelectStatement)
				if selectStmt.Limit == nil {
					t.Fatal("Expected LIMIT clause, got nil")
				}

				// Verify large OFFSET value
				offsetVal, ok := selectStmt.Limit.Offset.(*SQLVal)
				if !ok {
					t.Errorf("Expected *SQLVal for OFFSET, got %T", selectStmt.Limit.Offset)
				}

				if string(offsetVal.Val) != "1000" {
					t.Errorf("Expected offset value '1000', got '%s'", string(offsetVal.Val))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.validate != nil {
				tt.validate(t, stmt)
			}
		})
	}
}

func TestParseSQL_SHOW_Statements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(t *testing.T, stmt Statement)
	}{
		{
			name:    "SHOW DATABASES",
			sql:     "SHOW DATABASES",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				showStmt, ok := stmt.(*ShowStatement)
				if !ok {
					t.Fatalf("Expected *ShowStatement, got %T", stmt)
				}

				if showStmt.Type != "databases" {
					t.Errorf("Expected type 'databases', got '%s'", showStmt.Type)
				}
			},
		},
		{
			name:    "SHOW TABLES",
			sql:     "SHOW TABLES",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				showStmt, ok := stmt.(*ShowStatement)
				if !ok {
					t.Fatalf("Expected *ShowStatement, got %T", stmt)
				}

				if showStmt.Type != "tables" {
					t.Errorf("Expected type 'tables', got '%s'", showStmt.Type)
				}
			},
		},
		{
			name:    "SHOW TABLES FROM database",
			sql:     "SHOW TABLES FROM \"test_db\"",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement) {
				showStmt, ok := stmt.(*ShowStatement)
				if !ok {
					t.Fatalf("Expected *ShowStatement, got %T", stmt)
				}

				if showStmt.Type != "tables" {
					t.Errorf("Expected type 'tables', got '%s'", showStmt.Type)
				}

				if showStmt.Schema != "test_db" {
					t.Errorf("Expected schema 'test_db', got '%s'", showStmt.Schema)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.validate != nil {
				tt.validate(t, stmt)
			}
		})
	}
}
