package engine

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// CockroachSQLParser wraps CockroachDB's PostgreSQL-compatible SQL parser for use in SeaweedFS
type CockroachSQLParser struct{}

// NewCockroachSQLParser creates a new instance of the CockroachDB SQL parser wrapper
func NewCockroachSQLParser() *CockroachSQLParser {
	return &CockroachSQLParser{}
}

// ParseSQL parses a SQL statement using CockroachDB's parser
func (p *CockroachSQLParser) ParseSQL(sql string) (Statement, error) {
	// Parse using CockroachDB's parser
	stmts, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("CockroachDB parser error: %v", err)
	}

	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(stmts))
	}

	stmt := stmts[0].AST

	// Convert CockroachDB AST to SeaweedFS AST format
	switch s := stmt.(type) {
	case *tree.Select:
		return p.convertSelectStatement(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", s)
	}
}

// convertSelectStatement converts CockroachDB's Select AST to SeaweedFS format
func (p *CockroachSQLParser) convertSelectStatement(crdbSelect *tree.Select) (*SelectStatement, error) {
	selectClause, ok := crdbSelect.Select.(*tree.SelectClause)
	if !ok {
		return nil, fmt.Errorf("expected SelectClause, got %T", crdbSelect.Select)
	}

	seaweedSelect := &SelectStatement{
		SelectExprs: make([]SelectExpr, 0, len(selectClause.Exprs)),
		From:        []TableExpr{},
	}

	// Convert SELECT expressions
	for _, expr := range selectClause.Exprs {
		seaweedExpr, err := p.convertSelectExpr(expr)
		if err != nil {
			return nil, fmt.Errorf("failed to convert select expression: %v", err)
		}
		seaweedSelect.SelectExprs = append(seaweedSelect.SelectExprs, seaweedExpr)
	}

	// Convert FROM clause
	if len(selectClause.From.Tables) > 0 {
		for _, fromExpr := range selectClause.From.Tables {
			seaweedTableExpr, err := p.convertFromExpr(fromExpr)
			if err != nil {
				return nil, fmt.Errorf("failed to convert FROM clause: %v", err)
			}
			seaweedSelect.From = append(seaweedSelect.From, seaweedTableExpr)
		}
	}

	// Convert WHERE clause if present
	if selectClause.Where != nil {
		whereExpr, err := p.convertExpr(selectClause.Where.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to convert WHERE clause: %v", err)
		}
		seaweedSelect.Where = &WhereClause{
			Expr: whereExpr,
		}
	}

	// Convert LIMIT and OFFSET clauses if present
	if crdbSelect.Limit != nil {
		limitClause := &LimitClause{}

		// Convert LIMIT (Count)
		if crdbSelect.Limit.Count != nil {
			countExpr, err := p.convertExpr(crdbSelect.Limit.Count)
			if err != nil {
				return nil, fmt.Errorf("failed to convert LIMIT clause: %v", err)
			}
			limitClause.Rowcount = countExpr
		}

		// Convert OFFSET
		if crdbSelect.Limit.Offset != nil {
			offsetExpr, err := p.convertExpr(crdbSelect.Limit.Offset)
			if err != nil {
				return nil, fmt.Errorf("failed to convert OFFSET clause: %v", err)
			}
			limitClause.Offset = offsetExpr
		}

		seaweedSelect.Limit = limitClause
	}

	return seaweedSelect, nil
}

// convertSelectExpr converts CockroachDB SelectExpr to SeaweedFS format
func (p *CockroachSQLParser) convertSelectExpr(expr tree.SelectExpr) (SelectExpr, error) {
	// Handle star expressions (SELECT *)
	if _, isStar := expr.Expr.(tree.UnqualifiedStar); isStar {
		return &StarExpr{}, nil
	}

	// CockroachDB's SelectExpr is a struct, not an interface, so handle it directly
	seaweedExpr := &AliasedExpr{}

	// Convert the main expression
	convertedExpr, err := p.convertExpr(expr.Expr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert expression: %v", err)
	}
	seaweedExpr.Expr = convertedExpr

	// Convert alias if present (temporarily set to nil to avoid interface issues)
	if expr.As != "" {
		// TODO: Fix alias handling - for now set to nil to get compilation working
		seaweedExpr.As = nil
	}

	return seaweedExpr, nil
}

// convertExpr converts CockroachDB expressions to SeaweedFS format
func (p *CockroachSQLParser) convertExpr(expr tree.Expr) (ExprNode, error) {
	switch e := expr.(type) {
	case *tree.FuncExpr:
		// Function call
		seaweedFunc := &FuncExpr{
			Name:  stringValue(strings.ToUpper(e.Func.String())), // Convert to uppercase for consistency
			Exprs: make([]SelectExpr, 0, len(e.Exprs)),
		}

		// Convert function arguments
		for _, arg := range e.Exprs {
			// Special case: Handle star expressions in function calls like COUNT(*)
			if _, isStar := arg.(tree.UnqualifiedStar); isStar {
				seaweedFunc.Exprs = append(seaweedFunc.Exprs, &StarExpr{})
			} else {
				convertedArg, err := p.convertExpr(arg)
				if err != nil {
					return nil, fmt.Errorf("failed to convert function argument: %v", err)
				}
				seaweedFunc.Exprs = append(seaweedFunc.Exprs, &AliasedExpr{Expr: convertedArg})
			}
		}

		return seaweedFunc, nil

	case *tree.BinaryExpr:
		// Arithmetic/binary operations (including string concatenation ||)
		seaweedArith := &ArithmeticExpr{
			Operator: e.Operator.String(),
		}

		// Convert left operand
		left, err := p.convertExpr(e.Left)
		if err != nil {
			return nil, fmt.Errorf("failed to convert left operand: %v", err)
		}
		seaweedArith.Left = left

		// Convert right operand
		right, err := p.convertExpr(e.Right)
		if err != nil {
			return nil, fmt.Errorf("failed to convert right operand: %v", err)
		}
		seaweedArith.Right = right

		return seaweedArith, nil

	case *tree.ComparisonExpr:
		// Comparison operations (=, >, <, >=, <=, !=, etc.) used in WHERE clauses
		seaweedComp := &ComparisonExpr{
			Operator: e.Operator.String(),
		}

		// Convert left operand
		left, err := p.convertExpr(e.Left)
		if err != nil {
			return nil, fmt.Errorf("failed to convert comparison left operand: %v", err)
		}
		seaweedComp.Left = left

		// Convert right operand
		right, err := p.convertExpr(e.Right)
		if err != nil {
			return nil, fmt.Errorf("failed to convert comparison right operand: %v", err)
		}
		seaweedComp.Right = right

		return seaweedComp, nil

	case *tree.StrVal:
		// String literal
		return &SQLVal{
			Type: StrVal,
			Val:  []byte(string(e.RawString())),
		}, nil

	case *tree.NumVal:
		// Numeric literal
		valStr := e.String()
		if strings.Contains(valStr, ".") {
			return &SQLVal{
				Type: FloatVal,
				Val:  []byte(valStr),
			}, nil
		} else {
			return &SQLVal{
				Type: IntVal,
				Val:  []byte(valStr),
			}, nil
		}

	case *tree.UnresolvedName:
		// Column name
		return &ColName{
			Name: stringValue(e.String()),
		}, nil

	case *tree.AndExpr:
		// AND expression
		left, err := p.convertExpr(e.Left)
		if err != nil {
			return nil, fmt.Errorf("failed to convert AND left operand: %v", err)
		}
		right, err := p.convertExpr(e.Right)
		if err != nil {
			return nil, fmt.Errorf("failed to convert AND right operand: %v", err)
		}
		return &AndExpr{
			Left:  left,
			Right: right,
		}, nil

	case *tree.OrExpr:
		// OR expression
		left, err := p.convertExpr(e.Left)
		if err != nil {
			return nil, fmt.Errorf("failed to convert OR left operand: %v", err)
		}
		right, err := p.convertExpr(e.Right)
		if err != nil {
			return nil, fmt.Errorf("failed to convert OR right operand: %v", err)
		}
		return &OrExpr{
			Left:  left,
			Right: right,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", e)
	}
}

// convertFromExpr converts CockroachDB FROM expressions to SeaweedFS format
func (p *CockroachSQLParser) convertFromExpr(expr tree.TableExpr) (TableExpr, error) {
	switch e := expr.(type) {
	case *tree.TableName:
		// Simple table name
		tableName := TableName{
			Name: stringValue(e.Table()),
		}

		// Extract database qualifier if present

		if e.Schema() != "" {
			tableName.Qualifier = stringValue(e.Schema())
		}

		return &AliasedTableExpr{
			Expr: tableName,
		}, nil

	case *tree.AliasedTableExpr:
		// Handle aliased table expressions (which is what CockroachDB uses for qualified names)
		if tableName, ok := e.Expr.(*tree.TableName); ok {
			seaweedTableName := TableName{
				Name: stringValue(tableName.Table()),
			}

			// Extract database qualifier if present
			if tableName.Schema() != "" {
				seaweedTableName.Qualifier = stringValue(tableName.Schema())
			}

			return &AliasedTableExpr{
				Expr: seaweedTableName,
			}, nil
		}

		return nil, fmt.Errorf("unsupported expression in AliasedTableExpr: %T", e.Expr)

	default:
		return nil, fmt.Errorf("unsupported table expression type: %T", e)
	}
}
