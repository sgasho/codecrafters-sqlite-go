package parser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rqlite/sql"
)

func NewStatement(q string) (sql.Statement, error) {
	return sql.NewParser(strings.NewReader(q)).ParseStatement()
}

func NewSelectStatement(stmt sql.Statement) (*sql.SelectStatement, error) {
	switch stmt.(type) {
	case *sql.SelectStatement:
		return stmt.(*sql.SelectStatement), nil
	default:
		return nil, fmt.Errorf("invalid select statement type: %T", stmt)
	}
}

func IsCountStatement(q string, stmt sql.Statement) (bool, error) {
	ss, err := NewSelectStatement(stmt)
	if err != nil {
		return false, err
	}

	r, err := regexp.Compile(`(?i)SELECT COUNT\(\*\)`)
	if err != nil {
		return false, err
	}

	return len(ss.Columns) == 1 && r.MatchString(q), nil
}

// WhereClause only considers WHERE key = val so far
type WhereClause struct {
	Key   string
	Value string
}

func NewWhereClause(expr sql.Expr) (*WhereClause, error) {
	if expr == nil {
		return nil, nil
	}

	whereClause := strings.SplitN(expr.String(), "=", 2)

	if len(whereClause) != 2 {
		return nil, fmt.Errorf("invalid WHERE clause expression: %s", expr.String())
	}

	return &WhereClause{
		Key:   strings.Trim(whereClause[0], "\" "),
		Value: strings.Trim(whereClause[1], "' "),
	}, nil
}
