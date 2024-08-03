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

	r, err := regexp.Compile(`SELECT COUNT\(\*\)`)
	if err != nil {
		return false, err
	}

	return len(ss.Columns) == 1 && r.MatchString(q), nil
}
