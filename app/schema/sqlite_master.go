package schema

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/cell"
	"github/com/codecrafters-io/sqlite-starter-go/app/parser"
	"github/com/codecrafters-io/sqlite-starter-go/app/utils"

	"github.com/rqlite/sql"
)

type ObjectType string

const (
	ObjectTypeTable   ObjectType = "table"
	ObjectTypeIndex   ObjectType = "index"
	ObjectTypeTrigger ObjectType = "trigger"
	ObjectTypeView    ObjectType = "view"
)

func isObjectType(t string) bool {
	if t != string(ObjectTypeTable) && t != string(ObjectTypeIndex) && t != string(ObjectTypeTrigger) && t != string(ObjectTypeView) {
		return false
	}
	return true
}

type SQLiteMasterRow struct {
	RowID      uint64
	ObjectType ObjectType
	Name       string
	TableName  string
	RootPage   int8
	SQL        string
}

func (r *SQLiteMasterRow) GetColumn(column string) (*sql.ColumnDefinition, error) {
	stmt, err := parser.NewStatement(r.SQL)
	if err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *sql.CreateTableStatement:
		for _, c := range s.Columns {
			if c.Name.String() == column {
				return c, nil
			}
		}
		return nil, fmt.Errorf("column %s not found", column)
	default:
		return nil, fmt.Errorf("GetColumnNames() is not implemented for statement type %T", stmt)
	}
}

func (r *SQLiteMasterRow) GetColumns() ([]*sql.ColumnDefinition, error) {
	stmt, err := parser.NewStatement(r.SQL)
	if err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *sql.CreateTableStatement:
		columns := make([]*sql.ColumnDefinition, 0)
		for i, c := range s.Columns {
			// odd index data of s.Columns are for column types
			if i%2 == 1 {
				continue
			}
			columns = append(columns, c)
		}
		return columns, nil
	default:
		return nil, fmt.Errorf("GetColumnNames() is not implemented for statement type %T", stmt)
	}
}

type SQLiteMasterRows []*SQLiteMasterRow

func (rs SQLiteMasterRows) RootPageMapByTableNames() map[string]int8 {
	m := make(map[string]int8)
	for _, row := range rs {
		m[row.TableName] = row.RootPage
	}
	return m
}

func (rs SQLiteMasterRows) GetTableNames() []string {
	tableNames := make([]string, len(rs))
	for i, r := range rs {
		tableNames[i] = r.TableName
	}
	return tableNames
}

func (rs SQLiteMasterRows) GetColumn(table, column string) (*sql.ColumnDefinition, error) {
	for _, r := range rs {
		if r.TableName == table {
			return r.GetColumn(column)
		}
	}
	return nil, fmt.Errorf(`table "%s" not found`, table)
}

func (rs SQLiteMasterRows) GetColumns(table string) ([]*sql.ColumnDefinition, error) {
	for _, r := range rs {
		if r.TableName == table {
			return r.GetColumns()
		}
	}
	return nil, fmt.Errorf(`table "%s" not found`, table)
}

func (rs SQLiteMasterRows) GetColumnPosList(table string, columns []string) ([]int, error) {
	cs, err := rs.GetColumns(table)
	if err != nil {
		return nil, err
	}

	columnFound := make(map[string]bool)
	posList := make([]int, 0)
	for i, c := range cs {
		if utils.SliceIncludes(columns, c.Name.String()) {
			columnFound[c.Name.String()] = true
			posList = append(posList, i)
		}
	}

	for _, c := range columns {
		if !columnFound[c] {
			return nil, fmt.Errorf("column %s not found", c)
		}
	}

	return posList, nil
}

func newSQLiteMasterRow(c *cell.LeafTablePageCell) (*SQLiteMasterRow, error) {
	objectType, err := c.SerialTypeAndRecords[0].String()
	if err != nil {
		return nil, err
	}

	if !isObjectType(objectType) {
		return nil, fmt.Errorf("invalid object type: %s", objectType)
	}

	name, err := c.SerialTypeAndRecords[1].String()
	if err != nil {
		return nil, err
	}

	tableName, err := c.SerialTypeAndRecords[2].String()
	if err != nil {
		return nil, err
	}

	rootPage, err := c.SerialTypeAndRecords[3].Int8()
	if err != nil {
		return nil, err
	}

	q, err := c.SerialTypeAndRecords[4].String()
	if err != nil {
		return nil, err
	}

	return &SQLiteMasterRow{
		RowID:      c.RowID,
		ObjectType: ObjectType(objectType),
		Name:       name,
		TableName:  tableName,
		RootPage:   rootPage,
		SQL:        q,
	}, nil
}

func NewSQLiteMasterRows(cs cell.LeafTablePageCells) (SQLiteMasterRows, error) {
	rows := make(SQLiteMasterRows, len(cs))
	for i, c := range cs {
		row, err := newSQLiteMasterRow(c)
		if err != nil {
			return nil, err
		}
		rows[i] = row
	}
	return rows, nil
}
