package schema

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/cell"
	"github/com/codecrafters-io/sqlite-starter-go/app/parser"
	"strings"

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
	RootPage   int
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

func (r *SQLiteMasterRow) AutoIncrIntegerPrimaryKeys() ([]string, error) {
	stmt, err := parser.NewStatement(r.SQL)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0)
	switch s := stmt.(type) {
	case *sql.CreateTableStatement:
		for i, c := range s.Columns {
			if i%2 == 1 {
				continue
			}

			typeInfo := s.Columns[i+1]
			if typeInfo.Name.String() == `"integer"` {
				for _, constraint := range typeInfo.Constraints {
					if constraint.String() == "PRIMARY KEY AUTOINCREMENT" {
						keys = append(keys, c.Name.String())
					}
				}
			}
		}
		return keys, nil
	default:
		return nil, fmt.Errorf("HasAutoIncrIntegerPrimaryKey() is not implemented for statement type %T", stmt)
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

func (rs SQLiteMasterRows) AutoIncrIntegerPrimaryKeys(table string) ([]string, error) {
	for _, r := range rs {
		if r.TableName == table {
			return r.AutoIncrIntegerPrimaryKeys()
		}
	}
	return nil, fmt.Errorf(`table "%s" not found`, table)
}

func (rs SQLiteMasterRows) RootTablePageMapByTableNames() map[string]int {
	m := make(map[string]int)
	for _, row := range rs {
		if strings.Contains(row.SQL, "CREATE TABLE") {
			m[row.TableName] = row.RootPage
		}
	}
	return m
}

type IndexPageAndColumns struct {
	PageNum int
	Columns []string
}

func (rs SQLiteMasterRows) RootIndexPageAndColumnMapByTableNames() (map[string]*IndexPageAndColumns, error) {
	m := make(map[string]*IndexPageAndColumns)
	for _, row := range rs {
		if strings.Contains(row.SQL, "CREATE INDEX") {
			stmt, err := parser.NewStatement(row.SQL)
			if err != nil {
				return nil, err
			}
			switch s := stmt.(type) {
			case *sql.CreateIndexStatement:
				columns := s.Columns
				columnNames := make([]string, 0)
				for _, column := range columns {
					columnNames = append(columnNames, column.String())
				}
				m[row.TableName] = &IndexPageAndColumns{
					PageNum: row.RootPage,
					Columns: columnNames,
				}
			default:
				return nil, fmt.Errorf("RootIndexPageAndColumnMapByTableNames() is not implemented for statement type %T", stmt)
			}
		}
	}
	return m, nil
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

func (rs SQLiteMasterRows) ColumnPosMapByName(table string) (map[string]int, error) {
	for _, r := range rs {
		if r.TableName == table {
			columns, err := r.GetColumns()
			if err != nil {
				return nil, err
			}
			m := make(map[string]int)
			for i, c := range columns {
				m[c.Name.String()] = i
			}
			return m, nil
		}
	}
	return nil, fmt.Errorf(`table "%s" not found`, table)
}

func (rs SQLiteMasterRows) GetColumnPos(table, column string) (int, error) {
	cs, err := rs.GetColumns(table)
	if err != nil {
		return 0, err
	}

	for i, c := range cs {
		if strings.ReplaceAll(c.Name.String(), `"`, "") == column {
			return i, nil
		}
	}
	return 0, fmt.Errorf(`column "%s" not found`, column)
}

func (rs SQLiteMasterRows) GetColumnPosList(table string, columns []string) ([]int, error) {
	columnToPos, err := rs.ColumnPosMapByName(table)
	if err != nil {
		return nil, err
	}

	posList := make([]int, 0)
	for _, c := range columns {
		pos, exists := columnToPos[c]
		if !exists {
			return nil, fmt.Errorf(`column "%s" not found`, c)
		}
		posList = append(posList, pos)
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

	fmt.Println("get root page")
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
		RootPage:   int(rootPage),
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
