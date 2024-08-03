package schema

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/cell"
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

	sql, err := c.SerialTypeAndRecords[4].String()
	if err != nil {
		return nil, err
	}

	return &SQLiteMasterRow{
		RowID:      c.RowID,
		ObjectType: ObjectType(objectType),
		Name:       name,
		TableName:  tableName,
		RootPage:   rootPage,
		SQL:        sql,
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
