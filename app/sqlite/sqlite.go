package sqlite

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/page"
	"os"
)

type sqlite struct {
	f          *os.File
	pageSize   uint
	tablePages map[string]int8
	firstPage  *page.FirstPage
}

type DB interface {
	PageSize() uint
	PageNum(table string) (int8, error)
	TableCount() uint16
	Tables() []string
	SQLite
}

func NewDB(f *os.File) (DB, error) {
	fp, err := page.NewDBFirstPage(f)
	if err != nil {
		return nil, err
	}

	return &sqlite{
		f:          f,
		pageSize:   uint(fp.PageSize),
		tablePages: fp.SQLiteMasterRows.RootPageMapByTableNames(),
		firstPage:  fp,
	}, nil
}

func (db *sqlite) PageSize() uint {
	return db.pageSize
}

func (db *sqlite) PageNum(table string) (int8, error) {
	p, ok := db.tablePages[table]
	if !ok {
		return 0, fmt.Errorf("table %s not found", table)
	}
	return p, nil
}

func (db *sqlite) TableCount() uint16 {
	return db.firstPage.CellCount
}

func (db *sqlite) Tables() []string {
	return db.firstPage.SQLiteMasterRows.GetTableNames()
}
