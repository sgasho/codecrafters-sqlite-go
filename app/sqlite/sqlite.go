package sqlite

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/page"
	"github/com/codecrafters-io/sqlite-starter-go/app/parser"
	"github/com/codecrafters-io/sqlite-starter-go/app/schema"
	"github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"os"
)

type sqlite struct {
	f          *os.File
	pageSize   uint
	tablePages map[string]int
	indexPages map[string]*schema.IndexPageAndColumns
	firstPage  *page.FirstPage
}

type DB interface {
	PageSize() uint
	PageNum(table string) (int, error)
	TableCount() uint16
	Tables() []string
	SQLite
}

func NewDB(f *os.File) (DB, error) {
	fp, err := page.NewDBFirstPage(f)
	if err != nil {
		return nil, err
	}

	ipc, err := fp.SQLiteMasterRows.RootIndexPageAndColumnMapByTableNames()
	if err != nil {
		return nil, err
	}
	return &sqlite{
		f:          f,
		pageSize:   uint(fp.PageSize),
		tablePages: fp.SQLiteMasterRows.RootTablePageMapByTableNames(),
		indexPages: ipc,
		firstPage:  fp,
	}, nil
}

func (db *sqlite) PageSize() uint {
	return db.pageSize
}

func (db *sqlite) PageNum(table string) (int, error) {
	p, ok := db.tablePages[table]
	if !ok {
		return 0, fmt.Errorf("table %s not found", table)
	}
	return p, nil
}

func (db *sqlite) GetTraverseRootPageNum(table string, where *parser.WhereClause) (int, error) {
	ipc, ok := db.indexPages[table]
	if !ok {
		return db.PageNum(table)
	}
	if utils.SliceIncludes(ipc.Columns, fmt.Sprintf(`"%s"`, where.Key)) {
		return ipc.PageNum, nil
	}
	return db.PageNum(table)
}

func (db *sqlite) TableCount() uint16 {
	return db.firstPage.CellCount
}

func (db *sqlite) Tables() []string {
	return db.firstPage.SQLiteMasterRows.GetTableNames()
}
