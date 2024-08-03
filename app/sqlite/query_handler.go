package sqlite

import (
	"errors"
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/cell"
	"github/com/codecrafters-io/sqlite-starter-go/app/page"
	"github/com/codecrafters-io/sqlite-starter-go/app/parser"
	"strings"
)

type SQLite interface {
	Count(query string, args ...any) (int, error)
	Select(query string, args ...any) (cell.LeafTablePageCells, error)
}

var _ SQLite = (*sqlite)(nil)

func (db *sqlite) Count(q string, args ...any) (int, error) {
	stmt, err := parser.NewStatement(q)
	if err != nil {
		return 0, err
	}

	ss, err := parser.NewSelectStatement(stmt)
	if err != nil {
		return 0, err
	}

	table := strings.ReplaceAll(ss.Source.String(), `"`, "")
	where := ss.WhereExpr

	isCountStmt, err := parser.IsCountStatement(q, stmt)
	if err != nil {
		return 0, err
	}

	if !isCountStmt {
		return 0, fmt.Errorf("invalid count statement: %s", q)
	}

	// simply count rows
	if where == nil {
		pageNum, err := db.PageNum(table)
		if err != nil {
			return 0, err
		}

		lp, err := page.NewLeafTablePage(db.f, db.PageSize(), uint(pageNum))
		if err != nil {
			return 0, err
		}

		bhSize, err := lp.BTreeHeader.PageType.GetBTreeHeaderSize()
		if err != nil {
			return 0, err
		}

		cells, err := cell.NewLeafTablePageCells(db.f, &cell.LeafTablePageCellRequest{
			PageType:      lp.PageType,
			PageOffset:    uint64(lp.Offset),
			HeaderOffset:  uint64(bhSize),
			CellCount:     uint64(lp.BTreeHeader.CellCount),
			ColumnPosList: nil,
		})
		if err != nil {
			return 0, err
		}
		return len(cells), nil
	}

	return 0, nil
}

func (db *sqlite) Select(q string, args ...any) (cell.LeafTablePageCells, error) {
	stmt, err := parser.NewStatement(q)
	if err != nil {
		return nil, err
	}

	ss, err := parser.NewSelectStatement(stmt)
	if err != nil {
		return nil, err
	}

	table := strings.ReplaceAll(ss.Source.String(), `"`, "")
	columns := ss.Columns
	where := ss.WhereExpr

	// TODO: handle multiple columns
	if len(columns) > 1 {
		return nil, errors.New("multiple columns found")
	}

	if len(columns) == 0 {
		return nil, errors.New("no columns found")
	}

	column := columns[0].String()

	if where == nil {
		pageNum, err := db.PageNum(table)
		if err != nil {
			return nil, err
		}

		lp, err := page.NewLeafTablePage(db.f, db.PageSize(), uint(pageNum))
		if err != nil {
			return nil, err
		}

		bhSize, err := lp.BTreeHeader.PageType.GetBTreeHeaderSize()
		if err != nil {
			return nil, err
		}

		columnPos, err := db.firstPage.SQLiteMasterRows.GetColumnPos(table, column)
		if err != nil {
			return nil, err
		}

		return cell.NewLeafTablePageCells(db.f, &cell.LeafTablePageCellRequest{
			PageType:      lp.PageType,
			PageOffset:    uint64(lp.Offset),
			HeaderOffset:  uint64(bhSize),
			CellCount:     uint64(lp.BTreeHeader.CellCount),
			ColumnPosList: []int{columnPos},
		})
	}

	return nil, nil
}
