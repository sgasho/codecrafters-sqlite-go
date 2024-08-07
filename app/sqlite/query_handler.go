package sqlite

import (
	"errors"
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/cell"
	"github/com/codecrafters-io/sqlite-starter-go/app/header"
	"github/com/codecrafters-io/sqlite-starter-go/app/page"
	"github/com/codecrafters-io/sqlite-starter-go/app/parser"
	"strings"
	"sync"

	"github.com/rqlite/sql"
)

var mu sync.Mutex

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

		cells, err := cell.NewLeafTablePageCells(db.f, &cell.NewLeafTablePageCellRequest{
			PageType:           lp.PageType,
			PageOffset:         uint64(lp.Offset),
			HeaderOffset:       uint64(bhSize),
			CellCount:          uint64(lp.BTreeHeader.CellCount),
			ColumnPosList:      nil,
			AutoIncrKeyPosList: nil,
			Where:              nil,
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

	if len(columns) == 0 {
		return nil, errors.New("no columns found")
	}

	where, err := parser.NewWhereClause(ss.WhereExpr)
	if err != nil {
		return nil, err
	}

	wherePos := 0
	if where != nil {
		wherePos, err = db.firstPage.SQLiteMasterRows.GetColumnPos(table, where.Key)
		if err != nil {
			return nil, err
		}
	}

	pageNum, err := db.GetTraverseRootPageNum(table, where)
	if err != nil {
		return nil, err
	}

	pageType, err := page.GetPageType(db.f, db.PageSize(), uint(pageNum))
	if err != nil {
		return nil, err
	}

	traverse := &TraverseBTree{
		PageNum: uint(pageNum),
		Table:   table,
		Columns: columns,
		Where: &cell.Where{
			Clause:    where,
			ColumnPos: wherePos,
		},
	}

	switch pageType {
	case header.LeafTableBTree:
		return db.getLeafTablePageCells(traverse)
	case header.InteriorTableBTree:
		return db.traverseInteriorTableToGetCells(traverse)
	case header.InteriorIndexBTree:
		targetRowIDs, err := db.traverseInteriorIndexesToGetTargetRowIDs(traverse)
		if err != nil {
			return nil, err
		}
		pn, err := db.PageNum(table)
		if err != nil {
			return nil, err
		}
		cells, err := db.traverseInteriorTablesToGetCellsByPK(&TraverseBTreeByPrimaryKey{
			PageNum:     uint(pn),
			Table:       table,
			Columns:     columns,
			PrimaryKeys: targetRowIDs,
		})
		return cells, nil
	default:
		return nil, fmt.Errorf("invalid page type: %v", pageType)
	}
}

type TraverseBTree struct {
	PageNum uint
	Table   string
	Columns []*sql.ResultColumn
	Where   *cell.Where
}

// TODO: handle multiple primary key types
type TraverseBTreeByPrimaryKey struct {
	PageNum     uint
	Table       string
	Columns     []*sql.ResultColumn
	PrimaryKeys []int
}

func (db *sqlite) getLeafTablePageCells(t *TraverseBTree) (cell.LeafTablePageCells, error) {
	lp, err := page.NewLeafTablePage(db.f, db.PageSize(), t.PageNum)
	if err != nil {
		return nil, err
	}

	bhSize, err := lp.BTreeHeader.PageType.GetBTreeHeaderSize()
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, len(t.Columns))
	for i, column := range t.Columns {
		columnNames[i] = column.String()
	}

	columnPosList, err := db.firstPage.SQLiteMasterRows.GetColumnPosList(t.Table, columnNames)
	if err != nil {
		return nil, err
	}

	autoIncrPrimaryKeys, err := db.firstPage.SQLiteMasterRows.AutoIncrIntegerPrimaryKeys(t.Table)
	if err != nil {
		return nil, err
	}

	autoIncrPrimaryKeyPosList, err := db.firstPage.SQLiteMasterRows.GetColumnPosList(t.Table, autoIncrPrimaryKeys)
	if err != nil {
		return nil, err
	}

	return cell.NewLeafTablePageCells(db.f, &cell.NewLeafTablePageCellRequest{
		PageType:           lp.PageType,
		PageOffset:         uint64(lp.Offset),
		HeaderOffset:       uint64(bhSize),
		CellCount:          uint64(lp.BTreeHeader.CellCount),
		ColumnPosList:      columnPosList,
		AutoIncrKeyPosList: autoIncrPrimaryKeyPosList,
		Where:              t.Where,
	})
}

func (db *sqlite) traverseInteriorTableToGetCells(t *TraverseBTree) (cell.LeafTablePageCells, error) {
	b, bhSize, err := header.NewBTreeHeader(db.f, (t.PageNum-1)*db.PageSize())
	if err != nil {
		return nil, err
	}

	if b.PageType == header.LeafTableBTree {
		return db.getLeafTablePageCells(t)
	}

	ip, err := page.NewInteriorTable(db.f, db.PageSize(), t.PageNum)
	if err != nil {
		return nil, err
	}

	cells, err := cell.NewInteriorTablePageCells(db.f, &cell.NewInteriorTablePageCellRequest{
		PageType:     ip.PageType,
		PageOffset:   uint64(ip.Offset),
		HeaderOffset: uint64(bhSize),
		CellCount:    uint64(ip.BTreeHeader.CellCount),
	})
	if err != nil {
		return nil, err
	}

	leafCells := make(cell.LeafTablePageCells, 0)
	for _, c := range cells {
		cs, err := db.traverseInteriorTableToGetCells(&TraverseBTree{
			PageNum: uint(c.LeftChildPageNum),
			Table:   t.Table,
			Columns: t.Columns,
			Where:   t.Where,
		})
		if err != nil {
			return nil, err
		}
		leafCells = append(leafCells, cs...)
	}

	cs, err := db.traverseInteriorTableToGetCells(&TraverseBTree{
		PageNum: ip.RightMostPointer,
		Table:   t.Table,
		Columns: t.Columns,
		Where:   t.Where,
	})
	if err != nil {
		return nil, err
	}
	leafCells = append(leafCells, cs...)

	return leafCells, nil
}

func (db *sqlite) traverseInteriorIndexesToGetTargetRowIDs(t *TraverseBTree) ([]int, error) {
	b, bhSize, err := header.NewBTreeHeader(db.f, (t.PageNum-1)*db.PageSize())
	if err != nil {
		return nil, err
	}

	if b.PageType == header.LeafIndexBTree {
		return db.traverseLeafIndexesToGetTargetPrimaryKeys(t)
	}

	ii, err := page.NewInteriorIndex(db.f, db.PageSize(), t.PageNum)
	if err != nil {
		return nil, err
	}

	cells, err := cell.NewInteriorIndexPageCells(db.f, &cell.NewInteriorIndexPageCellRequest{
		PageType:     ii.PageType,
		PageOffset:   uint64(ii.Offset),
		HeaderOffset: uint64(bhSize),
		CellCount:    uint64(b.CellCount),
		Where:        t.Where,
	})
	if err != nil {
		return nil, err
	}

	// TODO: handle multiple where conditions
	targetRowIDs := make([]int, 0)
	leftChildPageNumsToDeepDive := make([]uint, 0)
	for _, c := range cells {
		rowIDColumn := c.SerialTypeAndRecords[1]
		whereKey := c.SerialTypeAndRecords[0].Record.String()
		// TODO: binary search
		if t.Where.Clause.Value <= whereKey {
			if t.Where.Clause.Value == whereKey {
				rowID, err := rowIDColumn.Int()
				if err != nil {
					return nil, err
				}
				targetRowIDs = append(targetRowIDs, rowID)
			}
			leftChildPageNumsToDeepDive = append(leftChildPageNumsToDeepDive, uint(c.LeftChildPageNum))
		}
	}

	for _, leftChildPageNum := range leftChildPageNumsToDeepDive {
		rowIDs, err := db.traverseInteriorIndexesToGetTargetRowIDs(&TraverseBTree{
			PageNum: leftChildPageNum,
			Table:   t.Table,
			Columns: t.Columns,
			Where:   t.Where,
		})
		if err != nil {
			return nil, err
		}
		targetRowIDs = append(targetRowIDs, rowIDs...)
	}

	if len(leftChildPageNumsToDeepDive) == 0 {
		rowIDs, err := db.traverseInteriorIndexesToGetTargetRowIDs(&TraverseBTree{
			PageNum: ii.RightMostPointer,
			Table:   t.Table,
			Columns: t.Columns,
			Where:   t.Where,
		})
		if err != nil {
			return nil, err
		}
		targetRowIDs = append(targetRowIDs, rowIDs...)
	}

	return targetRowIDs, nil
}

// TODO: not only row ids
func (db *sqlite) traverseLeafIndexesToGetTargetPrimaryKeys(t *TraverseBTree) ([]int, error) {
	b, bhSize, err := header.NewBTreeHeader(db.f, (t.PageNum-1)*db.PageSize())
	if err != nil {
		return nil, err
	}

	li, err := page.NewLeafIndex(db.f, db.PageSize(), t.PageNum)
	if err != nil {
		return nil, err
	}

	cells, err := cell.NewLeafIndexPageCells(db.f, &cell.NewLeafIndexPageCellRequest{
		PageType:     li.PageType,
		PageOffset:   uint64(li.Offset),
		HeaderOffset: uint64(bhSize),
		CellCount:    uint64(b.CellCount),
		Where:        t.Where,
	})
	if err != nil {
		return nil, err
	}

	targetRowIDs := make([]int, 0)
	for _, c := range cells {
		whereClauseColumn := c.SerialTypeAndRecords[0]
		rowIDColumn := c.SerialTypeAndRecords[1]
		if whereClauseColumn.Record.String() == t.Where.Clause.Value {
			rowID, err := rowIDColumn.Int()
			if err != nil {
				return nil, err
			}
			targetRowIDs = append(targetRowIDs, rowID)
		}
	}

	return targetRowIDs, nil
}

// TODO: not only row ids
func (db *sqlite) traverseInteriorTablesToGetCellsByPK(t *TraverseBTreeByPrimaryKey) (cell.LeafTablePageCells, error) {
	b, bhSize, err := header.NewBTreeHeader(db.f, (t.PageNum-1)*db.PageSize())
	if err != nil {
		return nil, err
	}

	if b.PageType == header.LeafTableBTree {
		return db.getLeafTablesToGetCellsByPK(t)
	}

	ii, err := page.NewInteriorIndex(db.f, db.PageSize(), t.PageNum)
	if err != nil {
		return nil, err
	}

	cells, err := cell.NewInteriorTablePageCells(db.f, &cell.NewInteriorTablePageCellRequest{
		PageType:     ii.PageType,
		PageOffset:   uint64(ii.Offset),
		HeaderOffset: uint64(bhSize),
		CellCount:    uint64(b.CellCount),
	})
	if err != nil {
		return nil, err
	}

	leafCells := make(cell.LeafTablePageCells, 0)
	for _, c := range cells {
		primaryKeysToTraverseBy := make([]int, 0)
		for _, pk := range t.PrimaryKeys {
			if pk <= int(c.RowID) {
				if pk <= int(c.RowID) {
					primaryKeysToTraverseBy = append(primaryKeysToTraverseBy, pk)
				}
			}
		}
		if len(primaryKeysToTraverseBy) > 0 {
			cs, err := db.traverseInteriorTablesToGetCellsByPK(&TraverseBTreeByPrimaryKey{
				PageNum:     uint(c.LeftChildPageNum),
				Table:       t.Table,
				Columns:     t.Columns,
				PrimaryKeys: primaryKeysToTraverseBy,
			})
			if err != nil {
				return nil, err
			}
			leafCells = append(leafCells, cs...)
		}
	}

	cs, err := db.traverseInteriorTablesToGetCellsByPK(&TraverseBTreeByPrimaryKey{
		PageNum:     ii.RightMostPointer,
		Table:       t.Table,
		Columns:     t.Columns,
		PrimaryKeys: t.PrimaryKeys,
	})
	if err != nil {
		return nil, err
	}
	leafCells = append(leafCells, cs...)

	return leafCells, nil
}

func (db *sqlite) getLeafTablesToGetCellsByPK(t *TraverseBTreeByPrimaryKey) (cell.LeafTablePageCells, error) {
	lp, err := page.NewLeafTablePage(db.f, db.PageSize(), t.PageNum)
	if err != nil {
		return nil, err
	}

	bhSize, err := lp.BTreeHeader.PageType.GetBTreeHeaderSize()
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, len(t.Columns))
	for i, column := range t.Columns {
		columnNames[i] = column.String()
	}

	columnPosList, err := db.firstPage.SQLiteMasterRows.GetColumnPosList(t.Table, columnNames)
	if err != nil {
		return nil, err
	}

	autoIncrPrimaryKeys, err := db.firstPage.SQLiteMasterRows.AutoIncrIntegerPrimaryKeys(t.Table)
	if err != nil {
		return nil, err
	}

	autoIncrPrimaryKeyPosList, err := db.firstPage.SQLiteMasterRows.GetColumnPosList(t.Table, autoIncrPrimaryKeys)
	if err != nil {
		return nil, err
	}

	return cell.NewLeafTablePageCellsByPK(db.f, &cell.NewLeafTablePageCellsByPKsRequest{
		PageType:           lp.PageType,
		PageOffset:         uint64(lp.Offset),
		HeaderOffset:       uint64(bhSize),
		CellCount:          uint64(lp.BTreeHeader.CellCount),
		ColumnPosList:      columnPosList,
		AutoIncrKeyPosList: autoIncrPrimaryKeyPosList,
		PrimaryKeys:        t.PrimaryKeys,
	})
}
