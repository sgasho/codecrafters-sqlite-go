package page

import (
	"errors"
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/cell"
	"github/com/codecrafters-io/sqlite-starter-go/app/header"
	"github/com/codecrafters-io/sqlite-starter-go/app/schema"
	"os"
)

type FirstPage struct {
	*header.FileHeader
	*header.BTreeHeader
	SQLiteMasterRows schema.SQLiteMasterRows
}

type LeafPage struct {
	Offset uint
	*header.BTreeHeader
}

func NewDBFirstPage(f *os.File) (*FirstPage, error) {
	fh, read, err := header.NewFileHeader(f)
	if err != nil {
		return nil, err
	}

	bh, read, err := header.NewBTreeHeader(f, read)
	if err != nil {
		return nil, err
	}

	bhSize, err := bh.PageType.GetBTreeHeaderSize()
	if err != nil {
		return nil, err
	}

	cells, err := cell.NewLeafTablePageCells(f, &cell.LeafTablePageCellRequest{
		PageType:     bh.PageType,
		PageOffset:   0,
		HeaderOffset: uint64(header.FileHeaderSize + bhSize),
		CellCount:    uint64(bh.CellCount),
	})
	if err != nil {
		return nil, err
	}

	masterRows, err := schema.NewSQLiteMasterRows(cells)
	if err != nil {
		return nil, err
	}

	return &FirstPage{
		FileHeader:       fh,
		BTreeHeader:      bh,
		SQLiteMasterRows: masterRows,
	}, nil
}

func NewLeafTablePage(f *os.File, pageSize, pageNum uint) (*LeafPage, error) {
	if pageNum <= 0 {
		return nil, fmt.Errorf("invalid pageNum: %d, should be greater than 1", pageNum)
	}

	if pageNum == 1 {
		return nil, errors.New("call NewDBFirstPage when pageNum == 1")
	}

	bh, _, err := header.NewBTreeHeader(f, (pageNum-1)*pageSize)
	if err != nil {
		return nil, err
	}

	return &LeafPage{
		Offset:      (pageNum - 1) * pageSize,
		BTreeHeader: bh,
	}, nil
}
