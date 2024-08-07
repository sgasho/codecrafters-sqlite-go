package cell

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/header"
	"github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"os"
)

type NewInteriorTablePageCellRequest struct {
	PageType     header.PageType
	PageOffset   uint64
	HeaderOffset uint64
	CellCount    uint64
}

type InteriorTablePageCell struct {
	RowID            uint64
	LeftChildPageNum uint32
}

type InteriorTablePageCells []*InteriorTablePageCell

func NewInteriorTablePageCells(f *os.File, r *NewInteriorTablePageCellRequest) (InteriorTablePageCells, error) {
	cells := make(InteriorTablePageCells, 0)
	for i := uint64(0); i < r.CellCount; i++ {
		cellContentOffset, err := GetCellContentOffset(f, int64(r.PageOffset+r.HeaderOffset+2*i))
		if err != nil {
			return nil, err
		}

		cell, err := GetInteriorTablePageCell(f, &GetInteriorTablePageCellRequest{
			PageType: r.PageType,
			Offset:   int64(r.PageOffset + uint64(cellContentOffset)),
		})
		if err != nil {
			return nil, err
		}
		cells = append(cells, cell)
	}
	return cells, nil
}

type GetInteriorTablePageCellRequest struct {
	PageType header.PageType
	Offset   int64
}

func GetInteriorTablePageCell(f *os.File, r *GetInteriorTablePageCellRequest) (*InteriorTablePageCell, error) {
	if r.PageType != header.InteriorTableBTree && r.PageType != header.InteriorIndexBTree {
		return nil, fmt.Errorf("GetInteriorTablePageCell() is not implemented for pageType: %v", r.PageType)
	}

	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, r.Offset); err != nil {
		return nil, err
	}

	var leftChildPageNum uint32
	if err := binary.Read(bytes.NewReader(buf), binary.BigEndian, &leftChildPageNum); err != nil {
		return nil, err
	}

	rowID, _, err := utils.ReadUvarint(f, r.Offset+4)
	if err != nil {
		return nil, err
	}

	return &InteriorTablePageCell{
		RowID:            rowID,
		LeftChildPageNum: leftChildPageNum,
	}, nil
}
