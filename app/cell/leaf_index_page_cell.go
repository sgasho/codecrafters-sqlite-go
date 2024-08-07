package cell

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/header"
	"github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"os"
)

type NewLeafIndexPageCellRequest struct {
	PageType     header.PageType
	PageOffset   uint64
	HeaderOffset uint64
	CellCount    uint64
	Where        *Where
}

type LeafIndexPageCell struct {
	SerialTypeAndRecords []*SerialTypeAndRecord
}

type LeafIndexPageCells []*LeafIndexPageCell

func NewLeafIndexPageCells(f *os.File, r *NewLeafIndexPageCellRequest) (LeafIndexPageCells, error) {
	cells := make(LeafIndexPageCells, 0)
	for i := uint64(0); i < r.CellCount; i++ {
		cellContentOffset, err := GetCellContentOffset(f, int64(r.PageOffset+r.HeaderOffset+2*i))
		if err != nil {
			return nil, err
		}

		cell, err := GetLeafIndexPageCell(f, &GetLeafIndexPageCellRequest{
			PageType: r.PageType,
			Offset:   int64(r.PageOffset + uint64(cellContentOffset)),
			Where:    r.Where,
		})
		if err != nil {
			return nil, err
		}
		cells = append(cells, cell)
	}
	return cells, nil
}

type GetLeafIndexPageCellRequest struct {
	PageType header.PageType
	Offset   int64
	Where    *Where
}

func GetLeafIndexPageCell(f *os.File, r *GetLeafIndexPageCellRequest) (*LeafIndexPageCell, error) {
	if r.PageType != header.LeafIndexBTree {
		return nil, fmt.Errorf("GetInteriorIndexPageCell() is not implemented for pageType: %v", r.PageType)
	}

	readAtOffset := r.Offset

	payloadBytes, read, err := utils.ReadUvarint(f, readAtOffset)
	if err != nil {
		return nil, err
	}
	readAtOffset += int64(read)

	recordHeaderSize, read, err := utils.ReadUvarint(f, readAtOffset)
	if err != nil {
		return nil, err
	}
	readAtOffset += int64(read)

	scs := make([]*SerialTypeAndContentSize, 0)
	headerRemain := recordHeaderSize - uint64(read) // The varint value is the size of the header in bytes including the size varint itself.
	for headerRemain > 0 {
		serialType, read, err := utils.ReadUvarint(f, readAtOffset)
		if err != nil {
			return nil, err
		}
		scs = append(scs, GetSerialTypeAndContentSize(serialType))
		headerRemain -= uint64(read)
		readAtOffset += int64(read)
	}

	srs := make([]*SerialTypeAndRecord, 0)
	bodyRemain := payloadBytes - recordHeaderSize
	for bodyRemain > 0 {
		for _, sc := range scs {
			off := readAtOffset
			bodyRemain -= sc.ContentSize
			readAtOffset += int64(sc.ContentSize)

			buf := make([]byte, sc.ContentSize)
			if _, err := f.ReadAt(buf, off); err != nil {
				return nil, err
			}

			srs = append(srs, &SerialTypeAndRecord{
				SerialType: sc.SerialType,
				Record:     buf,
			})
		}
	}

	return &LeafIndexPageCell{
		SerialTypeAndRecords: srs,
	}, nil
}
