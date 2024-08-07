package cell

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/header"
	"github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"os"
)

type NewInteriorIndexPageCellRequest struct {
	PageType     header.PageType
	PageOffset   uint64
	HeaderOffset uint64
	CellCount    uint64
	Where        *Where
}

type InteriorIndexPageCell struct {
	LeftChildPageNum     uint32
	SerialTypeAndRecords []*SerialTypeAndRecord
}

type InteriorIndexPageCells []*InteriorIndexPageCell

func NewInteriorIndexPageCells(f *os.File, r *NewInteriorIndexPageCellRequest) (InteriorIndexPageCells, error) {
	cells := make(InteriorIndexPageCells, 0)
	for i := uint64(0); i < r.CellCount; i++ {
		cellContentOffset, err := GetCellContentOffset(f, int64(r.PageOffset+r.HeaderOffset+2*i))
		if err != nil {
			return nil, err
		}

		cell, err := GetInteriorIndexPageCell(f, &GetInteriorIndexPageCellRequest{
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

type GetInteriorIndexPageCellRequest struct {
	PageType header.PageType
	Offset   int64
	Where    *Where
}

func GetInteriorIndexPageCell(f *os.File, r *GetInteriorIndexPageCellRequest) (*InteriorIndexPageCell, error) {
	if r.PageType != header.InteriorIndexBTree && r.PageType != header.LeafIndexBTree {
		return nil, fmt.Errorf("GetInteriorIndexPageCell() is not implemented for pageType: %v", r.PageType)
	}

	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, r.Offset); err != nil {
		return nil, err
	}

	var leftChildPageNum uint32
	if err := binary.Read(bytes.NewReader(buf), binary.BigEndian, &leftChildPageNum); err != nil {
		return nil, err
	}

	readAtOffset := r.Offset + 4

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

	buf = make([]byte, 4)
	if _, err := f.ReadAt(buf, readAtOffset); err != nil {
		return nil, err
	}

	return &InteriorIndexPageCell{
		LeftChildPageNum:     leftChildPageNum,
		SerialTypeAndRecords: srs,
	}, nil
}
