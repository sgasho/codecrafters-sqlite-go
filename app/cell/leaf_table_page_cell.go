package cell

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/header"
	"github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"os"
)

type LeafTablePageCell struct {
	RowID                uint64
	SerialTypeAndRecords []*SerialTypeAndRecord
}

type LeafTablePageCells []*LeafTablePageCell

type LeafTablePageCellRequest struct {
	PageType     header.PageType
	PageOffset   uint64
	HeaderOffset uint64
	CellCount    uint64
}

func NewLeafTablePageCells(f *os.File, r *LeafTablePageCellRequest) (LeafTablePageCells, error) {
	cells := make(LeafTablePageCells, r.CellCount)
	for i := uint64(0); i < r.CellCount; i++ {
		cellContentOffset, err := GetCellContentOffset(f, int64(r.PageOffset+r.HeaderOffset+2*i))
		if err != nil {
			return nil, err
		}

		cell, err := GetLeafTablePageCell(f, r.PageType, int64(r.PageOffset+uint64(cellContentOffset)))
		if err != nil {
			return nil, err
		}
		cells[i] = cell
	}
	return cells, nil
}

func GetCellContentOffset(f *os.File, offset int64) (uint16, error) {
	buf := make([]byte, 2)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return 0, err
	}

	var off uint16
	if err := binary.Read(bytes.NewReader(buf), binary.BigEndian, &off); err != nil {
		return 0, err
	}
	return off, nil
}

func GetLeafTablePageCell(f *os.File, t header.PageType, offset int64) (*LeafTablePageCell, error) {
	if t != header.LeafTableBTree {
		return nil, fmt.Errorf("GetLeafTablePageCell() is not implemented for pageType: %v", t)
	}

	readAtOffset := offset

	payloadBytes, read, err := utils.ReadUvarint(f, readAtOffset)
	if err != nil {
		return nil, err
	}
	readAtOffset += int64(read)

	rowID, read, err := utils.ReadUvarint(f, readAtOffset)
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
			buf := make([]byte, sc.ContentSize)
			if _, err := f.ReadAt(buf, readAtOffset); err != nil {
				return nil, err
			}

			srs = append(srs, &SerialTypeAndRecord{
				SerialType: sc.SerialType,
				Record:     buf,
			})

			bodyRemain -= sc.ContentSize
			readAtOffset += int64(sc.ContentSize)
		}
	}

	return &LeafTablePageCell{
		RowID:                rowID,
		SerialTypeAndRecords: srs,
	}, nil
}
