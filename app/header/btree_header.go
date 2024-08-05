package header

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type PageType uint

const (
	bTreeLeafPageHeaderSize     = 8
	bTreeInteriorPageHeaderSize = 12

	InteriorIndexBTree PageType = 2
	InteriorTableBTree PageType = 5
	LeafIndexBTree     PageType = 10
	LeafTableBTree     PageType = 13
)

type BTreeHeader struct {
	PageType                PageType
	CellCount               uint16
	CellContentAreaStartsAt uint16
	RightMostPointer        uint
}

func (t PageType) GetBTreeHeaderSize() (uint, error) {
	switch t {
	case InteriorIndexBTree, InteriorTableBTree:
		return bTreeInteriorPageHeaderSize, nil
	case LeafIndexBTree, LeafTableBTree:
		return bTreeLeafPageHeaderSize, nil
	default:
		return 0, fmt.Errorf("invalid page type: %v", t)
	}
}

func NewBTreeHeader(f *os.File, offset uint) (*BTreeHeader, uint, error) {
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, int64(offset)); err != nil {
		return nil, 0, err
	}

	var pageTypeNum uint8
	if err := binary.Read(bytes.NewReader(buf), binary.BigEndian, &pageTypeNum); err != nil {
		return nil, 0, err
	}
	pageType := PageType(pageTypeNum)

	bTreeHeaderSize, err := pageType.GetBTreeHeaderSize()
	if err != nil {
		return nil, 0, err
	}

	buf = make([]byte, bTreeHeaderSize)
	if _, err := f.ReadAt(buf, int64(offset)); err != nil {
		return nil, 0, err
	}

	var cellCount uint16
	if err := binary.Read(bytes.NewReader(buf[3:5]), binary.BigEndian, &cellCount); err != nil {
		return nil, 0, err
	}

	var cellContentAreaStartsAt uint16
	if err := binary.Read(bytes.NewReader(buf[5:7]), binary.BigEndian, &cellContentAreaStartsAt); err != nil {
		return nil, 0, err
	}

	var rightMostPointer uint32
	if pageType == InteriorTableBTree {
		if err := binary.Read(bytes.NewReader(buf[8:12]), binary.BigEndian, &rightMostPointer); err != nil {
			return nil, 0, err
		}
	}

	return &BTreeHeader{
		PageType:                pageType,
		CellCount:               cellCount,
		CellContentAreaStartsAt: cellContentAreaStartsAt,
		RightMostPointer:        uint(rightMostPointer),
	}, bTreeHeaderSize, nil
}
