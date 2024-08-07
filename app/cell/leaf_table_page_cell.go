package cell

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/header"
	"github/com/codecrafters-io/sqlite-starter-go/app/parser"
	"github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"os"
	"strconv"
)

type Where struct {
	Clause    *parser.WhereClause
	ColumnPos int
}

type LeafTablePageCell struct {
	RowID                uint64
	SerialTypeAndRecords []*SerialTypeAndRecord
}

type LeafTablePageCells []*LeafTablePageCell

func (cs LeafTablePageCells) RowsInStrings() ([][]string, error) {
	rows := make([][]string, len(cs))
	for i, c := range cs {
		row := make([]string, 0)
		for _, sr := range c.SerialTypeAndRecords {
			switch sr.SerialType {
			case SerialTypeString, SerialTypeNull:
				str, err := sr.String()
				if err != nil {
					return nil, err
				}
				row = append(row, str)
			case SerialTypeI8:
				i8, err := sr.Int8()
				if err != nil {
					return nil, err
				}
				row = append(row, fmt.Sprintf("%d", i8))
			case SerialTypeAutoIncrPrimaryKey:
				row = append(row, fmt.Sprintf("%d", c.RowID))
			default:
				return nil, fmt.Errorf("print() is not implemented for serial type %v", sr.SerialType)
			}
		}
		rows[i] = row
	}
	return rows, nil
}

type NewLeafTablePageCellRequest struct {
	PageType           header.PageType
	PageOffset         uint64
	HeaderOffset       uint64
	CellCount          uint64
	ColumnPosList      []int
	AutoIncrKeyPosList []int
	Where              *Where
}

type NewLeafTablePageCellsByPKRequest struct {
	PageType           header.PageType
	PageOffset         uint64
	HeaderOffset       uint64
	CellCount          uint64
	ColumnPosList      []int
	AutoIncrKeyPosList []int
	PrimaryKey         int
}

func NewLeafTablePageCells(f *os.File, r *NewLeafTablePageCellRequest) (LeafTablePageCells, error) {
	cells := make(LeafTablePageCells, 0)
	for i := uint64(0); i < r.CellCount; i++ {
		cellContentOffset, err := GetCellContentOffset(f, int64(r.PageOffset+r.HeaderOffset+2*i))
		if err != nil {
			return nil, err
		}

		cell, err := GetLeafTablePageCell(f, &GetLeafTablePageCellRequest{
			PageType:           r.PageType,
			Offset:             int64(r.PageOffset + uint64(cellContentOffset)),
			ColumnPosList:      r.ColumnPosList,
			AutoIncrKeyPosList: r.AutoIncrKeyPosList,
			Where:              r.Where,
		})
		if err != nil {
			return nil, err
		}
		if cell == nil {
			continue
		}
		cells = append(cells, cell)
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

type GetLeafTablePageCellRequest struct {
	PageType           header.PageType
	Offset             int64
	ColumnPosList      []int
	AutoIncrKeyPosList []int
	Where              *Where
}

func GetLeafTablePageCell(f *os.File, r *GetLeafTablePageCellRequest) (*LeafTablePageCell, error) {
	if r.PageType != header.LeafTableBTree {
		return nil, fmt.Errorf("GetLeafTablePageCell() is not implemented for pageType: %v", r.PageType)
	}

	readAtOffset := r.Offset

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
	currentColumnPos := 0
	for headerRemain > 0 {
		serialType, read, err := utils.ReadUvarint(f, readAtOffset)
		if err != nil {
			return nil, err
		}
		// has possibility to be auto increment primary key when null
		if serialType == uint64(SerialTypeNull) {
			if utils.SliceIncludes(r.AutoIncrKeyPosList, currentColumnPos) {
				serialType = uint64(SerialTypeAutoIncrPrimaryKey)
			}
		}
		scs = append(scs, GetSerialTypeAndContentSize(serialType))
		headerRemain -= uint64(read)
		readAtOffset += int64(read)
		currentColumnPos++
	}

	match, err := doesCellMatchCondition(f, scs, readAtOffset, r.Where)
	if err != nil {
		return nil, err
	}
	if !match {
		return nil, nil
	}

	srs := make([]*SerialTypeAndRecord, 0)

	if len(r.ColumnPosList) == 0 {
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
	} else {
		for _, columnPos := range r.ColumnPosList {
			off := readAtOffset
			for i, sc := range scs {
				if i != columnPos {
					off += int64(sc.ContentSize)
					continue
				}

				buf := make([]byte, sc.ContentSize)
				if _, err := f.ReadAt(buf, off); err != nil {
					return nil, err
				}

				srs = append(srs, &SerialTypeAndRecord{
					SerialType: sc.SerialType,
					Record:     buf,
				})
				off += int64(sc.ContentSize)
			}
		}
	}

	return &LeafTablePageCell{
		RowID:                rowID,
		SerialTypeAndRecords: srs,
	}, nil
}

func doesCellMatchCondition(f *os.File, scs []*SerialTypeAndContentSize, currentOffset int64, where *Where) (bool, error) {
	if where == nil || where.Clause == nil {
		return true, nil
	}

	wherePosOffset := currentOffset
	for i, sc := range scs {
		if i == where.ColumnPos {
			buf := make([]byte, sc.ContentSize)
			if _, err := f.ReadAt(buf, wherePosOffset); err != nil {
				return false, err
			}

			sr := &SerialTypeAndRecord{
				SerialType: sc.SerialType,
				Record:     buf,
			}

			switch sc.SerialType {
			case SerialTypeString, SerialTypeNull:
				str, err := sr.String()
				if err != nil {
					return false, err
				}
				if str != where.Clause.Value {
					return false, nil
				}
			case SerialTypeI8:
				i8, err := sr.Int8()
				if err != nil {
					return false, err
				}
				if strconv.Itoa(int(i8)) != where.Clause.Value {
					return false, nil
				}
			default:
				return false, fmt.Errorf("where-check is not implemented for serial type %v", sc.SerialType)
			}
		}
		wherePosOffset += int64(sc.ContentSize)
	}
	return true, nil
}

func NewLeafTablePageCellsByPK(f *os.File, r *NewLeafTablePageCellsByPKRequest) (LeafTablePageCells, error) {
	cells := make(LeafTablePageCells, 0)
	for i := uint64(0); i < r.CellCount; i++ {
		cellContentOffset, err := GetCellContentOffset(f, int64(r.PageOffset+r.HeaderOffset+2*i))
		if err != nil {
			return nil, err
		}

		cell, err := GetLeafTablePageCellByPK(f, &GetLeafTablePageCellByPKRequest{
			PageType:           r.PageType,
			Offset:             int64(r.PageOffset + uint64(cellContentOffset)),
			ColumnPosList:      r.ColumnPosList,
			AutoIncrKeyPosList: r.AutoIncrKeyPosList,
			PrimaryKey:         r.PrimaryKey,
		})
		if err != nil {
			return nil, err
		}
		if cell == nil {
			continue
		}
		cells = append(cells, cell)
	}
	return cells, nil
}

type GetLeafTablePageCellByPKRequest struct {
	PageType           header.PageType
	Offset             int64
	ColumnPosList      []int
	AutoIncrKeyPosList []int
	PrimaryKey         int
}

func GetLeafTablePageCellByPK(f *os.File, r *GetLeafTablePageCellByPKRequest) (*LeafTablePageCell, error) {
	if r.PageType != header.LeafTableBTree {
		return nil, fmt.Errorf("GetLeafTablePageCell() is not implemented for pageType: %v", r.PageType)
	}

	readAtOffset := r.Offset

	payloadBytes, read, err := utils.ReadUvarint(f, readAtOffset)
	if err != nil {
		return nil, err
	}
	readAtOffset += int64(read)

	rowID, read, err := utils.ReadUvarint(f, readAtOffset)
	if err != nil {
		return nil, err
	}
	if r.PrimaryKey != int(rowID) {
		return nil, nil
	}

	readAtOffset += int64(read)

	recordHeaderSize, read, err := utils.ReadUvarint(f, readAtOffset)
	if err != nil {
		return nil, err
	}
	readAtOffset += int64(read)

	scs := make([]*SerialTypeAndContentSize, 0)
	headerRemain := recordHeaderSize - uint64(read) // The varint value is the size of the header in bytes including the size varint itself.
	currentColumnPos := 0
	for headerRemain > 0 {
		serialType, read, err := utils.ReadUvarint(f, readAtOffset)
		if err != nil {
			return nil, err
		}
		// has possibility to be auto increment primary key when null
		if serialType == uint64(SerialTypeNull) {
			if utils.SliceIncludes(r.AutoIncrKeyPosList, currentColumnPos) {
				serialType = uint64(SerialTypeAutoIncrPrimaryKey)
			}
		}
		scs = append(scs, GetSerialTypeAndContentSize(serialType))
		headerRemain -= uint64(read)
		readAtOffset += int64(read)
		currentColumnPos++
	}

	srs := make([]*SerialTypeAndRecord, 0)
	if len(r.ColumnPosList) == 0 {
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
	} else {
		for _, columnPos := range r.ColumnPosList {
			off := readAtOffset
			for i, sc := range scs {
				if i != columnPos {
					off += int64(sc.ContentSize)
					continue
				}

				buf := make([]byte, sc.ContentSize)
				if _, err := f.ReadAt(buf, off); err != nil {
					return nil, err
				}

				srs = append(srs, &SerialTypeAndRecord{
					SerialType: sc.SerialType,
					Record:     buf,
				})
				off += int64(sc.ContentSize)
			}
		}
	}

	return &LeafTablePageCell{
		RowID:                rowID,
		SerialTypeAndRecords: srs,
	}, nil
}
