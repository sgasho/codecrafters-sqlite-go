package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const (
	headerSize                  = 100
	bTreeLeafPageHeaderSize     = 8
	bTreeInteriorPageHeaderSize = 12
	maxVarIntSize               = 9
)

type DB struct {
	*Header
	*BTreeHeader
	SQLiteMasterRows SQLiteMasterRows
}

func NewDB(f *os.File) (*DB, error) {
	header, read, err := NewHeader(f)
	if err != nil {
		return nil, err
	}

	bTreeHeader, read, err := NewBTreeHeader(f, read)
	if err != nil {
		return nil, err
	}

	cells, err := NewLeafTablePageCells(f, bTreeHeader.PageType, bTreeHeader.CellCount)
	if err != nil {
		return nil, err
	}

	masterRows, err := cells.ToSQLiteMasterRows()
	if err != nil {
		return nil, err
	}

	return &DB{
		Header:           header,
		BTreeHeader:      bTreeHeader,
		SQLiteMasterRows: masterRows,
	}, nil
}

type Header struct {
	PageSize uint16
}

func NewHeader(f *os.File) (*Header, uint, error) {
	buf := make([]byte, headerSize)
	if _, err := f.Read(buf); err != nil {
		return nil, 0, err
	}

	if len(buf) != headerSize {
		return nil, 0, fmt.Errorf("invalid header size: %d, should be %d", len(buf), headerSize)
	}

	var pageSize uint16
	if err := binary.Read(bytes.NewReader(buf[16:18]), binary.BigEndian, &pageSize); err != nil {
		return nil, 0, fmt.Errorf("failed to read integer: %v", err)
	}

	return &Header{pageSize}, headerSize, nil
}

type PageType uint

const (
	InteriorIndexBTree PageType = 2
	InteriorTableBTree PageType = 5
	LeafIndexBTree     PageType = 10
	LeafTableBTree     PageType = 13
)

func (t PageType) GetBTreeHeaderSize() (uint, error) {
	switch t {
	case InteriorIndexBTree, InteriorTableBTree:
		return bTreeInteriorPageHeaderSize, nil
	case LeafIndexBTree, LeafTableBTree:
		return bTreeLeafPageHeaderSize, nil
	default:
		return 0, fmt.Errorf("invalid page type: %s", t)
	}
}

func getHeadersSize(t PageType) (uint16, error) {
	bTreeHeaderSize, err := t.GetBTreeHeaderSize()
	if err != nil {
		return 0, err
	}
	return uint16(headerSize + bTreeHeaderSize), nil
}

type BTreeHeader struct {
	PageType                PageType
	CellCount               uint16
	CellContentAreaStartsAt uint16
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

	return &BTreeHeader{
		PageType:                pageType,
		CellCount:               cellCount,
		CellContentAreaStartsAt: cellContentAreaStartsAt,
	}, bTreeHeaderSize, nil
}

type LeafTablePageCell struct {
	RowID                uint64
	SerialTypeAndRecords []*SerialTypeAndRecord
}

type LeafTablePageCells []*LeafTablePageCell

func (cs LeafTablePageCells) ToSQLiteMasterRows() (SQLiteMasterRows, error) {
	rows := make(SQLiteMasterRows, len(cs))
	for i, c := range cs {
		row, err := c.ToSQLiteMasterRow()
		if err != nil {
			return nil, err
		}
		rows[i] = row
	}
	return rows, nil
}

type ObjectType string

const (
	ObjectTypeTable   ObjectType = "table"
	ObjectTypeIndex   ObjectType = "index"
	ObjectTypeTrigger ObjectType = "trigger"
	ObjectTypeView    ObjectType = "view"
)

func isObjectType(t string) bool {
	if t != string(ObjectTypeTable) && t != string(ObjectTypeIndex) && t != string(ObjectTypeTrigger) && t != string(ObjectTypeView) {
		return false
	}
	return true
}

type SQLiteMasterRow struct {
	RowID      uint64
	ObjectType ObjectType
	Name       string
	TableName  string
	RootPage   int8
	SQL        string
}

func (c *LeafTablePageCell) ToSQLiteMasterRow() (*SQLiteMasterRow, error) {
	objectType, err := c.SerialTypeAndRecords[0].String()
	if err != nil {
		return nil, err
	}

	if !isObjectType(objectType) {
		return nil, fmt.Errorf("invalid object type: %s", objectType)
	}

	name, err := c.SerialTypeAndRecords[1].String()
	if err != nil {
		return nil, err
	}

	tableName, err := c.SerialTypeAndRecords[2].String()
	if err != nil {
		return nil, err
	}

	rootPage, err := c.SerialTypeAndRecords[3].Int8()
	if err != nil {
		return nil, err
	}

	sql, err := c.SerialTypeAndRecords[4].String()
	if err != nil {
		return nil, err
	}

	return &SQLiteMasterRow{
		RowID:      c.RowID,
		ObjectType: ObjectType(objectType),
		Name:       name,
		TableName:  tableName,
		RootPage:   rootPage,
		SQL:        sql,
	}, nil
}

type SQLiteMasterRows []*SQLiteMasterRow

func (rs SQLiteMasterRows) GetTableNames() []string {
	tableNames := make([]string, len(rs))
	for i, r := range rs {
		tableNames[i] = r.TableName
	}
	return tableNames
}

func NewLeafTablePageCells(f *os.File, pageType PageType, cellCount uint16) (LeafTablePageCells, error) {
	headersSize, err := getHeadersSize(pageType)
	if err != nil {
		return nil, err
	}

	cells := make(LeafTablePageCells, cellCount)
	for i := uint16(0); i < cellCount; i++ {
		cellContentOffset, err := getCellContentOffset(f, int64(headersSize+2*i))
		if err != nil {
			return nil, err
		}

		cell, err := getLeafTablePageCell(f, pageType, int64(cellContentOffset))
		if err != nil {
			return nil, err
		}
		cells[i] = cell
	}
	return cells, nil
}

func getCellContentOffset(f *os.File, offset int64) (uint16, error) {
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

func getLeafTablePageCell(f *os.File, t PageType, offset int64) (*LeafTablePageCell, error) {
	if t != LeafTableBTree {
		return nil, fmt.Errorf("getLeafTablePageCell() is not implemented for pageType: %v", t)
	}

	readAtOffset := offset
	buf := make([]byte, maxVarIntSize)
	if _, err := f.ReadAt(buf, readAtOffset); err != nil {
		return nil, err
	}

	payloadBytes, read1 := Uvarint(buf)
	readAtOffset += int64(read1)

	buf = make([]byte, maxVarIntSize)
	if _, err := f.ReadAt(buf, readAtOffset); err != nil {
		return nil, err
	}

	rowID, read2 := Uvarint(buf)
	readAtOffset += int64(read2)

	buf = make([]byte, maxVarIntSize)
	if _, err := f.ReadAt(buf, readAtOffset); err != nil {
		return nil, err
	}

	recordHeaderSize, read3 := Uvarint(buf)
	readAtOffset += int64(read3)

	scs := make([]*SerialTypeAndContentSize, 0)
	headerRemain := recordHeaderSize - uint64(read3) // The varint value is the size of the header in bytes including the size varint itself.
	for headerRemain > 0 {
		buf = make([]byte, maxVarIntSize)
		if _, err := f.ReadAt(buf, readAtOffset); err != nil {
			return nil, err
		}

		serialType, read := Uvarint(buf)
		scs = append(scs, GetSerialTypeAndContentSize(serialType))
		headerRemain -= uint64(read)
		readAtOffset += int64(read)
	}

	srs := make([]*SerialTypeAndRecord, 0)
	bodyRemain := payloadBytes - recordHeaderSize
	for bodyRemain > 0 {
		for _, sc := range scs {
			buf = make([]byte, sc.ContentSize)
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

type SerialType int

const (
	SerialTypeNull SerialType = iota
	SerialTypeI8
	SerialTypeI16
	SerialTypeI24
	SerialTypeI32
	SerialTypeI48
	SerialTypeI64
	SerialTypeF64
	SerialTypeI0
	SerialTypeI1
	SerialTypeReserved1
	SerialTypeReserved2
	SerialTypeBLOB
	SerialTypeString
)

type SerialTypeAndContentSize struct {
	SerialType  SerialType
	ContentSize uint64
}

type Record []byte

func (r Record) String() string {
	return string(r)
}

func (r Record) Int8() (int8, error) {
	var i8 int8
	if err := binary.Read(bytes.NewReader(r), binary.BigEndian, &i8); err != nil {
		return 0, err
	}
	return i8, nil
}

type SerialTypeAndRecord struct {
	SerialType SerialType
	Record     Record
}

func (sr SerialTypeAndRecord) String() (string, error) {
	switch sr.SerialType {
	case SerialTypeString:
		return string(sr.Record), nil
	default:
		return "", fmt.Errorf("SerialTypeAndRecord.String() is not implemented for SerialType: %v", sr.SerialType)
	}
}

func (sr SerialTypeAndRecord) Int8() (int8, error) {
	if sr.SerialType != SerialTypeI8 {
		return 0, fmt.Errorf("SerialTypeAndRecord.Int8() is not implemented for SerialType: %v", sr.SerialType)
	}
	return sr.Record.Int8()
}

func GetSerialTypeAndContentSize(num uint64) *SerialTypeAndContentSize {
	var sc SerialTypeAndContentSize

	switch num {
	case 0:
		sc.SerialType = SerialTypeNull
		sc.ContentSize = 0
	case 1:
		sc.SerialType = SerialTypeI8
		sc.ContentSize = 1
	case 2:
		sc.SerialType = SerialTypeI16
		sc.ContentSize = 2
	case 3:
		sc.SerialType = SerialTypeI24
		sc.ContentSize = 3
	case 4:
		sc.SerialType = SerialTypeI32
		sc.ContentSize = 4
	case 5:
		sc.SerialType = SerialTypeI48
		sc.ContentSize = 6
	case 6:
		sc.SerialType = SerialTypeI64
		sc.ContentSize = 8
	case 7:
		sc.SerialType = SerialTypeF64
		sc.ContentSize = 8
	case 8:
		sc.SerialType = SerialTypeI0
		sc.ContentSize = 0
	case 9:
		sc.SerialType = SerialTypeI1
		sc.ContentSize = 0
	case 10:
		sc.SerialType = SerialTypeReserved1
		sc.ContentSize = 0
	case 11:
		sc.SerialType = SerialTypeReserved2
		sc.ContentSize = 0
	default:
		if num >= 12 && num%2 == 0 {
			sc.SerialType = SerialTypeBLOB
			sc.ContentSize = (num - 12) / 2
		} else if num >= 13 && num%2 == 1 {
			sc.SerialType = SerialTypeString
			sc.ContentSize = (num - 13) / 2
		}
	}

	return &sc
}
