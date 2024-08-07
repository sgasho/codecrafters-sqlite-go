package cell

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

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
	SerialTypeAutoIncrPrimaryKey = SerialTypeReserved1
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

func (r Record) Int16() (int16, error) {
	var i16 int16
	if err := binary.Read(bytes.NewReader(r), binary.BigEndian, &i16); err != nil {
		return 0, err
	}
	return i16, nil
}

func (r Record) Int32() (int32, error) {
	var i32 int32
	if err := binary.Read(bytes.NewReader(r), binary.BigEndian, &i32); err != nil {
		return 0, err
	}
	return i32, nil
}

type SerialTypeAndRecord struct {
	SerialType SerialType
	Record     Record
}

func (sr *SerialTypeAndRecord) String() (string, error) {
	switch sr.SerialType {
	case SerialTypeString:
		return string(sr.Record), nil
	case SerialTypeNull:
		return "<null>", nil
	default:
		return "", fmt.Errorf("SerialTypeAndRecord.String() is not implemented for SerialType: %v", sr.SerialType)
	}
}

func (sr *SerialTypeAndRecord) Int8() (int8, error) {
	if sr.SerialType != SerialTypeI8 {
		return 0, fmt.Errorf("SerialTypeAndRecord.Int8() is not implemented for SerialType: %v", sr.SerialType)
	}
	return sr.Record.Int8()
}

func (sr *SerialTypeAndRecord) Int16() (int16, error) {
	if sr.SerialType != SerialTypeI16 {
		return 0, fmt.Errorf("SerialTypeAndRecord.Int16() is not implemented for SerialType: %v", sr.SerialType)
	}
	return sr.Record.Int16()
}

func (sr *SerialTypeAndRecord) Int32() (int32, error) {
	if sr.SerialType != SerialTypeI32 && sr.SerialType != SerialTypeI24 {
		return 0, fmt.Errorf("SerialTypeAndRecord.Int32() is not implemented for SerialType: %v", sr.SerialType)
	}
	// prepend 0 in order to avoid EOF error when reading binaries and to convert to int32
	if sr.SerialType == SerialTypeI24 {
		sr.Record = append([]byte{0}, sr.Record...)
	}
	return sr.Record.Int32()
}

func (sr *SerialTypeAndRecord) Int() (int, error) {
	switch sr.SerialType {
	case SerialTypeI8:
		log.Println("Int(): sr.Int8()")
		i8, err := sr.Int8()
		return int(i8), err
	case SerialTypeI16:
		i16, err := sr.Int16()
		return int(i16), err
	case SerialTypeI24, SerialTypeI32:
		i, err := sr.Int32()
		return int(i), err
	default:
		return 0, fmt.Errorf("SerialTypeAndRecord.Int() is not implemented for SerialType: %v", sr.SerialType)
	}
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
