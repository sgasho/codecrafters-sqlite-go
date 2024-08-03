package header

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const (
	FileHeaderSize           = 100
	fileHeaderInitStringSize = 16
	fileHeaderString         = "SQLite format 3\000"
)

type FileHeader struct {
	PageSize uint16
}

func NewFileHeader(f *os.File) (*FileHeader, uint, error) {
	buf := make([]byte, FileHeaderSize)
	if _, err := f.Read(buf); err != nil {
		return nil, 0, err
	}

	headerStrBuf := string(buf[0:fileHeaderInitStringSize])
	if headerStrBuf != fileHeaderString {
		return nil, 0, errors.New("invalid file header")
	}

	var pageSize uint16
	if err := binary.Read(bytes.NewReader(buf[16:18]), binary.BigEndian, &pageSize); err != nil {
		return nil, 0, fmt.Errorf("failed to read integer: %v", err)
	}

	return &FileHeader{pageSize}, FileHeaderSize, nil
}
