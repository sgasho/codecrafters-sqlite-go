package utils

import "os"

const (
	maxVarIntSize = 9
)

// Uvarint decodes Big-endian bytes to uint64
func Uvarint(buf []byte) (uint64, int) {
	var result int64
	var bytesRead int

	for bytesRead < len(buf) {
		b := buf[bytesRead : bytesRead+1][0]

		bytesRead++

		result = (result << 7) | int64(b&0x7F)

		if b&0x80 == 0 {
			break
		}
	}

	return uint64(result), bytesRead
}

func ReadUvarint(f *os.File, offset int64) (uint64, int, error) {
	buf := make([]byte, maxVarIntSize)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return 0, 0, err
	}

	uv, read := Uvarint(buf)
	return uv, read, nil
}
