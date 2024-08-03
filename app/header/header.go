package header

func GetHeadersSize(t PageType) (uint16, error) {
	bTreeHeaderSize, err := t.GetBTreeHeaderSize()
	if err != nil {
		return 0, err
	}
	return uint16(FileHeaderSize + bTreeHeaderSize), nil
}
