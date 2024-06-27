package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type FileHeader struct {
	MagicString     string
	PageSize        int
	NumberOfPages   uint32
	FileFormatWrite byte
	FileFormatRead  byte
	Encoding        uint32
}

func ParseFileHeaderBytes(headerBytes []byte) (FileHeader, error) {
	var header FileHeader

	header.MagicString = string(headerBytes[0:16])

	var pageSize uint16
	if err := binary.Read(bytes.NewReader(headerBytes[16:18]), binary.BigEndian, &pageSize); err != nil {
		return FileHeader{}, err
	}
	header.PageSize = int(pageSize)

	if err := binary.Read(bytes.NewReader(headerBytes[18:19]), binary.BigEndian, &header.FileFormatWrite); err != nil {
		return FileHeader{}, err
	}

	if err := binary.Read(bytes.NewReader(headerBytes[19:20]), binary.BigEndian, &header.FileFormatRead); err != nil {
		return FileHeader{}, err
	}

	if err := binary.Read(bytes.NewReader(headerBytes[28:32]), binary.BigEndian, &header.NumberOfPages); err != nil {
		return FileHeader{}, err
	}

	if err := binary.Read(bytes.NewReader(headerBytes[58:62]), binary.BigEndian, &header.Encoding); err != nil {
		return FileHeader{}, err
	}

	return header, nil
}

func (h FileHeader) String() string {
	return fmt.Sprintf("%v|Page size: %v|Number of pages: %v|Write: %v|Read: %v|Encoding: %v",
		h.MagicString, h.PageSize, h.NumberOfPages, h.FileFormatWrite, h.FileFormatRead, h.Encoding)
}
