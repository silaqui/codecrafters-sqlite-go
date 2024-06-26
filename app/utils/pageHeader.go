package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func ParsePageHeaderBytes(headerBytes []byte) (PageHeader, error) {
	var header PageHeader

	if err := binary.Read(bytes.NewReader(headerBytes[0:1]), binary.BigEndian, &header.PageType); err != nil {
		return PageHeader{}, err
	}

	if err := binary.Read(bytes.NewReader(headerBytes[1:3]), binary.BigEndian, &header.FirstFreeBlockOnPage); err != nil {
		return PageHeader{}, err
	}

	if err := binary.Read(bytes.NewReader(headerBytes[3:5]), binary.BigEndian, &header.NumberOfCellsOnPage); err != nil {
		return PageHeader{}, err
	}

	if err := binary.Read(bytes.NewReader(headerBytes[5:8]), binary.BigEndian, &header.StartOffsetOfCellContentArea); err != nil {
		return PageHeader{}, err
	}

	if err := binary.Read(bytes.NewReader(headerBytes[8:9]), binary.BigEndian, &header.FragmentedBytes); err != nil {
		return PageHeader{}, err
	}

	return header, nil
}

type PageHeader struct {
	PageType                     uint8 // 0x0D is leaf
	FirstFreeBlockOnPage         uint16
	NumberOfCellsOnPage          uint16
	StartOffsetOfCellContentArea uint16
	FragmentedBytes              uint8
}

func (h PageHeader) String() string {
	return fmt.Sprintf("PageType: %v | FirstFreeBlockOnPage: %v | NumberOfCellsOnPage: %v | StartOffsetOfCellContentArea: %v | FragmentedBytes: %v",
		h.PageType, h.FirstFreeBlockOnPage, h.NumberOfCellsOnPage, h.StartOffsetOfCellContentArea, h.FragmentedBytes)
}
