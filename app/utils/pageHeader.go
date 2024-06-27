package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

type PageHeader struct {
	PageType                     uint8
	FirstFreeBlockOnPage         uint16
	NumberOfCellsOnPage          uint16
	StartOffsetOfCellContentArea uint16
	FragmentedBytes              uint8
	RightMostPointer             uint32
}

func (h PageHeader) String() string {
	return fmt.Sprintf("PageType: %v | FirstFreeBlockOnPage: %v | NumberOfCellsOnPage: %v | StartOffsetOfCellContentArea: %v | FragmentedBytes: %v | RightMostPointer: %v",
		h.PageType, h.FirstFreeBlockOnPage, h.NumberOfCellsOnPage, h.StartOffsetOfCellContentArea, h.FragmentedBytes, h.RightMostPointer)
}

func ParsePageHeaderBytes(headerBytes []byte) PageHeader {
	var header PageHeader
	if err := binary.Read(bytes.NewReader(headerBytes[0:1]), binary.BigEndian, &header.PageType); err != nil {
		log.Fatalf("Failed to parse PageHeader:PageType: %v", err)
	}

	if err := binary.Read(bytes.NewReader(headerBytes[1:3]), binary.BigEndian, &header.FirstFreeBlockOnPage); err != nil {
		log.Fatalf("Failed to parse PageHeader:FirstFreeBlockOnPage: %v", err)
	}

	if err := binary.Read(bytes.NewReader(headerBytes[3:5]), binary.BigEndian, &header.NumberOfCellsOnPage); err != nil {
		log.Fatalf("Failed to parse PageHeader:NumberOfCellsOnPage: %v", err)
	}

	if err := binary.Read(bytes.NewReader(headerBytes[5:7]), binary.BigEndian, &header.StartOffsetOfCellContentArea); err != nil {
		log.Fatalf("Failed to parse PageHeader:StartOffsetOfCellContentArea: %v", err)
	}

	if err := binary.Read(bytes.NewReader(headerBytes[7:8]), binary.BigEndian, &header.FragmentedBytes); err != nil {
		log.Fatalf("Failed to parse PageHeader:FragmentedBytes: %v", err)
	}

	if header.PageType == 0x05 {
		if err := binary.Read(bytes.NewReader(headerBytes[8:12]), binary.BigEndian, &header.RightMostPointer); err != nil {
			log.Fatalf("Failed to parse PageHeader:RightMostPointer: %v", err)
		}
	}

	return header
}

func (h PageHeader) isTableLeaf() bool {
	return h.PageType == 0x0D
}
func (h PageHeader) isTableInterior() bool {
	return h.PageType == 0x05
}
