package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type Database struct {
	Header       FileHeader
	databaseFile *os.File
	MasterTable  []MasterEntry
}

func NewDatabase(databaseFilePath string) *Database {
	var database Database

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	database.databaseFile = databaseFile

	headerBytes := make([]byte, 100)
	_, err = databaseFile.Read(headerBytes)
	if err != nil {
		log.Fatal(err)
	}

	header, err := ParseFileHeaderBytes(headerBytes)
	if err != nil {
		log.Fatal(fmt.Printf("Error parsing header: %v\n", err))
	}
	database.Header = header

	database.MasterTable = database.parsMasterTable()

	return &database
}

func (d Database) ReadPage(pageNumber int) []byte {
	if pageNumber > d.Header.PageSize || pageNumber < 1 {
		log.Fatal(fmt.Printf("Invalid page numbe: %v max page: %v", pageNumber, d.Header.NumberOfPages))
	}

	pageSize := d.Header.PageSize
	var page = make([]byte, pageSize)
	var offset = pageSize * (pageNumber - 1)
	_, err := d.databaseFile.ReadAt(page, int64(offset))
	if err != nil {
		log.Fatal(fmt.Printf("Error reading page"))
	}
	return page
}

func (d Database) parsMasterTable() []MasterEntry {
	var out []MasterEntry

	page, _ := d.readPage(1)
	pageHeader, _ := ParsePageHeaderBytes(page[100:108])
	cellPointers := GetCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 108)

	for _, pointer := range cellPointers {
		cell := ParseCell(pointer, page)
		out = append(out, MasterEntryFromCell(cell))
	}

	return out
}

func (d Database) readPage(pageNumber int) ([]byte, error) {
	var page = make([]byte, d.Header.PageSize)
	var offset = d.Header.PageSize * (pageNumber - 1)
	_, err := d.databaseFile.ReadAt(page, int64(offset))
	if err != nil {
		return []byte{}, err
	}
	return page, nil
}

func GetCellPointersArray(numberOfCellsOnPage uint16, page []byte, pageHeaderOffset int) []uint16 {
	var cellPointers []uint16
	for i := 0; i < int(numberOfCellsOnPage); i++ {
		var startByte = pageHeaderOffset + 2*i
		var endByte = startByte + 2
		var cellPointer uint16 = 0
		if err := binary.Read(bytes.NewReader(page[startByte:endByte]), binary.BigEndian, &cellPointer); err != nil {
			log.Fatal(err)
		}
		cellPointers = append(cellPointers, cellPointer)
	}
	return cellPointers
}

func (d Database) ReadAndPrintPage(pageNumber int) {
	pageHeaderOffset := 0
	if pageNumber == 1 {
		pageHeaderOffset = 100
	}

	page, _ := d.readPage(pageNumber)

	pageHeader, _ := ParsePageHeaderBytes(page[pageHeaderOffset : pageHeaderOffset+8])
	cellPointers := GetCellPointersArray(pageHeader.NumberOfCellsOnPage, page, pageHeaderOffset+8)

	for i := 0; i < len(cellPointers); i++ {
		cell := ParseCell(cellPointers[i], page)
		fmt.Printf("Cell %v: %v \n", i+1, cell.PrettyValues())
	}
}
