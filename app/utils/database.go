package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type Database struct {
	Header       *FileHeader
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
	database.Header = &header

	database.MasterTable = database.parsMasterTable()

	fmt.Printf("File Header: %v\n", database.Header)
	return &database
}

func (d Database) readPage(pageNumber int) []byte {
	if pageNumber > d.Header.PageSize || pageNumber < 1 {
		log.Fatal(fmt.Printf("Invalid page numbe: %v max page: %v", pageNumber, d.Header.NumberOfPages))
	}

	var page = make([]byte, d.Header.PageSize)
	var offset = d.Header.PageSize * (pageNumber - 1)
	_, err := d.databaseFile.ReadAt(page, int64(offset))
	if err != nil {
		log.Fatal(fmt.Printf("Error reading page"))
	}
	return page
}

func (d Database) parsMasterTable() []MasterEntry {
	var out []MasterEntry

	page := d.readPage(1)
	pageHeader := ParsePageHeaderBytes(page[100:108])
	cellPointers := GetCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 108)

	for _, pointer := range cellPointers {
		cell := parseCell(pointer, page)
		out = append(out, MasterEntryFromCell(cell))
	}

	return out
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

	page := d.readPage(pageNumber)
	pageHeader := ParsePageHeaderBytes(page[pageHeaderOffset:])

	if pageHeader.isTableLeaf() {
		cells := getPageLeafCells(page, pageHeaderOffset)
		for i, c := range cells {
			fmt.Printf("TableLeafCell %v: %v \n", i+1, c.PrettyValues())
		}
	} else if pageHeader.isTableInterior() {
		fmt.Printf("isTableInterior\n")
	} else {
		fmt.Printf("UNKNOWN PAGE TYPE %v\n%v\n", pageHeader.PageType, page[:50])
	}

}

func getPageLeafCells(page []byte, pageHeaderOffset int) []TableLeafCell {
	pageHeader := ParsePageHeaderBytes(page[pageHeaderOffset : pageHeaderOffset+8])
	fmt.Println(pageHeader)
	cellPointers := GetCellPointersArray(pageHeader.NumberOfCellsOnPage, page, pageHeaderOffset+8)
	fmt.Printf("Cell pointers array: %v\n", cellPointers)

	var cells []TableLeafCell
	for _, p := range cellPointers {
		cell := parseCell(p, page)
		cells = append(cells, cell)
	}

	return cells
}

func (d Database) GetTableEntries(tableName string) []TableLeafCell {
	var out []TableLeafCell
	rootPage := d.getRootTableFor(tableName)

	page := d.readPage(rootPage)
	pageHeader := ParsePageHeaderBytes(page[0:8])

	if pageHeader.isTableLeaf() {

		cellPointers := GetCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 8)
		fmt.Printf("Cell pointers array: %v", cellPointers)
		for _, pointer := range cellPointers {
			cell := parseCell(pointer, page)
			out = append(out, cell)
		}

	} else if pageHeader.isTableInterior() {
		cellPointers := GetCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 12)

		fmt.Printf("Cell pointers: %v", cellPointers)
		for i, p := range cellPointers {

			fmt.Printf("Cell %v  left child pointer: %v", i, page[int(p):int(p)+4])

		}

	} else {
		log.Fatalf("Unknown page type: %v", pageHeader.PageType)
	}

	return out
}

func (d Database) getRootTableFor(tableName string) int {
	rootPage := -1
	for _, e := range d.MasterTable {
		if e.TableName == tableName {
			rootPage = e.RootPage
			break
		}
	}
	if rootPage == -1 {
		log.Fatalf("No table: %v", tableName)
	}
	return rootPage
}
