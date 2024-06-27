package utils

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"strconv"
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
		log.Fatalf("Error parsing header: %v\n", err)
	}
	database.Header = &header

	database.MasterTable = database.parsMasterTable()

	log.Printf("File Header: %v\n", database.Header)
	return &database
}

func (d Database) readPage(pageNumber int) []byte {
	if pageNumber > d.Header.PageSize || pageNumber < 1 {
		log.Fatalf("Invalid page numbe: %v max page: %v", pageNumber, d.Header.NumberOfPages)
	}

	var page = make([]byte, d.Header.PageSize)
	var offset = d.Header.PageSize * (pageNumber - 1)
	_, err := d.databaseFile.ReadAt(page, int64(offset))
	if err != nil {
		log.Fatal("Error reading page")
	}
	return page
}

func (d Database) parsMasterTable() []MasterEntry {
	var out []MasterEntry

	page := d.readPage(1)
	pageHeader := ParsePageHeaderBytes(page[100:108])
	cellPointers := getCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 108)

	for _, pointer := range cellPointers {
		cell := parseLeafCell(pointer, page)
		out = append(out, MasterEntryFromCell(cell))
	}

	return out
}

func getCellPointersArray(numberOfCellsOnPage uint16, page []byte, pageHeaderOffset int) []uint16 {
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
			log.Printf("TableLeafCell %v: %v \n", i+1, c.PrettyValues())
		}
	} else if pageHeader.isTableInterior() {
		cells := getPageInteriorCells(page, pageHeaderOffset)
		for i, c := range cells {
			log.Printf("TableInteriorCell %v: %v \n", i+1, c.PrettyValues())
		}
	} else {
		log.Printf("UNKNOWN PAGE TYPE %v\n%v\n", pageHeader.PageType, page[:50])
	}

}

func getPageLeafCells(page []byte, pageHeaderOffset int) []TableLeafCell {
	pageHeader := ParsePageHeaderBytes(page[pageHeaderOffset : pageHeaderOffset+8])
	log.Println(pageHeader)
	cellPointers := getCellPointersArray(pageHeader.NumberOfCellsOnPage, page, pageHeaderOffset+8)
	log.Printf("Cell pointers array: %v\n", cellPointers)

	var cells []TableLeafCell
	for _, p := range cellPointers {
		cell := parseLeafCell(p, page)
		cells = append(cells, cell)
	}

	return cells
}

func getPageInteriorCells(page []byte, pageHeaderOffset int) []TableInteriorCell {
	pageHeader := ParsePageHeaderBytes(page[pageHeaderOffset : pageHeaderOffset+12])
	log.Println(pageHeader)
	cellPointers := getCellPointersArray(pageHeader.NumberOfCellsOnPage, page, pageHeaderOffset+12)
	log.Printf("Cell pointers array: %v\n", cellPointers)

	var cells []TableInteriorCell
	for _, p := range cellPointers {
		cell := parseInteriorCell(p, page)
		cells = append(cells, cell)
	}
	return cells
}

func (d Database) GetTableEntries(tableName string) []TableLeafCell {
	rootPage := d.getMasterEntryFor(tableName).RootPage
	return d.getLeafs(rootPage)
}

func (d Database) GetFieldFromTable(tableName, fieldName string) []string {
	masterEntry := d.getMasterEntryFor(tableName)
	rootPage := masterEntry.RootPage
	entries := d.getLeafs(rootPage)
	columnPosition := masterEntry.GetColumnPosition(fieldName)

	var out []string
	for _, e := range entries {
		out = append(out, string(e.Values[columnPosition]))
	}

	return out
}

func (d Database) ExecuteSQL(command SqlCommand) []string {
	var out []string
	if command.IsCount {
		tableName := command.TableName
		count := len(d.GetTableEntries(tableName))
		out = append(out, strconv.Itoa(count))
	} else {
		tableName := command.TableName
		masterEntry := d.getMasterEntryFor(tableName)
		entries := d.GetTableEntries(tableName)

		var positions []int
		for _, name := range command.ColumnsNames {
			columnPosition := masterEntry.GetColumnPosition(name)
			positions = append(positions, columnPosition)
		}

		for _, e := range entries {
			var buffer bytes.Buffer
			for _, p := range positions {
				if buffer.Len() != 0 {
					buffer.WriteString("|")
				}
				buffer.Write(e.Values[p])
			}
			out = append(out, buffer.String())
		}
	}
	return out
}

func (d Database) getLeafs(pageNumber int) []TableLeafCell {
	var out []TableLeafCell

	page := d.readPage(pageNumber)

	pageHeader := ParsePageHeaderBytes(page[0:8])
	if pageHeader.isTableLeaf() {
		cellPointers := getCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 8)
		for _, pointer := range cellPointers {
			cell := parseLeafCell(pointer, page)
			out = append(out, cell)
		}
	} else if pageHeader.isTableInterior() {
		cellPointers := getCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 12)
		out = append(out, d.getLeafs(int(pageHeader.RightMostPointer))...)
		for _, pointer := range cellPointers {
			cell := parseInteriorCell(pointer, page)
			out = append(out, d.getLeafs(int(cell.LeftChildPointer))...)
		}
	} else {
		log.Fatalf("Unknown page type: %v", pageHeader.PageType)
	}
	return out
}

func (d Database) getMasterEntryFor(tableName string) MasterEntry {
	for _, e := range d.MasterTable {
		if e.TableName == tableName {
			return e
		}
	}
	log.Fatalf("No table: %v", tableName)
	return MasterEntry{}
}
