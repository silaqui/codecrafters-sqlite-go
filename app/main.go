package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	. "github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	headerBytes := make([]byte, 100)
	_, err = databaseFile.Read(headerBytes)
	if err != nil {
		log.Fatal(err)
	}

	header, err := ParseFileHeaderBytes(headerBytes)
	if err != nil {
		log.Fatal(fmt.Printf("Error parsing header: %v\n", err))
	}

	masterEntries := parsMasterTable(databaseFile, header.PageSize)

	switch command {
	case ".dbinfo":
		fmt.Printf("database page size: %v\n", header.PageSize)
		var numberOfTables = len(masterEntries)
		fmt.Printf("number of tables: %v\n", numberOfTables)
	case ".print":
		fmt.Printf("----------- 1 ------------- \n")
		readAndPrintPage(databaseFile, 1, header.PageSize, 100)
		fmt.Printf("----------- 2 ------------- \n")
		readAndPrintPage(databaseFile, 2, header.PageSize, 0)
		fmt.Printf("----------- 3 ------------- \n")
		readAndPrintPage(databaseFile, 3, header.PageSize, 0)
		fmt.Printf("----------- 4 ------------- \n")
		readAndPrintPage(databaseFile, 4, header.PageSize, 0)
		fmt.Printf("----------- x ------------- \n")
	case ".tables":
		var names []string
		for _, e := range masterEntries {
			if e.Type_ == "table" {
				names = append(names, e.TableName)
			}
		}
		result := strings.Join(names, " ")
		fmt.Println(result)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func parsMasterTable(databaseFile *os.File, pageSize uint16) []MasterEntry {
	var out []MasterEntry

	page, _ := readPage(databaseFile, 1, pageSize)
	pageHeader, _ := ParsePageHeaderBytes(page[100:108])
	cellPointers := getCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 108)

	for _, pointer := range cellPointers {
		cell := ParseCell(pointer, page)
		out = append(out, MasterEntryFromCell(cell))
	}

	return out
}

func readAndPrintPage(databaseFile *os.File, pageNumber uint32, pageSize, pageHeaderOffset uint16) {
	page, _ := readPage(databaseFile, pageNumber, pageSize)
	pageHeader, _ := ParsePageHeaderBytes(page[pageHeaderOffset : pageHeaderOffset+8])
	//fmt.Println(pageHeader)
	cellPointers := getCellPointersArray(pageHeader.NumberOfCellsOnPage, page, int(pageHeaderOffset+8))

	for i := 0; i < len(cellPointers); i++ {
		cell := ParseCell(cellPointers[i], page)
		fmt.Printf("Cell %v: %v \n", i+1, cell.PrettyValues())
	}
}
func readPage(databaseFile *os.File, pageNumber uint32, pageSize uint16) ([]byte, error) {
	var page = make([]byte, pageSize)
	var offset = uint32(pageSize) * (pageNumber - 1)
	_, err := databaseFile.ReadAt(page, int64(offset))
	if err != nil {
		return []byte{}, err
	}
	return page, nil
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
