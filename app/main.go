package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	. "github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
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

		//fmt.Printf("%v\n", header)
		fmt.Printf("database page size: %v\n", header.PageSize)

		fmt.Printf("----------- 1 ------------- \n")
		readAndPrintPage(databaseFile, 1, header.PageSize, 100)
		fmt.Printf("----------- 2 ------------- \n")
		readAndPrintPage(databaseFile, 2, header.PageSize, 0)
		fmt.Printf("----------- 3 ------------- \n")
		readAndPrintPage(databaseFile, 3, header.PageSize, 0)
		fmt.Printf("----------- 4 ------------- \n")
		readAndPrintPage(databaseFile, 4, header.PageSize, 0)
		fmt.Printf("----------- x ------------- \n")

		//fmt.Printf("number of tables: %v\n", todo)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func readAndPrintPage(databaseFile *os.File, pageNumber uint32, pageSize, pageHeaderOffset uint16) {
	page, _ := readPage(databaseFile, pageNumber, pageSize)
	pageHeader, _ := ParsePageHeaderBytes(page[pageHeaderOffset : pageHeaderOffset+8])
	cellPointers := getCellPointersArray(pageHeader.NumberOfCellsOnPage, page, int(pageHeaderOffset+8))

	printCells(cellPointers, page)
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

func printCells(cellPointers []uint16, page []byte) {
	for i := 0; i < len(cellPointers); i++ {
		start := cellPointers[i]
		fmt.Printf("Cell %v: \n", i+1)
		cell := parseCell(start, page)
		fmt.Println(cell)
	}
}

func parseCell(cellStart uint16, page []byte) Cell {
	var cell Cell

	var index = 0
	var tmp = 0
	cell.Payload, tmp = binary.Uvarint(page[cellStart:])
	index += tmp
	cell.RowId, tmp = binary.Uvarint(page[int(cellStart)+index:])
	index += tmp
	cell.RecordHeaderSize, tmp = binary.Uvarint(page[int(cellStart)+index:])
	index += tmp

	//fmt.Printf("%v\n", page[int(cellStart):int(cellStart)+o1+o2+int(payload)])
	//fmt.Printf("Payload: %v | Id: %v | Hs:%v \n", payload, rowId, recordHeaderSize)
	//fmt.Printf("Payload: %v\n", page[int(cellStart)+o1+o2+int(recordHeaderSize):int(cellStart)+o1+o2+int(payload)])
	//fmt.Printf(string(page[int(cellStart)+o1+o2+int(recordHeaderSize) : int(cellStart)+o1+o2+int(payload)]))
	//fmt.Printf("\n")

	for i := tmp; i < int(cell.RecordHeaderSize); {

		temp := []byte{page[int(cellStart)+index]}
		index++
		if page[int(cellStart)+index-1] >= 128 {
			//fmt.Printf("Added byte: %v and next %v\n", temp, page[int(cellStart)+index])
			temp = append(temp, []byte{page[int(cellStart)+index]}...)
			index++
		}

		var serialType uint64 = 0
		if len(temp) == 1 {
			serialType, _ = binary.Uvarint(temp)
		} else if len(temp) == 2 {
			//fmt.Printf("Parsing bytes: %v\n", temp)
			serialType = MyUvarint(temp)
		} else {
			log.Fatalf("NOT YET IMPL")
		}
		i += len(temp)

		cell.SerialTypes = append(cell.SerialTypes, serialType)
		//fmt.Printf("Added %v index moved by %v to %v\n", serialType, o4, index)
		//fmt.Printf("Added %v index moved by %v to %v\n", serialType, len(temp), index)

	}

	//fmt.Printf("Serials: %v\n", serialTypes)

	for _, s := range cell.SerialTypes {
		if s == 0 {
			cell.Values = append(cell.Values, []byte{})
			//fmt.Printf("Adding: NULL\n")
		} else if s == 1 {
			startSerial := int(cellStart) + index
			//endSerial := startSerial + 1
			//fmt.Printf("Adding %v-%v : ", startSerial, endSerial)
			//fmt.Printf("%v\n", page[startSerial:endSerial])
			index += 1
			value := page[startSerial]
			//cell.Values = append(cell.Values, strconv.Itoa(int(value)))
			cell.Values = append(cell.Values, []byte{value})

		} else if s == 2 {
			index += 2
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 3 {
			index += 3
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 4 {
			index += 4
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 5 {
			index += 6
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 6 {
			index += 8
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 7 {
			index += 8
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 8 {
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 9 {
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 10 {
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 11 {
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s >= 12 && s%2 == 0 {
			serialSize := (int(s) - 12) / 2
			index += serialSize
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s >= 13 && s%2 == 1 {
			serialSize := (int(s) - 13) / 2
			startSerial := int(cellStart) + index
			endSerial := startSerial + serialSize
			//fmt.Printf("Adding %v-%v : ", startSerial, endSerial)
			//fmt.Printf("%v ", page[startSerial:endSerial])
			//value := string(page[startSerial:endSerial])
			value := page[startSerial:endSerial]
			//fmt.Printf("%v\n", value)
			index += serialSize
			//values = append(values, value)
			cell.Values = append(cell.Values, value)
		} else {
			log.Fatalf("INVALID: Serial Type: %v\n", s)
		}
	}
	//fmt.Printf("Values: |%v|\n", strings.Join(values, "|"))

	return cell
}

func MyUvarint(buf []byte) uint64 {
	var x uint64
	var s uint
	for _, b := range buf {
		//fmt.Printf("B: %v\n", strconv.FormatUint(uint64(b), 2))
		if b < 0x80 {
			x = x<<s | uint64(b)
			//fmt.Printf("X: %v\n", strconv.FormatUint(x, 2))
			return x
		}
		x = x << s
		x |= uint64(b & 0x7f)
		s += 7
		//fmt.Printf("X: %v\n", strconv.FormatUint(x, 2))
	}
	return 0
}
