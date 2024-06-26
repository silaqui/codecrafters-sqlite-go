package main

import (
	"fmt"
	. "github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"os"
	"strconv"
	"strings"
)

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	var d = NewDatabase(databaseFilePath)

	switch command {
	case ".dbinfo":
		fmt.Printf("database page size: %v\n", d.Header.PageSize)
		var numberOfTables = len(d.MasterTable)
		fmt.Printf("number of tables: %v\n", numberOfTables)
	case ".print":
		fmt.Printf("----------- 1 ------------- \n")
		d.ReadAndPrintPage(1)
		fmt.Printf("----------- 2 ------------- \n")
		d.ReadAndPrintPage(2)
		fmt.Printf("----------- 3 ------------- \n")
		d.ReadAndPrintPage(3)
		fmt.Printf("----------- 4 ------------- \n")
		d.ReadAndPrintPage(4)
		fmt.Printf("----------- x ------------- \n")
	case ".tables":
		var names []string
		for _, e := range d.MasterTable {
			if e.Type_ == "table" {
				names = append(names, e.TableName)
			}
		}
		result := strings.Join(names, " ")
		fmt.Println(result)
	default:
		tokens := strings.Split(command, " ")
		var tableName = tokens[len(tokens)-1]
		for _, e := range d.MasterTable {
			if e.TableName == tableName {
				page := d.ReadPage(e.RootPage)
				pageHeader, _ := ParsePageHeaderBytes(page[0:8])
				cellPointers := GetCellPointersArray(pageHeader.NumberOfCellsOnPage, page, 108)
				count := len(cellPointers)
				fmt.Printf(strconv.Itoa(count))
			}
		}
	}
}
