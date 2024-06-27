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
		dbInfo(d)
	case ".print":
		printContent(d, int(d.Header.NumberOfPages))
	case ".tables":
		tables(d)
	default:
		parseSQL(d, command)
	}
}

func parseSQL(d *Database, command string) {
	tokens := strings.Split(command, " ")

	if tokens[0] == "SELECT" {
		if tokens[1] == "COUNT(*)" {
			var tableName = tokens[len(tokens)-1]
			count := len(d.GetTableEntries(tableName))
			fmt.Printf(strconv.Itoa(count))
		} else {
			//var tableName = tokens[len(tokens)-1]
			//d.GetTableInfo(tableName)
			//entries := d.GetTableEntries(tableName)
		}
	} else {
		fmt.Printf("Unknown command: %v", command)
	}
}

func tables(d *Database) {
	var names []string
	for _, e := range d.MasterTable {
		if e.Type_ == "table" {
			names = append(names, e.TableName)
		}
	}
	result := strings.Join(names, " ")
	fmt.Println(result)
}

func printContent(d *Database, number int) {
	for i := 1; i <= number; i++ {
		fmt.Printf("----------- %v ------------- \n", i)
		d.ReadAndPrintPage(i)
	}
	fmt.Printf("----------- x ------------- \n")
}

func dbInfo(d *Database) {
	fmt.Printf("database page size: %v\n", d.Header.PageSize)
	var numberOfTables = len(d.MasterTable)
	fmt.Printf("number of tables: %v\n", numberOfTables)
}
