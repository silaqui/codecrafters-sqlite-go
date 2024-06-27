package main

import (
	"fmt"
	. "github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"log"
	"os"
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
	c, err := ParseSql(command)
	if err != nil {
		log.Printf("Unknown command: %v|", command, err)
		return
	}
	result := d.ExecuteSQL(c)
	for _, e := range result {
		fmt.Println(e)
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
	number = 1
	for i := 1; i <= number; i++ {
		log.Printf("----------- %v ------------- \n", i)
		d.ReadAndPrintPage(i)
	}
	log.Printf("----------- x ------------- \n")
}

func dbInfo(d *Database) {
	fmt.Printf("database page size: %v\n", d.Header.PageSize)
	var numberOfTables = len(d.MasterTable)
	fmt.Printf("number of tables: %v\n", numberOfTables)
}
