package main

import (
	"fmt"
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

	f, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	db, err := NewDB(f)
	if err != nil {
		log.Fatal(err)
	}

	switch command {
	case ".dbinfo":
		fmt.Println("Logs from your program will appear here!")
		fmt.Printf("database page size: %v\n", db.PageSize)

		fmt.Printf("number of tables: %v\n", db.CellCount)
	case ".tables":
		fmt.Println(strings.Join(db.SQLiteMasterRows.GetTableNames(), " "))
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
