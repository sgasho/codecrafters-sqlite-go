package main

import (
	"fmt"
	"github/com/codecrafters-io/sqlite-starter-go/app/parser"
	"github/com/codecrafters-io/sqlite-starter-go/app/sqlite"
	"github/com/codecrafters-io/sqlite-starter-go/app/utils"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"

	"github.com/rqlite/sql"
)

// Usage: your_sqlite3.sh sample.sqlite .dbinfo
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

	db, err := sqlite.NewDB(f)
	if err != nil {
		log.Fatal(err)
	}

	switch command {
	case ".dbinfo":
		fmt.Println("Logs from your program will appear here!")
		fmt.Printf("database page size: %v\n", db.PageSize())

		fmt.Printf("number of tables: %v\n", db.TableCount())
	case ".tables":
		fmt.Println(strings.Join(db.Tables(), " "))
	default:
		stmt, err := parser.NewStatement(command)
		if err != nil {
			log.Fatal(err)
		}

		switch stmt.(type) {
		case *sql.SelectStatement:
			isCountStmt, err := parser.IsCountStatement(command, stmt)
			if err != nil {
				log.Fatal(err)
			}
			if isCountStmt {
				count, err := db.Count(command)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(count)
			} else {
				cells, err := db.Select(command)
				if err != nil {
					log.Fatal(err)
				}
				rows, err := cells.RowsInStrings()
				if err != nil {
					log.Fatal(err)
				}
				utils.PrintRows(rows)
			}
		}
	}
}
