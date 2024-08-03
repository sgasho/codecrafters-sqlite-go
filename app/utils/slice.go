package utils

import "fmt"

func SliceIncludes[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func PrintRows(rows [][]string) {
	for _, row := range rows {
		for _, v := range row {
			fmt.Println(v) // TODO: allow multiple columns
		}
	}
}
