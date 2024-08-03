package utils

import (
	"fmt"
	"strings"
)

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
		fmt.Println(strings.Join(row, "|"))
	}
}
