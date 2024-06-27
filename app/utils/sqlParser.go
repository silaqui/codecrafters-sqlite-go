package utils

import (
	"errors"
	"fmt"
	"strings"
)

type SqlCommand struct {
	TableName    string
	IsCount      bool
	ColumnsNames []string
	Conditions   []Condition
}

type Condition struct {
	Field            string
	Operator         string
	RestrictingValue string
}

func ParseSql(command string) (SqlCommand, error) {
	var out SqlCommand
	tokens := strings.Split(command, " ")

	if tokens[0] != "SELECT" {
		return SqlCommand{}, errors.New(fmt.Sprintf("invalid SQL command - %v in SELECT spot", tokens[0]))
	}
	var countOfNames = 0
	if tokens[1] == "COUNT(*)" {
		out.IsCount = true
	} else {
		for {
			if tokens[1+countOfNames] == "FROM" {
				break
			} else {
				name := strings.Trim(tokens[1+countOfNames], ", ")
				out.ColumnsNames = append(out.ColumnsNames, name)
				countOfNames++
			}
		}
	}
	if tokens[1+countOfNames] != "FROM" {
		return SqlCommand{}, errors.New(fmt.Sprintf("invalid SQL command - %v in FROM spot", tokens[2+countOfNames]))
	}
	out.TableName = tokens[2+countOfNames]

	return out, nil
}
