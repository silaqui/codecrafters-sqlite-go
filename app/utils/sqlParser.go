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
	FieldPosition    int
	Operator         string
	RestrictingValue string
}

func ParseSql(command string) (SqlCommand, error) {
	var out SqlCommand
	var index = 0

	tokens := strings.Split(command, " ")
	for i := 0; i < len(tokens); i++ {
		tokens[i] = strings.Trim(tokens[i], ", ;")
	}

	if tokens[index] != "SELECT" {
		return SqlCommand{}, errors.New(fmt.Sprintf("invalid SQL command - %v in SELECT spot", tokens[0]))
	}
	index++

	if tokens[index] == "COUNT(*)" {
		out.IsCount = true
		index++
	} else {
		for {
			if tokens[index] == "FROM" {
				break
			} else {
				out.ColumnsNames = append(out.ColumnsNames, tokens[index])
				index++
			}
		}
	}

	if tokens[index] != "FROM" {
		return SqlCommand{}, errors.New(fmt.Sprintf("invalid SQL command - %v in FROM spot", tokens[index]))
	}
	index++

	out.TableName = tokens[index]
	index++

	for index < len(tokens) {
		if tokens[index] == "WHERE" {
			index++
			condition := Condition{
				tokens[index],
				-1,
				tokens[index+1],
				tokens[index+2],
			}
			index += 3
			out.Conditions = []Condition{condition}
		} else {
			return SqlCommand{}, errors.New(fmt.Sprintf("invalid SQL command - %v in WHERE spot", tokens[index]))
		}
	}
	return out, nil
}
