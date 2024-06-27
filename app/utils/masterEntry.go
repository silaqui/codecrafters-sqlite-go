package utils

import (
	"encoding/binary"
	"strings"
)

type MasterEntry struct {
	Type_      string
	EntityName string
	TableName  string
	RootPage   int
	Sql        string
}

func MasterEntryFromCell(c TableLeafCell) MasterEntry {
	var out = MasterEntry{}

	out.Type_ = string(c.Values[0])
	out.EntityName = string(c.Values[1])
	out.TableName = string(c.Values[2])
	root, _ := binary.Uvarint(c.Values[3])
	out.RootPage = int(root)
	out.Sql = string(c.Values[4])

	return out
}

func (m MasterEntry) GetColumnPosition(columnName string) int {
	out := -1
	open := strings.IndexRune(m.Sql, '(') + 1
	part := m.Sql[open:]
	split := strings.Split(part, ",")
	for i, s := range split {
		name := strings.Split(strings.TrimSpace(s), " ")[0]
		if name == columnName {
			out = i
			break
		}
	}
	return out
}
