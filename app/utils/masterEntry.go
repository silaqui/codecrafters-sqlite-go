package utils

import "encoding/binary"

type MasterEntry struct {
	Type_      string
	EntityName string
	TableName  string
	RootPage   int
	Sql        string
}

func MasterEntryFromCell(c Cell) MasterEntry {
	var out = MasterEntry{}

	out.Type_ = string(c.Values[0])
	out.EntityName = string(c.Values[1])
	out.TableName = string(c.Values[2])
	root, _ := binary.Uvarint(c.Values[3])
	out.RootPage = int(root)
	out.Sql = string(c.Values[4])

	return out
}
