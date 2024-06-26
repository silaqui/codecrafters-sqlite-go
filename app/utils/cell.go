package utils

import "fmt"

type Cell struct {
	Payload          uint64
	RowId            uint64
	RecordHeaderSize uint64
	SerialTypes      []uint64
	Values           [][]byte
}

func (c Cell) String() string {
	return fmt.Sprintf(
		"Payload: %v | ID: %v | HS:%v | S:%v | V:%v\n", c.Payload, c.RowId, c.RecordHeaderSize, c.SerialTypes, c.Values)
}
