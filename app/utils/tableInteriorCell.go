package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type TableInteriorCell struct {
	LeftChildPointer uint32
	Key              uint64
}

func parseInteriorCell(cellStart uint16, page []byte) TableInteriorCell {
	var cell TableInteriorCell
	if err := binary.Read(bytes.NewReader(page[cellStart:cellStart+4]), binary.BigEndian, &cell.LeftChildPointer); err != nil {
	}
	cell.Key, _ = MyUvarint(page[cellStart+4:])
	return cell
}

func (c TableInteriorCell) PrettyValues() string {
	return fmt.Sprintf("K: %v | P: %v", c.Key, c.LeftChildPointer)
}
