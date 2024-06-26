package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
)

type Cell struct {
	Payload          uint64
	RowId            uint64
	RecordHeaderSize uint64
	SerialTypes      []uint64
	Values           [][]byte
}

func (c Cell) String() string {
	return fmt.Sprintf(
		"Payload: %v | ID: %v | HS:%v | S:%v | V:%v", c.Payload, c.RowId, c.RecordHeaderSize, c.SerialTypes, c.Values)
}

func (c Cell) PrettyValues() string {
	var buffer bytes.Buffer
	buffer.WriteString("|")

	for i := 0; i < len(c.SerialTypes); i++ {
		s := c.SerialTypes[i]
		v := c.Values[i]
		if s == 0 {
			buffer.WriteString("NULL|")
		} else if s == 1 {
			var str = strconv.Itoa(int(v[0]))
			buffer.WriteString(str)
			buffer.WriteString("|")
		} else if s >= 13 && s%2 == 1 {
			buffer.Write(v)
			buffer.WriteString("|")
		} else {
			var str = fmt.Sprintf("Unknown type: %v at position: %v|", s, i)
			buffer.WriteString(str)
		}
	}

	return buffer.String()
}

func ParseCell(cellStart uint16, page []byte) Cell {
	var cell Cell

	var index = 0
	var tmp = 0
	cell.Payload, tmp = binary.Uvarint(page[cellStart:])
	index += tmp
	cell.RowId, tmp = binary.Uvarint(page[int(cellStart)+index:])
	index += tmp
	cell.RecordHeaderSize, tmp = binary.Uvarint(page[int(cellStart)+index:])
	index += tmp

	for i := tmp; i < int(cell.RecordHeaderSize); {

		temp := []byte{page[int(cellStart)+index]}
		index++
		if page[int(cellStart)+index-1] >= 128 {
			temp = append(temp, []byte{page[int(cellStart)+index]}...)
			index++
		}

		var serialType uint64 = 0
		if len(temp) == 1 {
			serialType, _ = binary.Uvarint(temp)
		} else if len(temp) == 2 {
			serialType = MyUvarint(temp)
		} else {
			log.Fatalf("NOT YET IMPL")
		}
		i += len(temp)

		cell.SerialTypes = append(cell.SerialTypes, serialType)

	}

	for _, s := range cell.SerialTypes {
		if s == 0 {
			cell.Values = append(cell.Values, []byte{})
		} else if s == 1 {
			startSerial := int(cellStart) + index
			index += 1
			value := page[startSerial]
			cell.Values = append(cell.Values, []byte{value})

		} else if s == 2 {
			index += 2
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 3 {
			index += 3
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 4 {
			index += 4
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 5 {
			index += 6
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 6 {
			index += 8
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 7 {
			index += 8
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 8 {
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 9 {
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 10 {
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s == 11 {
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s >= 12 && s%2 == 0 {
			serialSize := (int(s) - 12) / 2
			index += serialSize
			log.Fatalf("NOT IMPLEMENTED: Serial Type: %v\n", s)
		} else if s >= 13 && s%2 == 1 {
			serialSize := (int(s) - 13) / 2
			startSerial := int(cellStart) + index
			endSerial := startSerial + serialSize
			value := page[startSerial:endSerial]
			index += serialSize
			cell.Values = append(cell.Values, value)
		} else {
			log.Fatalf("INVALID: Serial Type: %v\n", s)
		}
	}

	return cell
}

func MyUvarint(buf []byte) uint64 {
	var x uint64
	var s uint
	for _, b := range buf {
		if b < 0x80 {
			x = x<<s | uint64(b)
			return x
		}
		x = x << s
		x |= uint64(b & 0x7f)
		s += 7
	}
	return 0
}
