package Element

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

type Element struct {
	Crc       uint32
	Timestamp int64
	Tombstone uint8
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func InitElement(key string, value []byte, isDeleted byte) *Element {
	crc := CRC32(value)
	timestamp := time.Now().Unix()
	tombstone := isDeleted
	keySize := uint64(len([]byte(key)))
	valueSize := uint64(len(value))
	return &Element{crc, timestamp, tombstone, keySize, valueSize, key, value}
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (elem *Element) GetSize() uint64{
	return 29 + elem.KeySize + elem.ValueSize
}

func (elem *Element) Encode() []byte {
	elemBytes := make([]byte, 0, elem.GetSize())
	buffer := bytes.NewBuffer(elemBytes)

	err := binary.Write(buffer, binary.LittleEndian, elem.Crc)
	if err != nil {
		return nil
	}

	err = binary.Write(buffer, binary.LittleEndian, elem.Timestamp)
	if err != nil {
		return nil
	}

	err = binary.Write(buffer, binary.LittleEndian, elem.Tombstone)
	if err != nil {
		return nil
	}

	err = binary.Write(buffer, binary.LittleEndian, elem.KeySize)
	if err != nil {
		return nil
	}

	err = binary.Write(buffer, binary.LittleEndian, elem.ValueSize)
	if err != nil {
		return nil
	}

	err = binary.Write(buffer, binary.LittleEndian, []byte(elem.Key))
	if err != nil {
		return nil
	}

	err = binary.Write(buffer, binary.LittleEndian, elem.Value)
	if err != nil {
		return nil
	}

	return buffer.Bytes()
}

func (elem *Element) Decode(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &elem.Crc)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	err = binary.Read(reader, binary.LittleEndian, &elem.Timestamp)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	err = binary.Read(reader, binary.LittleEndian, &elem.Tombstone)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	err = binary.Read(reader, binary.LittleEndian, &elem.KeySize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	err = binary.Read(reader, binary.LittleEndian, &elem.ValueSize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	keyByteSlice := make([]byte, elem.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	elem.Key = string(keyByteSlice)

	elem.Value = make([]byte, elem.ValueSize)
	err = binary.Read(reader, binary.LittleEndian, &elem.Value)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	return false
}

func (elem *Element) Print() {
	fmt.Println("Crc:", elem.Crc)
	fmt.Println("TimeStamp:", elem.Timestamp)
	fmt.Println("Tombstone:", elem.Tombstone)
	fmt.Println("Key size:", elem.KeySize)
	fmt.Println("Value size:", elem.ValueSize)
	fmt.Println("Key:", elem.Key)
	fmt.Println("Value:", elem.Value)
}
