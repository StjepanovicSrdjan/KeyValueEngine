package SSTable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type SummeryHeader struct {
	MinKeySize uint
	MinKey string
	MaxKeySize uint
	MaxKey string
	ElementBlockSize uint
}

type SummeryElement struct {
	KeySize uint
	Key string
	Position uint
}

func (summeryElement *SummeryElement) GetSize() uint{
	return 16 + summeryElement.KeySize
}

func (summaryHeader *SummeryHeader) Write(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, summaryHeader.MinKeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryHeader.MinKey))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, summaryHeader.MaxKeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryHeader.MaxKey))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, summaryHeader.ElementBlockSize)
	if err != nil {
		panic(err)
	}
}

func (summaryHeader *SummeryHeader) Read(reader *bufio.Reader)  {
	err := binary.Read(reader, binary.LittleEndian, &summaryHeader.MinKeySize)
	if err != nil {
		panic(err)
	}

	minKeyByteSlice := make([]byte, summaryHeader.MinKeySize)
	err = binary.Read(reader, binary.LittleEndian, &minKeyByteSlice)
	if err != nil {
		panic(err)
	}
	summaryHeader.MinKey = string(minKeyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryHeader.MaxKeySize)
	if err != nil {
		panic(err)
	}

	maxKeyByteSlice := make([]byte, summaryHeader.MaxKeySize)
	err = binary.Read(reader, binary.LittleEndian, &maxKeyByteSlice)
	if err != nil {
		panic(err)
	}
	summaryHeader.MaxKey = string(maxKeyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryHeader.ElementBlockSize)
	if err != nil {
		panic(err)
	}
}

func (summaryEntry *SummeryElement) Write(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, summaryEntry.KeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryEntry.Key))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, summaryEntry.Position)
	if err != nil {
		panic(err)
	}
}

func (summaryEntry *SummeryElement) Read(reader *bufio.Reader)  {
	err := binary.Read(reader, binary.LittleEndian, &summaryEntry.KeySize)
	if err != nil {
		panic(err)
	}

	keyByteSlice := make([]byte, summaryEntry.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if err != nil {
		panic(err)
	}
	summaryEntry.Key = string(keyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryEntry.Position)
	if err != nil {
		panic(err)
	}
}

func GetPosition(key string, path string) uint {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	summaryHeader := SummeryHeader{}
	summaryHeader.Read(reader)

	if summaryHeader.MinKey > key {
		return 0
	}

	if summaryHeader.MaxKey < key {
		return 0
	}

	elements := make([]byte, summaryHeader.ElementBlockSize)
	_, err = io.ReadFull(reader, elements)
	if err != nil {
		panic(err)
	}

	reader = bufio.NewReader(bytes.NewBuffer(elements))
	prevElem := SummeryElement{}
	nextElem := SummeryElement{}

	for {
		prevElem = nextElem
		nextElem.Read(reader)
		if prevElem == nextElem {
			return prevElem.Position
		}

		if prevElem.Key <= key && key < nextElem.Key {
			break
		}
	}

	return prevElem.Position
}



