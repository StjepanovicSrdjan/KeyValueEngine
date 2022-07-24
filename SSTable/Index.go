package SSTable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type IndexElement struct {
	KeySize uint64
	Key string
	Offset uint64
}

func (indexElement *IndexElement) GetSize() uint64 {
	return 16 + indexElement.KeySize
}

func (indexElement *IndexElement) WriteIndexElement(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, indexElement.KeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(indexElement.Key))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, indexElement.Offset)
	if err != nil {
		panic(err)
	}
}

func (indexElement *IndexElement) ReadIndexElement(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &indexElement.KeySize)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}

	keyByteSlice := make([]byte, indexElement.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)	}
	indexElement.Key = string(keyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &indexElement.Offset)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	return false
}

func getOffsetInDataTableForKey(key string, filePath string, offset uint64, intervalSize uint64) (uint64, bool) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
		return 0, false
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return 0, false
	}

	currentIndexEl := IndexElement{}
	for i := uint64(0); i < intervalSize; i++ {
		eof := currentIndexEl.ReadIndexElement(reader)
		if eof {
			return 0, false
		}

		if currentIndexEl.Key == key {
			return currentIndexEl.Offset, true
		}
	}
	return 0, false
}

