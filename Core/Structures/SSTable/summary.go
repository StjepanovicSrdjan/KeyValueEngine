package SSTable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"strconv"
)

type SummeryHeader struct {
	MinKeySize uint64
	MinKey string
	MaxKeySize uint64
	MaxKey string
	ElementBlockSize uint64
}

type SummeryElement struct {
	KeySize uint64
	Key string
	Position uint64
}

func (summaryElement *SummeryElement) GetSize() uint64{
	return 16 + summaryElement.KeySize
}

func (summeryHeader *SummeryHeader) GetSize() uint64{
	return 24 + summeryHeader.MinKeySize + summeryHeader.MaxKeySize
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

func (summaryElement *SummeryElement) Write(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, summaryElement.KeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryElement.Key))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, summaryElement.Position)
	if err != nil {
		panic(err)
	}
}

func (summaryElement *SummeryElement) Read(reader *bufio.Reader)  {
	err := binary.Read(reader, binary.LittleEndian, &summaryElement.KeySize)
	if err != nil {
		panic(err)
	}

	keyByteSlice := make([]byte, summaryElement.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if err != nil {
		panic(err)
	}
	summaryElement.Key = string(keyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryElement.Position)
	if err != nil {
		panic(err)
	}
}

func (summaryElement *SummeryElement) ReadRange(file *os.File) (error){

	keySizeByte := make([]byte, 8)
	_, err := file.Read(keySizeByte)
	if err != nil {
		return err
	}
	keySize, _ := strconv.Atoi(string(keySizeByte))
	summaryElement.KeySize = uint64(keySize)

	keyByte := make([]byte, keySize)
	_, err = file.Seek(8, 1)
	if err != nil {
		return err
	}
	_, err = file.Read(keyByte)
	if err != nil {
		return err
	}
	summaryElement.Key = string(keyByte)

	positionByte := make([]byte, 8)
	_, err = file.Seek(int64(keySize), 1)
	if err != nil {
		return err
	}
	_, err = file.Read(positionByte)
	if err != nil {
		return err
	}
	position, _ := strconv.Atoi(string(positionByte))
	summaryElement.Position = uint64(position)

	_, err = file.Seek(8, 1)
	if err != nil {
		return err
	}

	return nil

	/*if startIndex < 0 {
		return errors.New("invalid startIndex")
	}
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()

	if startIndex + 8 >= len(mmapf) {
		return errors.New("indices invalid")
	}
	keySizeByte := make([]byte, 8)
	copy(keySizeByte, mmapf[startIndex:startIndex+8])
	keySize, _ := strconv.Atoi(string(keySizeByte))
	summaryElement.KeySize = uint64(keySize)

	keyByte := make([]byte, keySize)
	copy(keyByte, mmapf[startIndex+8:startIndex+8+keySize])
	summaryElement.Key = string(keyByte)

	positionByte := make([]byte, 8)
	copy(positionByte, mmapf[startIndex+8+keySize: startIndex+keySize+16])
	position, _ := strconv.Atoi(string(positionByte))
 	summaryElement.Position = uint64(position)

	return  nil*/
}

func GetPositionInIndex(key string, path string) uint64 {
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

	prevElem := SummeryElement{}
	nextElem := SummeryElement{}

	for {
		prevElem = nextElem
		err = nextElem.ReadRange(file)
		if err != nil && err != io.EOF{
			panic(err)
		}
		if prevElem == nextElem {
			return prevElem.Position
		}

		if (prevElem.Key <= key && key < nextElem.Key) || err == io.EOF{
			break
		}
	}

	return prevElem.Position
}



