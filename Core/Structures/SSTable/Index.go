package SSTable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type IndexElement struct {
	KeySize uint64
	Key      string
	Position uint64
}

func (indexElement *IndexElement) GetSize() uint64 {
	return 16 + indexElement.KeySize
}

func (indexElement *IndexElement) Write(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, indexElement.KeySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, []byte(indexElement.Key))
	if err != nil {
		panic(err)
	}

	err = binary.Write(writer, binary.LittleEndian, indexElement.Position)
	if err != nil {
		panic(err)
	}
}

func (indexElement *IndexElement) Read(reader *bufio.Reader) bool {
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

	err = binary.Read(reader, binary.LittleEndian, &indexElement.Position)
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	return false
}

func (indexElement *IndexElement) ReadRange(file *os.File) (error){

	keySizeByte := make([]byte, 8)
	_, err := file.Read(keySizeByte)
	if err != nil {
		return err
	}
	//keySize, _ := strconv.Atoi(string(keySizeByte))
	keySize := binary.LittleEndian.Uint64(keySizeByte)
	indexElement.KeySize = uint64(keySize)

	keyByte := make([]byte, keySize)
	/*_, err = file.Seek(8, 1)
	if err != nil {
		return err
	}*/
	_, err = file.Read(keyByte)
	if err != nil {
		return err
	}
	indexElement.Key = string(keyByte)

	positionByte := make([]byte, 8)
	/*_, err = file.Seek(int64(keySize), 1)
	if err != nil {
		return err
	}
	*/_, err = file.Read(positionByte)
	if err != nil {
		return err
	}
	//position, _ := strconv.Atoi(string(positionByte))
	position := binary.LittleEndian.Uint64(positionByte)
	indexElement.Position = uint64(position)

	/*_, err = file.Seek(8, 1)
	if err != nil {
		return err
	}*/

	return nil

}

func getPositionInData(key string, filePath string, position uint64, intervalSize uint64) (uint64, bool) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
		return 0, false
	}
	defer file.Close()
	/*reader := bufio.NewReader(file)

	_, err = file.Seek(int64(position), 0)
	if err != nil {
		return 0, false
	}
*/
	currentIndexEl := IndexElement{}
	_, _ = file.Seek(int64(position), 0)
	for i := 0; i < int(intervalSize); i++ {
		err = currentIndexEl.ReadRange(file)
		if err != nil {
			return 0, false
		}

		if currentIndexEl.Key == key {
			return currentIndexEl.Position, true
		}
	}
	return 0, false
}

