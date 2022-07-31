package SSTable

import (
	"KeyValueEngine/BloomFilter"
	"KeyValueEngine/Element"
	"KeyValueEngine/MerkleTree"
	"bufio"
	"encoding/binary"
	"errors"
	"os"
)

type SSTable struct {
	DataFilePath string
	IndexFilePath string
	SummeryFilePath string
	FilterFilePath string
	MetadataFilePath string
	TOCFilePath string
}

func InitSSTable(elements []Element.Element, dataPath, indexPath, summaryPath,
	filterPath, metadataPath, tocPath string) *SSTable {
	createDIS(elements, dataPath, indexPath, summaryPath)
	createFilter(elements, filterPath)
	createMetadata(elements, metadataPath)
	createTOC(dataPath, indexPath, summaryPath, filterPath, metadataPath, tocPath)

	return &SSTable{dataPath, indexPath, summaryPath,
		filterPath, metadataPath, tocPath}
}

func (ssTable *SSTable) GetElement(key string) (*Element.Element, error) {
	indexPosition := GetPositionInIndex(key, ssTable.SummeryFilePath)
	if indexPosition == 0 {
		return &Element.Element{}, errors.New("Key not found.")
	}

	dataPosition, found := getPositionInData(key, ssTable.IndexFilePath, indexPosition, 12)
	if !found {
		return &Element.Element{}, errors.New("Key not found.")
	}

	element := getElementByPosition(ssTable.DataFilePath, dataPosition)
	if element.Key == "" {
		return &Element.Element{}, errors.New("Key not found.")
	}

	return element, nil
}

func (ssTable *SSTable) Delete() {
	err := os.Remove(ssTable.DataFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(ssTable.IndexFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(ssTable.SummeryFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(ssTable.FilterFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(ssTable.MetadataFilePath)
	if err!= nil {
		panic(err)
	}

	err = os.Remove(ssTable.TOCFilePath)
	if err!= nil {
		panic(err)
	}
}




// creates date, index and summery files
func createDIS(elements []Element.Element, dataPath string, indexPath string, summeryPath string){
	dataFile, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	dataFileWriter := bufio.NewWriter(dataFile)

	indexFile, err := os.OpenFile(indexPath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	indexFileWriter := bufio.NewWriter(indexFile)

	summaryFile, err := os.OpenFile(summeryPath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	summaryFileWriter := bufio.NewWriter(summaryFile)

	summaryHeader := SummeryHeader{}
	summaryHeader.MinKey = elements[0].Key
	summaryHeader.MinKeySize = uint64(len(summaryHeader.MinKey))
	summaryHeader.MaxKey = elements[len(elements)-1].Key
	summaryHeader.MaxKeySize = uint64(len(summaryHeader.MaxKey))

	summeryElements := make([]SummeryElement, 0)

	positionData := uint64(0)
	positionIndex := uint64(0)

	for index, element := range elements {
		WriteElement(&element, dataFileWriter)

		indexElement := IndexElement{KeySize: uint64(len(element.Key)),
			Key: element.Key, Position: positionData}
		indexElement.Write(indexFileWriter)

		positionData += element.GetSize()

		if index%12 == 0 || index == len(elements)-1 {
			summaryEntry := SummeryElement{KeySize: indexElement.KeySize, Key: indexElement.Key,
				Position: positionIndex}
			summeryElements = append(summeryElements, summaryEntry)

			summaryHeader.ElementBlockSize += summaryEntry.GetSize()
		}
		positionIndex += indexElement.GetSize()
	}

	summaryHeader.Write(summaryFileWriter)
	for _, summaryEntry := range summeryElements {
		summaryEntry.Write(summaryFileWriter)
	}

	err = dataFileWriter.Flush()
	if err != nil { panic(err) }
	err = dataFile.Close()
	if err != nil { panic(err) }
	err = indexFileWriter.Flush()
	if err != nil { panic(err) }
	err = indexFile.Close()
	if err != nil { panic(err) }
	err = summaryFileWriter.Flush()
	if err != nil { panic(err) }
	err = summaryFile.Close()
	if err != nil { panic(err) }
}

func createFilter(elements []Element.Element, filterPath string) {
	filter := BloomFilter.InitBF(len(elements), 0.01)
	for _, recordElement := range elements {
		filter.Add(recordElement.Key)
	}
	filter.Encode(filterPath)
}

func createMetadata(elements []Element.Element, metadataPath string) {
	elementsBytes :=  make([][]byte, 0)
	for _, element := range elements {
		elementBytes := element.Encode()
		elementsBytes = append(elementsBytes, elementBytes)
	}

	metadata := MerkleTree.InitMerkleTree(elementsBytes)
	metadata.Serialize(metadataPath)
}

func createTOC(tocPath, dataPath, indexPath, summeryPath, metadataPath, filterPath string) {
	tocFile, err := os.OpenFile(tocPath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}
	tocFileWriter := bufio.NewWriter(tocFile)

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(dataPath + "\n"))
	if err != nil {
		return
	}

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(indexPath + "\n"))
	if err != nil {
		return
	}

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(summeryPath + "\n"))
	if err != nil {
		return
	}

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(filterPath + "\n"))
	if err != nil {
		return
	}

	err = binary.Write(tocFileWriter, binary.LittleEndian, []byte(metadataPath + "\n"))
	if err != nil {
		return
	}

	err = tocFileWriter.Flush()
	if err != nil {
		return
	}
	err = tocFile.Close()
	if err != nil {
		return
	}
}
