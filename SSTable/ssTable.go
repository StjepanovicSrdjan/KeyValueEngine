package SSTable

import (
	"KeyValueEngine/BloomFilter"
	"KeyValueEngine/Element"
	"bufio"
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
