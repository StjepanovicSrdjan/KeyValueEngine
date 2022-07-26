package SSTable

import (
	"KeyValueEngine/Core/Structures/Element"
	"bufio"
	"encoding/binary"
	"os"
)

func WriteElement(element *Element.Element, writer *bufio.Writer) {
	elToByte := element.Encode()
	err := binary.Write(writer, binary.LittleEndian, elToByte)
	if err != nil {
		return
	}
}

func ReadElement(element *Element.Element, reader *bufio.Reader) bool {
	e := element.Decode(reader)
	return e
}

func ReadAll(path string) ([]Element.Element) {
	file, _ := os.OpenFile(path, os.O_RDONLY, 0777)

	elements := make([]Element.Element, 0)
	reader := bufio.NewReader(file)
	currentElement := Element.Element{}

	for {
		e := ReadElement(&currentElement, reader)
		if e {
			break
		}
		elements = append(elements, currentElement)
	}
	return elements
}

func getElementByPosition(path string, offset uint64) (*Element.Element) {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)

	file.Seek(int64(offset), 0)

	retElement := Element.Element{}
	e := ReadElement(&retElement, reader)
	if e {
		return &Element.Element{}
	}
	return &retElement
}
