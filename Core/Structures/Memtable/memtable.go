package Memtable

import (
	"KeyValueEngine/Core/Structures/Element"
	"KeyValueEngine/Core/Structures/SkipList"
)

const (
	defaultCapacity float64 = 10
	defaultThreshold float64 = 0.8
)

type Memtable struct {
	Capacity float64
	Threshold float64
	SkipList *SkipList.SkipList
}

func InitMemtable(capacity int, threshold float64) *Memtable {
	sList := SkipList.InitSkipList()
	return &Memtable{Capacity: float64(capacity), Threshold: threshold, SkipList: sList}
}

func (memtable *Memtable) Clear() {
	sList := SkipList.InitSkipList()
	if memtable.Capacity == 0 {
		memtable.Capacity = defaultCapacity
	}
	if memtable.Threshold == 0 {
		memtable.Threshold = defaultThreshold
	}
	memtable.SkipList = sList
}

func (memtable *Memtable) Add(element Element.Element) ([]Element.Element) {
	var elements []Element.Element = nil
	oldElement, err := memtable.SkipList.Search(element.Key)
	if element.Tombstone == 1 {
		if err == nil {
			if memtable.SkipList != nil && memtable.SkipList.Size >= int(memtable.Capacity*memtable.Threshold) {
				elements = memtable.getAll()
				memtable.Clear()
			}
			memtable.SkipList.Delete(oldElement.Element)
			memtable.SkipList.Insert(element)
		} else {
			memtable.SkipList.Insert(element)
		}
	} else {
		_, err = memtable.SkipList.Search(element.Key)
		if err == nil {
			memtable.SkipList.Insert(element)
		} else {
			if memtable.SkipList != nil && memtable.SkipList.Size >= int(memtable.Capacity*memtable.Threshold) {
				elements = memtable.getAll()
				memtable.Clear()
			}
			memtable.SkipList.Insert(element)
		}
	}
	return elements
}

func (memtable *Memtable) GetElement(key string) (Element.Element, error) {
	node, err := memtable.SkipList.Search(key)
	/*if err == nil {
		if node.Element.Tombstone == 1 {
			return node.Element, errors.New("Not found.")
		}
	}*/
	return node.Element, err
}


func (memtable *Memtable) getAll() []Element.Element {
	if memtable.SkipList.Size == 0 {
		return nil
	}
	var elements []Element.Element
	var currentNode = memtable.SkipList.HeadNode.Forward[0]
	for {
		if currentNode == nil {
			break
		}
		elements = append(elements, currentNode.Element)
		currentNode = currentNode.Forward[0]
	}
	return elements
}
