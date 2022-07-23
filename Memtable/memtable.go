package Memtable

import (
	"KeyValueEngine/SkipList"
	"errors"
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

func InitMemtable() *SkipList.SkipList {
	sList := SkipList.InitSkipList()
	return &Memtable{Capacity: defaultCapacity, Threshold: defaultThreshold, SkipList: sList}
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

func (memtable *Memtable) Add(element SkipList.Element) ([]SkipList.Element) {
	var elements []SkipList.Element = nil
	_, err := memtable.SkipList.Search(element.Key)
	if element.Tombstone == 1 {
		if err != nil {
			if memtable.SkipList != nil && memtable.SkipList.Size >= int(memtable.Capacity*memtable.Threshold) {
				elements = memtable.getAll()
				memtable.Clear()
			}
			err = memtable.SkipList.Delete(element)
		} else {
			err = memtable.SkipList.Delete(element)
		}
	} else {
		_, err = memtable.SkipList.Search(element.Key)
		if err == nil {
			memtable.SkipList.Add(element)
		} else {
			if memtable.SkipList != nil && memtable.SkipList.Size >= int(memtable.Capacity*memtable.Threshold) {
				elements = memtable.getAll()
				memtable.Clear()
			}
			memtable.SkipList.Add(element)
		}
	}
	return elements
}

func (memtable *Memtable) GetElement(key string) (SkipList.Element, error) {
	node, err := memtable.SkipList.Search(key)
	if err == nil {
		if node.Element.Tombstone == 1 {
			return node, errors.New("Not found.")
		}
	}
	return node, err
}


func (memtable *Memtable) getAll() []SkipList.Element {
	if memtable.SkipList.Size == 0 {
		return nil
	}
	var elements []SkipList.Element
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
