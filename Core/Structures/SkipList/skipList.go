package SkipList

import (
	"KeyValueEngine/Core/Structures/Element"
	"errors"
	"fmt"
	"math/rand"
)

const (
	DefaultMaxLevel int = 10
)

type Node struct {
	Element Element.Element
	Forward []*Node
	Level int
}

func InitNode(element Element.Element, creatingLevel int, maxLevel int) *Node {
	forwardEmpty := make([]*Node, maxLevel)
	for i := 0; i <= maxLevel-1; i++ {
		forwardEmpty[i] = nil
	}
	return &Node{Element: element, Forward: forwardEmpty, Level: creatingLevel}
}


type SkipList struct {
	HeadNode *Node
	MaxLevel int
	SkipListLevel int
	Size int
}

func InitSkipList() *SkipList {
	var headElem = Element.Element{
		Crc: 0,
		Timestamp: 0,
		Tombstone: 0,
		KeySize: 0,
		ValueSize: 0,
		Key: "",
		Value: nil,
	}
	newSkipList := &SkipList{HeadNode: InitNode(headElem, 1, DefaultMaxLevel), SkipListLevel: 1, Size: 0}
	newSkipList.MaxLevel = DefaultMaxLevel
	return newSkipList
}

func (sList *SkipList) SetMaxLevel(maxLevel int){
	sList.MaxLevel = maxLevel
}

func (sList *SkipList) GetRandomLevel() int {
	currentLevel := 1
	for rand.Intn(2) == 1 && currentLevel < sList.MaxLevel{
		currentLevel++
	}
	return currentLevel
}

func (sList *SkipList) Search(key string) (Node, error){
	currentNode := sList.HeadNode

	for i := sList.SkipListLevel - 1; i >= 0; i-- {
		for currentNode.Forward[i] != nil && currentNode.Forward[i].Element.Key < key {
			currentNode = currentNode.Forward[i]
		}
	}

	currentNode = currentNode.Forward[0]

	if currentNode != nil && currentNode.Element.Key == key {
		return *currentNode, nil
	}
	return Node{}, errors.New("Not found.")
}

func (sList *SkipList) Insert(element Element.Element) {
	updateList := make([]*Node, sList.MaxLevel)
	currentNode := sList.HeadNode

	for i:= sList.HeadNode.Level - 1; i >= 0; i-- {
		for currentNode.Forward[i] != nil && currentNode.Forward[i].Element.Key < element.Key {
			currentNode = currentNode.Forward[i]
		}
		updateList[i] = currentNode
	}

	currentNode = currentNode.Forward[0]

	if currentNode != nil &&currentNode.Element.Key == element.Key{
		currentNode.Element.Value = element.Value
		currentNode.Element.ValueSize = element.ValueSize
	} else {
		newLevel := sList.GetRandomLevel()
		if newLevel > sList.SkipListLevel{
			for i := sList.SkipListLevel + 1; i <= newLevel; i++ {
				updateList[i-1] = sList.HeadNode
			}
			sList.SkipListLevel = newLevel
			sList.HeadNode.Level = newLevel
		}

		newNode := InitNode(element, newLevel, sList.MaxLevel)
		for i := 0; i < newLevel; i++ {
			newNode.Forward[i] = updateList[i].Forward[i]
			updateList[i].Forward[i] = newNode
		}
		sList.Size++
	}
}

func (sList *SkipList) Delete(element Element.Element) error {
	updateList := make([]*Node, sList.MaxLevel)
	currentNode := sList.HeadNode

	for i := sList.HeadNode.Level - 1; i >= 0; i-- {
		for currentNode.Forward[i] != nil && currentNode.Forward[i].Element.Key < element.Key {
			currentNode = currentNode.Forward[i]
		}
		updateList[i] = currentNode
	}

	currentNode = currentNode.Forward[0]

	if currentNode.Element.Key == element.Key {
		for i := 0; i <= currentNode.Level-1; i++ {
			if updateList[i].Forward[i] != nil && updateList[i].Forward[i].Element.Key != currentNode.Element.Key {
				break
			}
			updateList[i].Forward[i] = currentNode.Forward[i]
		}

		for currentNode.Level > 1 && sList.HeadNode.Forward[currentNode.Level] == nil {
			currentNode.Level--
		}

		currentNode = nil
		return nil
	}
	return errors.New("Not found")
}

func (sList *SkipList) Print() {
	fmt.Printf("\nhead->")
	currentNode := sList.HeadNode

	for {
		fmt.Printf("[key:%d][val:%v]->", currentNode.Element.Key, currentNode.Element.Value)
		if currentNode.Forward[0] == nil {
			break
		}
		currentNode = currentNode.Forward[0]
	}
}




