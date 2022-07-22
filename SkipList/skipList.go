package SkipList

import (
	"errors"
	"math/rand"
)

const (
	DefaultMaxLevel int = 10
)

type Node struct {
	Element Element
	Forward []*Node
	Level int
}

func InitNode(element Element, creatingLevel int, maxLevel int) *Node {
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
}

func InitSkipList() *SkipList{
	var headElem = Element{
		Crc: 0,
		Timestamp: 0,
		Tombstone: 0,
		KeySize: 0,
		ValueSize: 0,
		Key: "",
		Value: nil,
	}
	newSkipList := &SkipList{HeadNode: InitNode(headElem, 1, DefaultMaxLevel), SkipListLevel: 1}
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
	return *currentNode, errors.New("Not found.")
}



