package Cache

import (
	"container/list"
)

type ListElement struct {
	key string
	value []byte
}

type CacheLRU struct {
	size int
	capacity int
	queue list.List
	elements map[string]*list.Element
}

func InitCache(capacity int) *CacheLRU {
	if capacity < 1 {
		panic("Error")
	}

	retLRU := CacheLRU{
		capacity: capacity,
		size: 0,
		queue: list.List{},
		elements: make(map[string]*list.Element, capacity)}
	return &retLRU
}

func (cache *CacheLRU) SetCapacity(cap int) {
	cache.capacity = cap
}

func (cache *CacheLRU) Add(key string, value []byte) {
	elem, found := cache.elements[key]
	if found {
		newElement := ListElement{key: key, value: value}
		elem.Value = newElement
		cache.queue.MoveToFront(elem)
		return
	}

	if cache.size == cache.capacity {
		oldest := cache.queue.Back()
		cache.queue.Remove(oldest)
		delete(cache.elements, oldest.Value.(ListElement).key)
		cache.size--
	}

	newElement := ListElement{key: key, value: value}
	listEl := cache.queue.PushFront(newElement)
	cache.elements[key] = listEl
	cache.size++
}
