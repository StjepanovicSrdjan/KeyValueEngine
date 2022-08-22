package Cache

import (
	"container/list"
	"KeyValueEngine/Core/Structures/Element"
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

func (cache *CacheLRU) Add(element Element.Element) {
	elem, found := cache.elements[element.Key]
	if found {
		elem.Value = element
		cache.queue.MoveToFront(elem)
		return
	}

	if cache.size == cache.capacity {
		oldest := cache.queue.Back()
		cache.queue.Remove(oldest)
		delete(cache.elements, oldest.Value.(Element.Element).Key)
		cache.size--
	}

	listEl := cache.queue.PushFront(element)
	cache.elements[element.Key] = listEl
	cache.size++
}

func (cache *CacheLRU) Delete(key string) {
	elem, found := cache.elements[key]
	if found{
		delete(cache.elements, key)
		cache.queue.Remove(elem)
		cache.size--
	}
}

func (cache *CacheLRU) Get(key string) (Element.Element, bool){
	elem, found := cache.elements[key]
	if !found {
		return Element.Element{}, false
	}

	cache.queue.MoveToFront(elem)
	return elem.Value.(Element.Element), true
}
