package lru

import "container/list"

// Cache is a LRU cache. It is not safe for concurrent access.
type Cache struct {
	maxBytes int64
	// nbytes     int64
	oldBytes   int64
	youngBytes int64
	youngll    *list.List
	oldll      *list.List
	youngCache map[string]*list.Element
	oldCache   map[string]*list.Element
	// optional and executed when an entry is purged.
	OnEvicted func(key string, value Value)
	k         uint // lru-k, default 2
	ratio     float64
}

type entry struct {
	key   string
	value Value
	visit uint // Visit count
}

// Value use Len to count how many bytes it takes
type Value interface {
	Len() int
}

// New is the Constructor of Cache
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {

	return &Cache{
		maxBytes:   maxBytes,
		youngll:    list.New(),
		oldll:      list.New(),
		youngCache: make(map[string]*list.Element),
		oldCache:   make(map[string]*list.Element),
		// oldBytes:   int64(float64(maxBytes) * (1 - ratio)),
		// youngBytes: int64(float64(maxBytes) * ratio),
		OnEvicted: onEvicted,
		k:         2,
		ratio:     0.75,
	}
}

// func NewCache(maxBytes int64, ratio float64, threshold uint, onEvicted func(string, Value)) *Cache {
// 	return &Cache{
// 		maxBytes:   maxBytes,
// 		youngll:    list.New(),
// 		oldll:      list.New(),
// 		youngCache: make(map[string]*list.Element),
// 		oldCache:   make(map[string]*list.Element),
// 		OnEvicted:  onEvicted,
// 		k:          threshold,
// 	}
// }

// Get look up a key's value
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.youngCache[key]; ok {
		c.youngll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value, true
	} else if ele, ok := c.oldCache[key]; ok {
		// c.oldll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		if kv.visit < c.k {
			kv.visit++
		}
		if kv.visit >= c.k { // Move the ele from oldll to youngll, then move the last element from youngll to oldll
			c.oldll.Remove(ele)
			delete(c.oldCache, key)
			c.youngll.PushFront(ele)
			c.youngCache[key] = ele

			for float64(c.youngBytes) > float64(c.maxBytes)*c.ratio {
				c.PopFromYoung2Old()
			}
			// oldEle := c.RemoveOldestFromYoung()
			// c.oldll.PushFront(oldEle)
			// oldKv := oldEle.Value.(*entry)
			// c.oldCache[oldKv.key] = oldEle
		} else {
			c.oldll.MoveToFront(ele)
		}
		return kv.value, true
	}
	return
}

// RemoveOldest removes the odest item
func (c *Cache) RemoveOldestFromOld() *list.Element {
	ele := c.oldll.Back()
	if ele != nil {
		c.oldll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.oldCache, kv.key)
		c.oldBytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
	return ele
}

// RemoveOldest removes the odest item
func (c *Cache) RemoveOldestFromYoung() *list.Element {
	ele := c.youngll.Back()
	if ele != nil {
		c.youngll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.youngCache, kv.key)

		c.youngBytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
	return ele
}

// PopFromYoung2Old remove the oldest element in youngll to the head of oldll
func (c *Cache) PopFromYoung2Old() {
	ele := c.RemoveOldestFromYoung()
	c.oldll.PushFront(ele)
	kv := ele.Value.(*entry)
	c.oldCache[kv.key] = ele
	c.youngBytes += int64(len(kv.key)) + int64(kv.value.Len())
}

// Add adds a value to the cache.
func (c *Cache) Add(key string, value Value) {
	if ele, ok := c.youngCache[key]; ok { // if youngCache contain key, update
		c.youngll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.youngBytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else if ele, ok := c.oldCache[key]; ok { // update
		kv := ele.Value.(*entry)
		c.oldBytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value

		if kv.visit < c.k {
			kv.visit++
		}
		if kv.visit >= c.k { // Move the ele from oldll to youngll
			c.oldll.Remove(ele)
			delete(c.oldCache, key)
			c.oldBytes -= int64(len(key)) + int64(value.Len())

			c.youngll.PushFront(ele)
			c.youngCache[key] = ele
			c.youngBytes += int64(len(key)) + int64(value.Len())

			for float64(c.youngBytes) > float64(c.maxBytes)*c.ratio {
				c.PopFromYoung2Old()
			}
		} else {
			c.oldll.MoveToFront(ele)
		}
	} else { // insert
		ele = c.oldll.PushFront(&entry{key: key, value: value})
		c.oldCache[key] = ele
		c.oldBytes += int64(len(key)) + int64(value.Len())
	}

	for c.maxBytes != 0 && float64(c.oldBytes) > float64(c.maxBytes)*c.ratio {
		c.RemoveOldestFromOld()
	}
}

// Len the number of cache entries
func (c *Cache) Len() int {
	return c.oldll.Len() + c.youngll.Len()
}
