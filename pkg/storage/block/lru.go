package block

import (
	"sync"
)

type lruEntry struct {
	key   string
	value []byte
	size  int
	prev  *lruEntry
	next  *lruEntry
}

// LRUShard is an individual shard of the block cache guarded by its own mutex.
type LRUShard struct {
	mu       sync.Mutex
	capacity int
	currSize int
	items    map[string]*lruEntry
	head     *lruEntry // most recently used
	tail     *lruEntry // least recently used
}

func newLRUShard(capacity int) *LRUShard {
	return &LRUShard{
		capacity: capacity,
		items:    make(map[string]*lruEntry),
	}
}

func (s *LRUShard) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[key]
	if !ok {
		return nil, false
	}
	s.moveToHead(entry)
	return entry.value, true
}

func (s *LRUShard) Put(key string, val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entrySize := len(key) + len(val)

	if entry, ok := s.items[key]; ok {
		s.currSize += (entrySize - entry.size)
		entry.value = val
		entry.size = entrySize
		s.moveToHead(entry)
	} else {
		newEntry := &lruEntry{
			key:   key,
			value: val,
			size:  entrySize,
		}
		s.items[key] = newEntry
		s.addToHead(newEntry)
		s.currSize += entrySize
	}

	// Evict least recently used entries if over capacity
	for s.currSize > s.capacity && s.tail != nil {
		s.evictTail()
	}
}

func (s *LRUShard) addToHead(e *lruEntry) {
	e.next = s.head
	e.prev = nil
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

func (s *LRUShard) remove(e *lruEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}

	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
}

func (s *LRUShard) moveToHead(e *lruEntry) {
	if s.head == e {
		return
	}
	s.remove(e)
	s.addToHead(e)
}

func (s *LRUShard) evictTail() {
	if s.tail == nil {
		return
	}
	old := s.tail
	s.remove(old)
	delete(s.items, old.key)
	s.currSize -= old.size
}
