package golsm

import (
	"sync"

	"github.com/huandu/skiplist"
)

// In-memory table that supports writes, reads, deletes, and range scans.
type Memtable struct {
	data skiplist.SkipList	
	mu   sync.RWMutex
}

// Create a new Memtable.
func NewMemtable() *Memtable {
	return &Memtable{
		data: *skiplist.New(skiplist.String),
	}
}

// Insert a key-value pair into the Memtable.
func (m *Memtable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data.Set(key, *getMemtableEntry(&value, Command_PUT))
}

// Delete a key-value pair from the Memtable.
func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data.Set(key, *getMemtableEntry(nil, Command_DELETE))
}

// Retrieve a value from the Memtable.
func (m *Memtable) Get(key string) []byte {
	m.mu.RLock()
	value := m.data.Get(key)
	m.mu.RUnlock()

	if value == nil {
		return nil
	}

	if value.Value.(MemtableEntry).Command == Command_DELETE {
		return nil
	}

	return value.Value.(MemtableEntry).Value
}

// Range scan the Memtable, inclusive of startKey and endKey.
func (m *Memtable) Scan(startKey string, endKey string) [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results [][]byte
	// Find the first key that is >= startKey and use the FindNext() method to
	// iterate through the rest of the keys.
	iter := m.data.Find(startKey)
	for iter != nil {
		if iter.Element().Key().(string) > endKey {
			break
		}

		if iter.Value.(MemtableEntry).Command == Command_DELETE {
			iter = iter.Next()
			continue
		}

		results = append(results, iter.Value.(MemtableEntry).Value)
		iter = iter.Next()
	}

	return results
}
