package golsm

import (
	"sync"

	"github.com/huandu/skiplist"
)

// In-memory table that supports writes, reads, deletes, and range scans.
type Memtable struct {
	data skiplist.SkipList
	size int64 // Size of the Memtable in bytes.
	mu   sync.RWMutex
}

// Create a new Memtable.
func NewMemtable() *Memtable {
	return &Memtable{
		data: *skiplist.New(skiplist.String),
		size: 0,
	}
}

// Insert a key-value pair into the Memtable.
func (m *Memtable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sizeChange := int64(len(value))
	existingEntry := m.data.Get(key)
	if existingEntry != nil {
		// If the entry already exists, we need to account for the size of the
		// old value.
		m.size -= int64(len(existingEntry.Value.(*MemtableEntry).Value))
	} else {
		// If the entry doesn't exist, we need to account for the size of the key.
		sizeChange += int64(len((key)))
	}

	// Update with the new entry.
	entry := getMemtableEntry(&value, Command_PUT)
	m.data.Set(key, entry)
	m.size += sizeChange
}

// Delete a key-value pair from the Memtable.
func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If the entry already exists, we need to account for the size of the
	// old value.
	existingEntry := m.data.Get(key)
	if existingEntry != nil {
		// Only subtract the size of the existing value, not the key,
		// since the key remains in the memtable with a tombstone marker
		m.size -= int64(len(existingEntry.Value.(*MemtableEntry).Value))
	} else {
		// If the entry doesn't exist, we need to account for the size of the key.
		m.size += int64(len(key))
	}

	m.data.Set(key, getMemtableEntry(nil, Command_DELETE))
}

// Retrieve a value from the Memtable.
func (m *Memtable) Get(key string) []byte {
	m.mu.RLock()
	value := m.data.Get(key)
	m.mu.RUnlock()

	if value == nil {
		return nil
	}

	if value.Value.(*MemtableEntry).Command == Command_DELETE {
		return nil
	}

	return value.Value.(*MemtableEntry).Value
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

		if iter.Value.(*MemtableEntry).Command == Command_DELETE {
			iter = iter.Next()
			continue
		}

		results = append(results, iter.Value.(*MemtableEntry).Value)
		iter = iter.Next()
	}

	return results
}

func (m *Memtable) SizeInBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.size
}
