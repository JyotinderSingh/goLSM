package golsm

import (
	"sync"
	"time"

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
	entry := getMemtableEntry(key, &value, Command_PUT)
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

	m.data.Set(key, getMemtableEntry(key, nil, Command_DELETE))
}

// Retrieve a value from the Memtable.
func (m *Memtable) Get(key string) *MemtableEntry {
	m.mu.RLock()
	value := m.data.Get(key)
	m.mu.RUnlock()

	if value == nil {
		return nil
	}

	// We need to include the tombstones in the range scan. The caller will
	// need to check the Command field of the MemtableEntry to determine if
	// the entry is a tombstone.
	return value.Value.(*MemtableEntry)
}

// Range scan the Memtable, inclusive of startKey and endKey.
func (m *Memtable) RangeScan(startKey string, endKey string) []*MemtableEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*MemtableEntry
	// Find the first key that is >= startKey and use the FindNext() method to
	// iterate through the rest of the keys.
	iter := m.data.Find(startKey)
	for iter != nil {
		if iter.Element().Key().(string) > endKey {
			break
		}

		// We need to include the tombstones in the range scan. The caller will
		// need to check the Command field of the MemtableEntry to determine if
		// the entry is a tombstone.
		results = append(results, iter.Value.(*MemtableEntry))
		iter = iter.Next()
	}

	return results
}

// Get the size of the Memtable in bytes.
func (m *Memtable) SizeInBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.size
}

// Clears the Memtable.
func (m *Memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data.Init()
	m.size = 0
}

// Get the number of entries in the Memtable. Includes tombstones.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.data.Len()
}

// Generates serializable list of memtable entries in sorted order for SSTable.
func (m *Memtable) GetEntries() []*MemtableEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*MemtableEntry
	iter := m.data.Front()
	for iter != nil {
		results = append(results, iter.Value.(*MemtableEntry))
		iter = iter.Next()
	}

	return results
}

func getMemtableEntry(key string, value *[]byte, command Command) *MemtableEntry {
	entry := &MemtableEntry{
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
	}
	if value != nil {
		entry.Value = *value
	}
	return entry
}
