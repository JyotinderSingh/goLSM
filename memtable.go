package golsm

import (
	"time"

	"github.com/huandu/skiplist"
)

// In-memory table that supports writes, reads, deletes, and range scans.
type Memtable struct {
	data skiplist.SkipList
	size int64 // Size of the Memtable in bytes.
}

// Create a new Memtable.
func NewMemtable() *Memtable {
	return &Memtable{
		data: *skiplist.New(skiplist.String),
		size: 0,
	}
}

// Insert a key-value pair into the Memtable. Not thread-safe.
func (m *Memtable) Put(key string, value []byte) {
	sizeChange := int64(len(value))
	existingEntry := m.data.Get(key)
	if existingEntry != nil {
		// If the entry already exists, we need to account for the size of the
		// old value.
		m.size -= int64(len(existingEntry.Value.(*LSMEntry).Value))
	} else {
		// If the entry doesn't exist, we need to account for the size of the key.
		sizeChange += int64(len((key)))
	}

	// Update with the new entry.
	entry := getLSMEntry(key, &value, Command_PUT)
	m.data.Set(key, entry)
	m.size += sizeChange
}

// Delete a key-value pair from the Memtable. Not thread-safe.
func (m *Memtable) Delete(key string) {
	// If the entry already exists, we need to account for the size of the
	// old value.
	existingEntry := m.data.Get(key)
	if existingEntry != nil {
		// Only subtract the size of the existing value, not the key,
		// since the key remains in the memtable with a tombstone marker
		m.size -= int64(len(existingEntry.Value.(*LSMEntry).Value))
	} else {
		// If the entry doesn't exist, we need to account for the size of the key.
		m.size += int64(len(key))
	}

	m.data.Set(key, getLSMEntry(key, nil, Command_DELETE))
}

// Retrieve a value from the Memtable. Not thread-safe.
func (m *Memtable) Get(key string) *LSMEntry {
	value := m.data.Get(key)

	if value == nil {
		return nil
	}

	// We need to include the tombstones in the range scan. The caller will
	// need to check the Command field of the LSMEntry to determine if
	// the entry is a tombstone.
	return value.Value.(*LSMEntry)
}

// Range scan the Memtable, inclusive of startKey and endKey. Not thread-safe.
func (m *Memtable) RangeScan(startKey string, endKey string) []*LSMEntry {

	var results []*LSMEntry
	// Find the first key that is >= startKey and use the FindNext() method to
	// iterate through the rest of the keys.
	iter := m.data.Find(startKey)
	for iter != nil {
		if iter.Element().Key().(string) > endKey {
			break
		}

		// We need to include the tombstones in the range scan. The caller will
		// need to check the Command field of the LSMEntry to determine if
		// the entry is a tombstone.
		results = append(results, iter.Value.(*LSMEntry))
		iter = iter.Next()
	}

	return results
}

// Get the size of the Memtable in bytes. Not thread-safe.
func (m *Memtable) SizeInBytes() int64 {
	return m.size
}

// Clears the Memtable.
func (m *Memtable) Clear() {
	m.data.Init()
	m.size = 0
}

// Get the number of entries in the Memtable. Includes tombstones. Not thread-safe.
func (m *Memtable) Len() int {
	return m.data.Len()
}

// Generates serializable list of memtable entries in sorted order for SSTable. Not thread-safe.
func (m *Memtable) GetEntries() []*LSMEntry {
	var results []*LSMEntry
	iter := m.data.Front()
	for iter != nil {
		results = append(results, iter.Value.(*LSMEntry))
		iter = iter.Next()
	}

	return results
}

func getLSMEntry(key string, value *[]byte, command Command) *LSMEntry {
	entry := &LSMEntry{
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
	}
	if value != nil {
		entry.Value = *value
	}
	return entry
}
