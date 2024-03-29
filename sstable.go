package golsm

import (
	"io"
	"os"
)

// Alias for int64 size
type EntrySize int64

type SSTable struct {
	bloomFilter *BloomFilter // Bloom filter for the SSTable
	index       *Index       // Index of the SSTable
	file        *os.File     // File handle for the on-disk SSTable file.
	dataOffset  EntrySize    // Offset from where the actual entries begin
}

type SSTableIterator struct {
	s     *SSTable  // Pointer to the associated SSTable
	file  *os.File  // File handle for the on-disk SSTable file.
	Value *LSMEntry // Current entry
}

// Writes a list of MemtableKeyValue to a file in SSTable format.
// Format of the file is:
// 1. Bloom filter size (OffsetSize)
// 2. Bloom filter data (BloomFilter Protobuf)
// 3. Index size (OffsetSize)
// 4. Index data (Index Protobuf)
// 5. Entries data
//
// The entries data is written in as:
// 1. Size of the entry (OffsetSize)
// 2. Entry data (LSMEntry Protobuf)
func SerializeToSSTable(messages []*LSMEntry, filename string) (*SSTable, error) {
	bloomFilter, index, entriesBuffer, err := buildMetadataAndEntriesBuffer(messages)
	if err != nil {
		return nil, err
	}

	indexData := mustMarshal(index)
	bloomFilterData := mustMarshal(bloomFilter)

	dataOffset, err := writeSSTable(filename, bloomFilterData, indexData, entriesBuffer)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &SSTable{bloomFilter: bloomFilter, index: index, file: file, dataOffset: dataOffset}, nil
}

// Opens an SSTable file for reading and returns a handle to it.
func OpenSSTable(filename string) (*SSTable, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	bloomFilter, index, dataOffset, err := readSSTableMetadata(file)
	if err != nil {
		return nil, err
	}

	return &SSTable{bloomFilter: bloomFilter, index: index, file: file, dataOffset: dataOffset}, nil
}

func (s *SSTable) Close() error {
	return s.file.Close()
}

// Reads the value for a given key from the SSTable. Returns nil if the key is
// not found.
func (s *SSTable) Get(key string) (*LSMEntry, error) {
	// Check if the key is in the bloom filter. If it is not, we can return
	// immediately.
	if !s.bloomFilter.Test([]byte(key)) {
		return nil, nil
	}

	offset, found := findOffsetForKey(s.index.Entries, key)
	if !found {
		return nil, nil
	}

	// Seek to the offset of the entry in the file. The offset is relative to the
	// start of the entries data therefore we add the dataOffset to it.
	if _, err := s.file.Seek(int64(offset)+int64(s.dataOffset), io.SeekStart); err != nil {
		return nil, err
	}

	size, err := readDataSize(s.file)
	if err != nil {
		return nil, err
	}

	data, err := readEntryDataFromFile(s.file, size)
	if err != nil {
		return nil, err
	}

	entry := &LSMEntry{}
	mustUnmarshal(data, entry)

	// We need to include the tombstones in the range scan. The caller will
	// need to check the Command field of the LSMEntry to determine if
	// the entry is a tombstone.
	return entry, nil
}

// RangeScan returns all the values in the SSTable between startKey and endKey
// inclusive.
func (s *SSTable) RangeScan(startKey string, endKey string) ([]*LSMEntry, error) {
	startOffset, found := findStartOffsetForRangeScan(s.index.Entries, startKey)
	if !found {
		return nil, nil
	}

	if _, err := s.file.Seek(int64(startOffset)+int64(s.dataOffset), io.SeekStart); err != nil {
		return nil, err
	}

	var results []*LSMEntry
	for {
		size, err := readDataSize(s.file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		data, err := readEntryDataFromFile(s.file, size)
		if err != nil {
			return nil, err
		}

		entry := &LSMEntry{}
		mustUnmarshal(data, entry)

		if entry.Key > endKey {
			break
		}

		// We need to include the tombstones in the range scan. The caller will
		// need to check the Command field of the LSMEntry to determine if
		// the entry is a tombstone.
		results = append(results, entry)
	}

	return results, nil
}

// GetEntries returns all the values in the SSTable.
func (s *SSTable) GetEntries() ([]*LSMEntry, error) {
	if _, err := s.file.Seek(int64(s.dataOffset), io.SeekStart); err != nil {
		return nil, err
	}

	var results []*LSMEntry
	for {
		size, err := readDataSize(s.file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		data, err := readEntryDataFromFile(s.file, size)
		if err != nil {
			return nil, err
		}

		entry := &LSMEntry{}
		mustUnmarshal(data, entry)

		// We need to include the tombstones in the range scan. The caller will
		// need to check the Command field of the LSMEntry to determine if
		// the entry is a tombstone.
		results = append(results, entry)
	}

	return results, nil
}

// Returns an iterator for the SSTable. The iterator is positioned at the
// beginning of the SSTable.
func (s *SSTable) Front() *SSTableIterator {
	// Open a new file handle for the iterator.
	file, err := os.Open(s.file.Name())
	if err != nil {
		return nil
	}
	i := &SSTableIterator{s: s, file: file, Value: &LSMEntry{}}

	if _, err := i.file.Seek(int64(i.s.dataOffset), io.SeekStart); err != nil {
		panic(err)
	}

	size, err := readDataSize(i.file)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}

	data, err := readEntryDataFromFile(i.file, size)
	if err != nil {
		panic(err)
	}

	mustUnmarshal(data, i.Value)

	return i
}

// Returns the next entry in the SSTable. Returns nil if there are no more
// entries.
func (i *SSTableIterator) Next() *SSTableIterator {
	size, err := readDataSize(i.file)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}

	data, err := readEntryDataFromFile(i.file, size)
	if err != nil {
		panic(err)
	}

	i.Value = &LSMEntry{}
	mustUnmarshal(data, i.Value)

	return i
}

// Closes the iterator.
func (i *SSTableIterator) Close() error {
	return i.file.Close()
}
