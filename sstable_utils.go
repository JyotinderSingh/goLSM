package golsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

// Generate a bloom filter, index, and entries buffer from a list of Memtable entries.
func buildMetadataAndEntriesBuffer(messages []*LSMEntry) (*BloomFilter, *Index, *bytes.Buffer, error) {
	var index []*IndexEntry
	var bloomFilter *BloomFilter = newBloomFilter(1000000)
	var currentOffset EntrySize = 0
	entriesBuffer := &bytes.Buffer{}

	for _, message := range messages {
		data := mustMarshal(message)
		entrySize := EntrySize(len(data))

		// Add the entry to the index and bloom filter.
		index = append(index, &IndexEntry{Key: message.Key, Offset: int64(currentOffset)})
		bloomFilter.Add([]byte(message.Key))

		if err := binary.Write(entriesBuffer, binary.LittleEndian, int64(entrySize)); err != nil {
			return nil, nil, nil, err
		}
		if _, err := entriesBuffer.Write(data); err != nil {
			return nil, nil, nil, err
		}

		currentOffset += EntrySize(binary.Size(entrySize)) + EntrySize(entrySize)
	}

	return bloomFilter, &Index{Entries: index}, entriesBuffer, nil
}

// Binary search for the offset of the key in the index.
func findOffsetForKey(index []*IndexEntry, key string) (EntrySize, bool) {
	low := 0
	high := len(index) - 1
	for low <= high {
		mid := (low + high) / 2
		if index[mid].Key == key {
			return EntrySize(index[mid].Offset), true
		} else if index[mid].Key < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return 0, false
}

// Find Start offset for a range scan, inclusive of startKey.
// This is the smallest key >= startKey. Performs a binary search on the index.
func findStartOffsetForRangeScan(index []*IndexEntry, startKey string) (EntrySize, bool) {
	low := 0
	high := len(index) - 1
	for low <= high {
		mid := (low + high) / 2
		if index[mid].Key == startKey {
			return EntrySize(index[mid].Offset), true
		} else if index[mid].Key < startKey {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	// If the key is not found, low will be the index of the smallest key > startKey.
	if low >= len(index) {
		return 0, false
	}

	return EntrySize(index[low].Offset), true
}

// Read the size of the entry from the file.
func readDataSize(file *os.File) (EntrySize, error) {
	var size EntrySize
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return 0, err
	}
	return size, nil
}

// Read a single entry from the file.
func readEntryDataFromFile(file *os.File, size EntrySize) ([]byte, error) {
	data := make([]byte, size)
	if _, err := file.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}

// Write the bloom filter, index, and entries to the SSTable file. Returns the
// offset of the entries data.
func writeSSTable(filename string, bloomFilterData []byte, indexData []byte, entriesBuffer io.Reader) (EntrySize, error) {
	file, err := os.Create(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var dataOffset EntrySize = 0

	// Write the bloom filter size.
	if err := binary.Write(file, binary.LittleEndian, EntrySize(len(bloomFilterData))); err != nil {
		return 0, err
	}
	dataOffset += EntrySize(binary.Size(EntrySize(len(bloomFilterData))))

	// Write the bloom filter data.
	if _, err := file.Write(bloomFilterData); err != nil {
		return 0, err
	}
	dataOffset += EntrySize(len(bloomFilterData))

	// Write the index size.
	if err := binary.Write(file, binary.LittleEndian, EntrySize(len(indexData))); err != nil {
		return 0, err
	}
	dataOffset += EntrySize(binary.Size(EntrySize(len(indexData))))

	// Write the index data.
	if _, err := file.Write(indexData); err != nil {
		return 0, err
	}
	dataOffset += EntrySize(len(indexData))

	// Write the entries.
	if _, err := io.Copy(file, entriesBuffer); err != nil {
		return 0, err
	}

	return dataOffset, nil
}

// Read the bloom filter, index, and data offset from the SSTable file.
func readSSTableMetadata(file *os.File) (*BloomFilter, *Index, EntrySize, error) {
	var dataOffset EntrySize = 0

	// Read the bloom filter size.
	bloomFilterSize, err := readDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += EntrySize(binary.Size(bloomFilterSize))

	// Read the bloom filter data.
	bloomFilterData := make([]byte, bloomFilterSize)
	bytesRead, err := file.Read(bloomFilterData)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += EntrySize(bytesRead)

	// Read the index size.
	indexSize, err := readDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += EntrySize(binary.Size(indexSize))

	// Read the index data.
	indexData := make([]byte, indexSize)
	bytesRead, err = file.Read(indexData)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += EntrySize(bytesRead)

	// Unmarshal the bloom filter and index.
	bloomFilter := &BloomFilter{}
	mustUnmarshal(bloomFilterData, bloomFilter)
	index := &Index{}
	mustUnmarshal(indexData, index)

	return bloomFilter, index, dataOffset, nil
}
