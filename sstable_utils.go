package golsm

import (
	"bytes"
	"encoding/binary"
	"os"
)

func buildIndexAndEntriesBuffer(messages []*MemtableKeyValue) (*Index, *bytes.Buffer, error) {
	var index []*IndexEntry
	var currentOffset OffsetSize = 0
	entriesBuffer := &bytes.Buffer{}

	for _, message := range messages {
		data := mustMarshal(message)
		entrySize := OffsetSize(len(data))

		index = append(index, &IndexEntry{Key: message.Key, Offset: int64(currentOffset)})

		if err := binary.Write(entriesBuffer, binary.LittleEndian, int64(entrySize)); err != nil {
			return nil, nil, err
		}
		if _, err := entriesBuffer.Write(data); err != nil {
			return nil, nil, err
		}

		currentOffset += OffsetSize(binary.Size(entrySize)) + OffsetSize(entrySize)
	}

	return &Index{Entries: index}, entriesBuffer, nil
}

// Binary search for the offset of the key in the index.
func findOffsetForKey(index []*IndexEntry, key string) (OffsetSize, bool) {
	low := 0
	high := len(index) - 1
	for low <= high {
		mid := (low + high) / 2
		if index[mid].Key == key {
			return OffsetSize(index[mid].Offset), true
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
func findStartOffsetForRangeScan(index []*IndexEntry, startKey string) (OffsetSize, bool) {
	low := 0
	high := len(index) - 1
	for low <= high {
		mid := (low + high) / 2
		if index[mid].Key == startKey {
			return OffsetSize(index[mid].Offset), true
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

	return OffsetSize(index[low].Offset), true
}

// Read the size of the entry from the file.
func readOffsetSize(file *os.File) (OffsetSize, error) {
	var size OffsetSize
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return 0, err
	}
	return size, nil
}

func readEntryDataFromFile(file *os.File, size OffsetSize) ([]byte, error) {
	data := make([]byte, size)
	if _, err := file.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}
