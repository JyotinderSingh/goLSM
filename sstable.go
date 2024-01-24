package golsm

import (
	"encoding/binary"
	"io"
	"os"
)

// Alias for int64 size
type OffsetSize int64

// Writes a list of MemtableKeyValue to a file in SSTable format.
// Format of the file is:
// 1. Size of the index (OffsetSize)
// 2. Index data (Index Protobuf)
// 3. Entries data (MemtableKeyValue Protobuf)
//
// The entries data is written in as:
// 1. Size of the entry (OffsetSize)
// 2. Entry data (MemtableKeyValue Protobuf)
//
// The index is a list of IndexEntry, which is a struct containing the key and
// the offset of the entry in the file (after the index).
func WriteSSTableFromMemtable(messages []*MemtableKeyValue, filename string) error {
	index, entriesBuffer, err := buildIndexAndEntriesBuffer(messages)
	if err != nil {
		return err
	}

	indexData := mustMarshal(&Index{Entries: index})
	if err != nil {
		return err
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.LittleEndian, OffsetSize(len(indexData))); err != nil {
		return err
	}
	if _, err := file.Write(indexData); err != nil {
		return err
	}
	if _, err := io.Copy(file, entriesBuffer); err != nil {
		return err
	}

	return nil
}

// Reads the value for a given key from the SSTable. Returns nil if the key is
// not found.
func ReadEntryForKey(filename string, key string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	index, err := readIndexFromFile(file)
	if err != nil {
		return nil, err
	}

	offset, found := findOffsetForKey(index, key)
	if !found {
		return nil, nil
	}

	if _, err := file.Seek(int64(offset), io.SeekCurrent); err != nil {
		return nil, err
	}

	size, err := readOffsetSize(file)
	if err != nil {
		return nil, err
	}

	data, err := readEntryDataFromFile(file, size)
	if err != nil {
		return nil, err
	}

	entry := &MemtableKeyValue{}
	mustUnmarshal(data, entry)

	// If the entry is a tombstone, return nil
	if entry.GetValue().GetCommand() == Command_DELETE {
		return nil, nil
	}

	return entry.GetValue().GetValue(), nil
}

// RangeScan returns all the values in the SSTable between startKey and endKey
// inclusive.
func RangeScan(filename string, startKey string, endKey string) ([][]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	index, err := readIndexFromFile(file)
	if err != nil {
		return nil, err
	}

	startOffset, found := findStartOffsetForRangeScan(index, startKey)
	if !found {
		return nil, nil
	}

	if _, err := file.Seek(int64(startOffset), io.SeekCurrent); err != nil {
		return nil, err
	}

	var results [][]byte
	for {
		size, err := readOffsetSize(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		data, err := readEntryDataFromFile(file, size)
		if err != nil {
			return nil, err
		}

		entry := &MemtableKeyValue{}
		mustUnmarshal(data, entry)

		if entry.Key > endKey {
			break
		}

		if entry.GetValue().GetCommand() == Command_DELETE {
			continue
		}

		results = append(results, entry.GetValue().GetValue())
	}

	return results, nil
}
