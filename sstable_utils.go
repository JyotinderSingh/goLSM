package golsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

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

	if err := binary.Write(file, binary.LittleEndian, int64(len(indexData))); err != nil {
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

	if _, err := file.Seek(offset, io.SeekCurrent); err != nil {
		return nil, err
	}

	size, err := readEntrySizeFromFile(file)
	if err != nil {
		return nil, err
	}

	data, err := readEntryDataFromFile(file, size)
	if err != nil {
		return nil, err
	}

	entry := &MemtableKeyValue{}
	mustUnmarshal(data, entry)

	if entry.GetValue().GetCommand() == Command_DELETE {
		return nil, nil
	}

	return entry.GetValue().GetValue(), nil
}

func buildIndexAndEntriesBuffer(messages []*MemtableKeyValue) ([]*IndexEntry, *bytes.Buffer, error) {
	var index []*IndexEntry
	var currentOffset int64 = 0
	entriesBuffer := &bytes.Buffer{}

	for _, message := range messages {
		data := mustMarshal(message)
		entrySize := int64(len(data))

		index = append(index, &IndexEntry{Key: message.Key, Offset: currentOffset})

		if err := binary.Write(entriesBuffer, binary.LittleEndian, int64(entrySize)); err != nil {
			return nil, nil, err
		}
		if _, err := entriesBuffer.Write(data); err != nil {
			return nil, nil, err
		}

		currentOffset += int64(binary.Size(entrySize)) + int64(entrySize)
	}

	return index, entriesBuffer, nil
}

func readIndexFromFile(file *os.File) ([]*IndexEntry, error) {
	var indexSize int64
	if err := binary.Read(file, binary.LittleEndian, &indexSize); err != nil {
		return nil, err
	}

	indexData := make([]byte, indexSize)
	if _, err := file.Read(indexData); err != nil {
		return nil, err
	}

	index := &Index{}
	mustUnmarshal(indexData, index)

	return index.Entries, nil
}

func findOffsetForKey(index []*IndexEntry, key string) (int64, bool) {
	for _, entry := range index {
		if entry.Key == key {
			return entry.Offset, true
		}
	}
	return 0, false
}

func readEntrySizeFromFile(file *os.File) (int64, error) {
	var size int64
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return 0, err
	}
	return size, nil
}

func readEntryDataFromFile(file *os.File, size int64) ([]byte, error) {
	data := make([]byte, size)
	if _, err := file.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}
