package golsm

import (
	"os"
	"sort"
)

func getMemtableEntry(value *[]byte, command Command) *MemtableEntry {
	entry := &MemtableEntry{
		Command: command,
	}
	if value != nil {
		entry.Value = *value
	}
	return entry
}

// Returns true if the filename is an SSTable file. Checks the prefix of the
// filename.
func isSSTableFile(filename string) bool {
	return filename[:len(SSTableFilePrefix)] == SSTableFilePrefix
}

func (l *LSMTree) loadSSTables() error {
	// Create directory if it doesn't exist.
	if err := os.MkdirAll(l.directory, 0755); err != nil {
		return err
	}

	// Load SSTables from disk.
	// Read all the files in the directory.
	files, err := os.ReadDir(l.directory)
	if err != nil {
		return err
	}

	// Sort the files by name.
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	// Open all the SSTables and load their handles into memory.
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if !isSSTableFile(file.Name()) {
			continue
		}

		sstable, err := OpenSSTable(l.directory + "/" + file.Name())
		if err != nil {
			return err
		}
		l.sstables = append(l.sstables, sstable)
	}
	return nil
}
