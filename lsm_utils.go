package golsm

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

// Checks if the given entry is a tombstone. Returns the value itself if not,
// returns nil if it is.
func handleValue(value *MemtableEntry) ([]byte, error) {
	if value.Command == Command_DELETE {
		return nil, nil
	}
	return value.Value, nil
}
