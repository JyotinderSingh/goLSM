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
