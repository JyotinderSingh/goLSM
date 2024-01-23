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
