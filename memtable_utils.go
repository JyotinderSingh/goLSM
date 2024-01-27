package golsm

import "time"

func getMemtableEntry(value *[]byte, command Command) *MemtableEntry {
	entry := &MemtableEntry{
		Command:   command,
		Timestamp: time.Now().Unix(),
	}
	if value != nil {
		entry.Value = *value
	}
	return entry
}
