package golsm

import "time"

func getMemtableEntry(key string, value *[]byte, command Command) *MemtableEntry {
	entry := &MemtableEntry{
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
	}
	if value != nil {
		entry.Value = *value
	}
	return entry
}
