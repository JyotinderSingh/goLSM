package golsm

// Returns true if the filename is an SSTable file. Checks the prefix of the
// filename.
func isSSTableFile(filename string) bool {
	return filename[:len(SSTableFilePrefix)] == SSTableFilePrefix
}

// Checks if the given entry is a tombstone. Returns the value itself if not,
// returns nil if it is.
func handleValue(value *LSMEntry) ([]byte, error) {
	if value.Command == Command_DELETE {
		return nil, nil
	}
	return value.Value, nil
}
