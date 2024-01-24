package tests

import (
	"os"
	"testing"

	golsm "github.com/JyotinderSingh/go-lsm"
	"github.com/stretchr/testify/assert"
)

// Perform put and delete opertaions on the LSM tree and verify the results.
// Then write out the memtable to an SSTable and verify the contents of the
// SSTable.
func TestSSTableUtils(t *testing.T) {
	// Create a new LSM memtable.
	memtable := golsm.NewMemtable()

	// Put some keys.
	memtable.Put("key1", []byte("value1"))
	memtable.Put("key2", []byte("value2"))
	memtable.Put("key3", []byte("value3"))
	memtable.Put("key4", []byte("value4"))
	memtable.Put("key5", []byte("value5"))

	// Delete some keys.
	memtable.Delete("key2")
	memtable.Delete("key4")

	// Verify the contents of the tree.
	assert.Equal(t, []byte("value1"), memtable.Get("key1"))
	assert.Equal(t, []byte("value3"), memtable.Get("key3"))
	assert.Equal(t, []byte("value5"), memtable.Get("key5"))
	assert.Equal(t, []byte(nil), memtable.Get("key2"))
	assert.Equal(t, []byte(nil), memtable.Get("key4"))

	testFileName := "test.sst"
	defer os.Remove(testFileName)
	// Write the memtable to an SSTable.
	err := golsm.WriteSSTableFromMemtable(memtable.GetSerializableEntries(), testFileName)
	assert.Nil(t, err)

	// Read the SSTable and verify the contents.
	entry, err := golsm.ReadEntryForKey(testFileName, "key1")
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), entry)

	entry, err = golsm.ReadEntryForKey(testFileName, "key3")
	assert.Nil(t, err)
	assert.Equal(t, []byte("value3"), entry)

	// Read deleted entry.
	entry, err = golsm.ReadEntryForKey(testFileName, "key2")
	assert.Nil(t, err)
	assert.Equal(t, []byte(nil), entry)
}
