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
func TestSSTable(t *testing.T) {
	t.Parallel()
	testFileName := "TestSSTable.sst"
	defer os.Remove(testFileName)
	// Create a new LSM memtable.
	memtable := golsm.NewMemtable()

	populateMemtableWithTestData(memtable)

	// Verify the contents of the tree.
	assert.Equal(t, []byte("value1"), memtable.Get("key1"))
	assert.Equal(t, []byte("value3"), memtable.Get("key3"))
	assert.Equal(t, []byte("value5"), memtable.Get("key5"))
	assert.Equal(t, []byte(nil), memtable.Get("key2"))
	assert.Equal(t, []byte(nil), memtable.Get("key4"))

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

	// Read non-existent entry.
	entry, err = golsm.ReadEntryForKey(testFileName, "key6")
	assert.Nil(t, err)
	assert.Equal(t, []byte(nil), entry)
}

// Test RangeScan on an SSTable.
func TestRangeScan(t *testing.T) {
	t.Parallel()
	testFileName := "TestRangeScan.sst"
	defer os.Remove(testFileName)
	// Create a new LSM memtable.
	memtable := golsm.NewMemtable()

	populateMemtableWithTestData(memtable)

	// Write the memtable to an SSTable.
	err := golsm.WriteSSTableFromMemtable(memtable.GetSerializableEntries(), testFileName)
	assert.Nil(t, err)

	// Range scan the SSTable.
	entries, err := golsm.RangeScan(testFileName, "key1", "key5")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(entries))
	assert.Equal(t, []byte("value1"), entries[0])
	assert.Equal(t, []byte("value3"), entries[1])
	assert.Equal(t, []byte("value5"), entries[2])
}

// Test RangeScan on an SSTable with a non-existent Range.
func TestRangeScanNonExistentRange(t *testing.T) {
	t.Parallel()
	testFileName := "TestRangeScanNonExistentRange.sst"
	defer os.Remove(testFileName)
	// Create a new LSM memtable.
	memtable := golsm.NewMemtable()

	populateMemtableWithTestData(memtable)

	// Write the memtable to an SSTable.
	err := golsm.WriteSSTableFromMemtable(memtable.GetSerializableEntries(), testFileName)
	assert.Nil(t, err)

	// Range scan the SSTable.
	entries, err := golsm.RangeScan(testFileName, "key6", "key7")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(entries))
}

// Test RangeScan on an SSTable with non-exact Range.
func TestRangeScanNonExactRange1(t *testing.T) {
	t.Parallel()
	testFileName := "TestRangeScanNonExactRange1.sst"
	defer os.Remove(testFileName)
	// Create a new LSM memtable.
	memtable := golsm.NewMemtable()

	populateMemtableWithTestData(memtable)

	// Write the memtable to an SSTable.
	err := golsm.WriteSSTableFromMemtable(memtable.GetSerializableEntries(), testFileName)
	assert.Nil(t, err)

	// Range scan the SSTable.
	entries, err := golsm.RangeScan(testFileName, "a", "z")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(entries))
	assert.Equal(t, []byte("value1"), entries[0])
	assert.Equal(t, []byte("value3"), entries[1])
	assert.Equal(t, []byte("value5"), entries[2])
}

// Test RangeScan on an SSTable with non-exact Range.
func TestRangeScanNonExactRange2(t *testing.T) {
	t.Parallel()
	testFileName := "TestRangeScanNonExactRange2.sst"
	defer os.Remove(testFileName)
	// Create a new LSM memtable.
	memtable := golsm.NewMemtable()

	populateMemtableWithTestData(memtable)

	// Write the memtable to an SSTable.
	err := golsm.WriteSSTableFromMemtable(memtable.GetSerializableEntries(), testFileName)
	assert.Nil(t, err)

	// Range scan the SSTable.
	entries, err := golsm.RangeScan(testFileName, "z", "za")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(entries))
}

func populateMemtableWithTestData(memtable *golsm.Memtable) {
	memtable.Put("key1", []byte("value1"))
	memtable.Put("key2", []byte("value2"))
	memtable.Put("key3", []byte("value3"))
	memtable.Put("key4", []byte("value4"))
	memtable.Put("key5", []byte("value5"))

	memtable.Delete("key2")
	memtable.Delete("key4")
}
