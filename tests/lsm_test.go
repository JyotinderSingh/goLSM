package tests

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	golsm "github.com/JyotinderSingh/golsm"
	"github.com/stretchr/testify/assert"
)

func TestLSMTreePut(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreePut"
	l, err := golsm.Open(dir, 1000, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	// Put a key-value pair into the LSMTree.
	err = l.Put("key", []byte("value"))
	assert.Nil(t, err)

	// Check that the key-value pair exists in the LSMTree.
	value, err := l.Get("key")
	assert.Nil(t, err)
	assert.Equal(t, "value", string(value), "Expected value to be 'value', got '%v'", string(value))

	// Delete the key-value pair from the LSMTree.
	err = l.Delete("key")
	assert.Nil(t, err)

	// Check that the key-value pair no longer exists in the LSMTree.
	value, err = l.Get("key")
	assert.Nil(t, err)
	assert.Nil(t, value, "Expected value to be nil, got '%v'", string(value))

	l.Close()

	// Check that the key-value pair still exists in the LSMTree after closing and
	// reopening it.
	l, err = golsm.Open(dir, 1000, true)
	assert.Nil(t, err)

	value, err = l.Get("key")
	assert.Nil(t, err)
	assert.Nil(t, value, "Expected value to be nil, got '%v'", string(value))

	l.Close()
}

// Write a thousand key-value pairs to the LSMTree and check that they all exist.
func TestLSMTreePutMany(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreePutMany"
	l, err := golsm.Open(dir, 300, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	defer l.Close()

	for i := 0; i < 10000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)

	}

	for i := 0; i < 10000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}

	// time.Sleep(5 * time.Second)
}

// Checks SSTable loading on startup.
// 1. Write 1000 key-value pairs to the LSMTree.
// 2. Close the LSMTree.
// 3. Open the LSMTree again.
// 4. Check that the key-value pairs exist.
// 5. Write another 1000 key-value pairs to the LSMTree.
// 6. Close the LSMTree.
// 7. Open the LSMTree again.
// 8. Check that all 2000 key-value pairs exist.
func TestLSMTreeSSTableLoading(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreeSSTableLoading"
	l, err := golsm.Open(dir, 100, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	for i := 0; i < 1000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
	}

	// Check that the key-value pairs exist.
	for i := 0; i < 1000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}

	l.Close()

	l, err = golsm.Open(dir, 100, true)
	assert.Nil(t, err)

	for i := 0; i < 1000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}

	for i := 1000; i < 2000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
	}

	// Check that the key-value pairs exist.
	for i := 0; i < 2000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}

	// Delete range 400-600.
	for i := 400; i < 600; i++ {
		err := l.Delete(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
	}

	// Check that these key-value pairs no longer exist.
	for i := 400; i < 600; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Nil(t, value, "Expected value to be nil, got '%v'", string(value))
	}

	l.Close()

	l, err = golsm.Open(dir, 100, true)
	assert.Nil(t, err)

	for i := 0; i < 2000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		if i >= 400 && i < 600 {
			assert.Nil(t, value, "Expected value to be nil, got '%v'", string(value))
		} else {
			assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
		}
	}

	l.Close()

}

// Stress test to check that the LSMTree can handle a large number of reads and
// writes with a small memtable size.
func TestLSMTreeStressTest(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreeStressTest"
	l, err := golsm.Open(dir, 32000, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	for i := 0; i < 100000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
	}

	l.Close()

	l, err = golsm.Open(dir, 32000, true)
	assert.Nil(t, err)

	for i := 0; i < 100000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}
}

// Concurrently write a large number of writes to the LSMTree from multiple
// goroutines.
func TestLSMTreeConcurrentWrites(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreeConcurrentWrites"
	l, err := golsm.Open(dir, 32000, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(i int) {
			for j := 0; j < 1000; j++ {
				err := l.Put(fmt.Sprintf("%d%d", i, j), []byte(fmt.Sprintf("%d%d", i, j)))
				assert.Nil(t, err)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	l.Close()

	l, err = golsm.Open(dir, 32000, true)
	assert.Nil(t, err)

	// Read all the values back.
	for i := 0; i < 100; i++ {
		for j := 0; j < 1000; j++ {
			value, err := l.Get(fmt.Sprintf("%d%d", i, j))
			assert.Nil(t, err)
			assert.Equal(t, fmt.Sprintf("%d%d", i, j), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d%d", i, j), string(value))
		}
	}
}

// Test updates to the same key. Writes 1000 key-value pairs to the LSMTree,
// then updates them all with new values.
func TestLSMTreeUpdate(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreeUpdate"
	l, err := golsm.Open(dir, 200, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	// Write 1000 key-value pairs to the LSMTree.
	for i := 0; i < 1000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
	}

	// Update all the key-value pairs.
	for i := 0; i < 1000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d%d", i, i)))
		assert.Nil(t, err)
	}

	// Check that the key-value pairs exist.
	for i := 0; i < 1000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d%d", i, i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d%d", i, i), string(value))
	}

	// Close and reopen the LSMTree to ensure that the key-value pairs are
	// persisted correctly.
	l.Close()
	l, err = golsm.Open(dir, 32000, true)
	assert.Nil(t, err)

	// Check that the key-value pairs still exist.
	for i := 0; i < 1000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d%d", i, i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d%d", i, i), string(value))
	}

	l.Close()
}

// Test RangeScan on the LSMTree. Writes key-value pairs to the LSMTree and then
// performs a RangeScan to check that they are all returned.
// Then updates some key-value pairs and performs another RangeScan to check that
// the updated values are returned.
// Then deletes some other key-value pairs and performs another RangeScan to check that
// they are no longer returned.
func TestLSMTreeRangeScan(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreeRangeScan"
	l, err := golsm.Open(dir, 16, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	// Write alphabet key-value pairs to the LSMTree.
	for i := 0; i < 26; i++ {
		err := l.Put(fmt.Sprintf("%c", 'a'+i), []byte(fmt.Sprintf("%c", 'a'+i)))
		assert.Nil(t, err)
	}

	// Check that the key-value pairs exist.
	for i := 0; i < 26; i++ {
		value, err := l.Get(fmt.Sprintf("%c", 'a'+i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%c", 'a'+i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%c", 'a'+i), string(value))
	}

	// Perform a RangeScan to check that all the key-value pairs are returned.
	values, err := l.RangeScan("a", "z")
	assert.Nil(t, err)
	assert.Equal(t, 26, len(values), "Expected 26 values, got %v", len(values))

	// Update some key-value pairs.
	for i := 0; i < 26; i++ {
		err := l.Put(fmt.Sprintf("%c", 'a'+i), []byte(fmt.Sprintf("%c%c", 'a'+i, 'a'+i)))
		assert.Nil(t, err)
	}

	// Perform a RangeScan to check that all the updated key-value pairs are returned.
	values, err = l.RangeScan("a", "z")
	assert.Nil(t, err)
	assert.Equal(t, 26, len(values), "Expected 26 values, got %v", len(values))

	// Validate the entries
	for i := 0; i < 26; i++ {
		assert.Equal(t, fmt.Sprintf("%c%c", 'a'+i, 'a'+i), string(values[i].Value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%c%c", 'a'+i, 'a'+i), string(values[i].Value))
	}

	// Perform a RangeScan on a subset of the key-value pairs.
	values, err = l.RangeScan("a", "m")
	assert.Nil(t, err)
	assert.Equal(t, 13, len(values), "Expected 13 values, got %v", len(values))
	for i := 0; i < 13; i++ {
		assert.Equal(t, fmt.Sprintf("%c%c", 'a'+i, 'a'+i), string(values[i].Value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%c%c", 'a'+i, 'a'+i), string(values[i].Value))
	}

	l.Close()

	// Check that the key-value pairs still exist after closing and reopening the LSMTree.
	l, err = golsm.Open(dir, 16, true)
	assert.Nil(t, err)

	values, err = l.RangeScan("c", "x")
	assert.Nil(t, err)
	assert.Equal(t, 22, len(values), "Expected 22 values, got %v", len(values))
	for i := 0; i < 22; i++ {
		assert.Equal(t, fmt.Sprintf("%c%c", 'a'+i+2, 'a'+i+2), string(values[i].Value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%c%c", 'a'+i+2, 'a'+i+2), string(values[i].Value))
	}

	// Delete some key-value pairs.
	for i := 0; i < 26; i++ {
		err := l.Delete(fmt.Sprintf("%c", 'a'+i))
		assert.Nil(t, err)

	}

	// Perform a RangeScan to check that the deleted key-value pairs are not returned.
	values, err = l.RangeScan("a", "z")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(values), "Expected 0 values, got %v", len(values))

	l.Close()

	// Check that the key-value pairs still exist after closing and reopening the LSMTree.
	l, err = golsm.Open(dir, 16, true)
	assert.Nil(t, err)

	// Perform a RangeScan to check that the deleted key-value pairs are not returned.
	values, err = l.RangeScan("a", "z")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(values), "Expected 0 values, got %v", len(values))
}

// TestWALRecovery tests that the LSMTree can recover from a crash.
func TestWALRecovery(t *testing.T) {
	t.Parallel()
	dir := "TestWALRecovery"
	l, err := golsm.Open(dir, 3200, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	// Write 1000 key-value pairs to the LSMTree.
	for i := 0; i < 1000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
	}

	l.Close()

	l, err = golsm.Open(dir, 64000, true)
	assert.Nil(t, err)

	// Check that the key-value pairs exist.
	for i := 0; i < 1000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}

	// Write another 1000 key-value pairs to the LSMTree.
	for i := 1000; i < 2000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
	}

	// Give the WAL time to flush. Test becomes flaky if this is not done.
	time.Sleep(200 * time.Millisecond)

	// Suddenly crash the LSMTree. We simulate this by initializing the LSMTree
	// with a new object.
	l, err = golsm.Open(dir, 64000, true)
	assert.Nil(t, err)

	// Check that the key-value pairs still exist.
	for i := 0; i < 2000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}

	// Delete range 400-600.
	for i := 400; i < 600; i++ {
		err := l.Delete(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
	}

	// Give the WAL time to flush. Test becomes flaky if this is not done.
	time.Sleep(600 * time.Millisecond)

	// Suddenly crash the LSMTree.
	l, err = golsm.Open(dir, 64000, true)
	assert.Nil(t, err)

	// Check that the key-value pairs still exist and updates are applied.
	for i := 0; i < 2000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		if i >= 400 && i < 600 {
			assert.Nil(t, value, "Expected value to be nil, got '%v'", string(value))
		} else {
			assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
		}
	}
}

// Test compaction of SSTables in the LSMTree. Writes 50k key-value pairs to the
// LSMTree and then checks that they all exist. Validates the presence of the
// SSTables on disk.
func TestLSMTreeCompaction(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreeCompaction"
	l, err := golsm.Open(dir, 2048, true)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(dir + golsm.WALDirectorySuffix)

	// Write 50k key-value pairs to the LSMTree.
	for i := 0; i < 50000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
	}

	// Check that the key-value pairs exist.
	for i := 0; i < 50000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}

	l.Close()

	// Read the files in the directory.
	files, err := os.ReadDir(dir)
	assert.Nil(t, err)
	assert.Less(t, 3, len(files), "Expected 3 files, got %v", len(files))

	// Check that at least one file of each kind exists: prefix sstable_0, sstable_1, sstable_2.
	var sstable1Exists, sstable2Exists bool
	for _, file := range files {
		if strings.Contains(file.Name(), "sstable_1") {
			sstable1Exists = true
		} else if strings.Contains(file.Name(), "sstable_2") {
			sstable2Exists = true
		}
	}
	assert.True(t, sstable1Exists, "Expected at least SST at level 1 to exist")
	assert.True(t, sstable2Exists, "Expected at least SST at level 2 to exist")

	l, err = golsm.Open(dir, 2048, true)
	assert.Nil(t, err)

	// Check that the key-value pairs still exist.
	for i := 0; i < 50000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}

	l.Close()
}
