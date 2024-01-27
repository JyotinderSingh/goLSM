package tests

import (
	"fmt"
	"os"
	"sync"
	"testing"

	golsm "github.com/JyotinderSingh/go-lsm"
	"github.com/stretchr/testify/assert"
)

func TestLSMTreePut(t *testing.T) {
	t.Parallel()
	dir := "TestLSMTreePut"
	l, err := golsm.OpenLSMTree(dir, 1000)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

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
	l, err = golsm.OpenLSMTree(dir, 1000)
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
	l, err := golsm.OpenLSMTree(dir, 32000)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	defer l.Close()

	for i := 0; i < 1000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)

	}

	for i := 0; i < 1000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d", i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
	}
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
	l, err := golsm.OpenLSMTree(dir, 100)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

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

	l, err = golsm.OpenLSMTree(dir, 100)
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

	l, err = golsm.OpenLSMTree(dir, 100)
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
	l, err := golsm.OpenLSMTree(dir, 32000)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	for i := 0; i < 100000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
	}

	l.Close()

	l, err = golsm.OpenLSMTree(dir, 32000)
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
	l, err := golsm.OpenLSMTree(dir, 32000)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

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

	l, err = golsm.OpenLSMTree(dir, 32000)
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
	l, err := golsm.OpenLSMTree(dir, 200)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

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
	l, err = golsm.OpenLSMTree(dir, 32000)
	assert.Nil(t, err)

	// Check that the key-value pairs still exist.
	for i := 0; i < 1000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%d%d", i, i), string(value), "Expected value to be '%v', got '%v'", fmt.Sprintf("%d%d", i, i), string(value))
	}

	l.Close()
}
