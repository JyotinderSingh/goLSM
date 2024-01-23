package tests

import (
	"fmt"
	"testing"

	golsm "github.com/JyotinderSingh/go-lsm"
	"github.com/stretchr/testify/assert"
)

// Test the Put(), Get(), and Delete() methods of the Memtable.
func TestMemtablePutGetDelete(t *testing.T) {
	memtable := golsm.NewMemtable()

	// Test Put() and Get().
	memtable.Put("foo", []byte("bar"))
	assert.Equal(t, []byte("bar"), memtable.Get("foo"), "memtable.Get(\"foo\") should return \"bar\"")

	// Test Delete().
	memtable.Delete("foo")
	assert.Nil(t, memtable.Get("foo"), "memtable.Get(\"foo\") should return nil")
}

// Test the Scan() method of the Memtable.
func TestMemtableScan(t *testing.T) {
	memtable := golsm.NewMemtable()

	// Test Scan() with no results.
	results := memtable.Scan("foo", "foo")
	assert.Empty(t, results, "memtable.Scan(\"foo\", \"foo\") should return an empty slice")

	// Test Scan() with one result.
	memtable.Put("foo", []byte("bar"))
	results = memtable.Scan("foo", "foo")
	assert.Len(t, results, 1, "memtable.Scan(\"foo\", \"foo\") should return a slice with length 1")
	assert.Equal(t, []byte("bar"), results[0], "memtable.Scan(\"foo\", \"foo\") should return [\"bar\"]")

	// Test Scan() with multiple results.
	memtable.Put("foo", []byte("bar0"))
	memtable.Put("foo8", []byte("bar8"))
	memtable.Put("foo1", []byte("bar1"))
	memtable.Put("foo7", []byte("bar7"))
	memtable.Put("foo3", []byte("bar3"))
	memtable.Put("foo9", []byte("bar9"))
	memtable.Put("foo6", []byte("bar6"))
	memtable.Put("foo2", []byte("bar2"))
	memtable.Put("foo4", []byte("bar4"))
	memtable.Put("foo5", []byte("bar5"))
	results = memtable.Scan("foo", "foo9")
	assert.Len(t, results, 10, "memtable.Scan(\"foo\", \"foo9\") should return a slice with length 10")
	for i := 0; i < 10; i++ {
		assert.Equal(t, []byte(fmt.Sprintf("bar%v", i)), results[i], "memtable.Scan(\"foo\", \"foo9\") should return [\"bar0\", \"bar1\", ..., \"bar9\"]")
	}

	// Scan another range
	results = memtable.Scan("foo2", "foo7")
	assert.Len(t, results, 6, "memtable.Scan(\"foo2\", \"foo7\") should return a slice with length 6")
	for i := 2; i < 8; i++ {
		assert.Equal(t, []byte(fmt.Sprintf("bar%v", i)), results[i-2], "memtable.Scan(\"foo2\", \"foo7\") should return [\"bar2\", \"bar3\", ..., \"bar7\"]")
	}

	// Scan another range with no results
	results = memtable.Scan("foo2", "foo1")
	assert.Empty(t, results, "memtable.Scan(\"foo2\", \"foo1\") should return an empty slice")

	// Scan another range with one result
	results = memtable.Scan("foo2", "foo2")
	assert.Len(t, results, 1, "memtable.Scan(\"foo2\", \"foo2\") should return a slice with length 1")
	assert.Equal(t, []byte("bar2"), results[0], "memtable.Scan(\"foo2\", \"foo2\") should return [\"bar2\"]")

	// Scan another range with non-exact start and end keys
	results = memtable.Scan("foo2", "fooz")
	assert.Len(t, results, 8, "memtable.Scan(\"foo2\", \"fooz\") should return a slice with length 8")
	for i := 2; i < 10; i++ {
		assert.Equal(t, []byte(fmt.Sprintf("bar%v", i)), results[i-2], "memtable.Scan(\"foo2\", \"fooz\") should return [\"bar2\", \"bar3\", ..., \"bar9\"]")
	}

	// Scan another range with non-exact start and end keys
	results = memtable.Scan("a", "foo3")
	assert.Len(t, results, 4, "memtable.Scan(\"fo\", \"foo3\") should return a slice with length 4")
	for i := 0; i < 4; i++ {
		assert.Equal(t, []byte(fmt.Sprintf("bar%v", i)), results[i], "memtable.Scan(\"fo\", \"foo3\") should return [\"bar0\", \"bar1\", \"bar2\", \"bar3\"]")
	}

}
