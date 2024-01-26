package tests

import (
	"fmt"
	"os"
	"testing"

	golsm "github.com/JyotinderSingh/go-lsm"
)

func TestLSMTreePut(t *testing.T) {
	dir := "TestLSMTreePut"
	l, err := golsm.OpenLSMTree(dir, 1000)
	if err != nil {
		t.Errorf("OpenLSMTree returned error: %v", err)
	}
	defer os.RemoveAll(dir)
	defer l.Close()

	// Put a key-value pair into the LSMTree.
	err = l.Put("key", []byte("value"))
	if err != nil {
		t.Errorf("Put returned error: %v", err)
	}

	// Check that the key-value pair exists in the LSMTree.
	value, err := l.Get("key")
	if err != nil {
		t.Errorf("Get returned error: %v", err)
	}
	if string(value) != "value" {
		t.Errorf("Expected value to be 'value', got '%v'", string(value))
	}
}

// Write a thousand key-value pairs to the LSMTree and check that they all exist.
func TestLSMTreePutMany(t *testing.T) {
	dir := "TestLSMTreePutMany"
	l, err := golsm.OpenLSMTree(dir, 100)
	if err != nil {
		t.Errorf("OpenLSMTree returned error: %v", err)
	}
	defer os.RemoveAll(dir)

	defer l.Close()

	for i := 0; i < 1000; i++ {
		err := l.Put(fmt.Sprintf("%d", i), []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			t.Errorf("Put returned error: %v", err)
		}
	}

	for i := 0; i < 1000; i++ {
		value, err := l.Get(fmt.Sprintf("%d", i))
		if err != nil {
			t.Errorf("Get returned error: %v", err)
		}
		if string(value) != fmt.Sprintf("%d", i) {
			t.Errorf("Expected value to be '%v', got '%v'", fmt.Sprintf("%d", i), string(value))
		}
	}
}
