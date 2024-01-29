package golsm

import (
	"github.com/spaolacci/murmur3"
)

// newBloomFilter creates a new BloomFilter with the specified bitset size.
func newBloomFilter(size int64) *BloomFilter {
	return &BloomFilter{
		Bitset: make([]bool, size),
		Size:   size,
	}
}

// Add adds an item to the Bloom filter.
func (bf *BloomFilter) Add(item []byte) {
	// Generate 3 hash values for the item.
	hash1 := murmur3.Sum64(item)
	hash2 := murmur3.Sum64WithSeed(item, 1)
	hash3 := murmur3.Sum64WithSeed(item, 2)

	// Set the bits in the bitset
	bf.Bitset[hash1%uint64(bf.Size)] = true
	bf.Bitset[hash2%uint64(bf.Size)] = true
	bf.Bitset[hash3%uint64(bf.Size)] = true
}

// Test checks if an item might be in the Bloom filter.
func (bf *BloomFilter) Test(item []byte) bool {
	// Generate 3 hash values for the item.
	hash1 := murmur3.Sum64(item)
	hash2 := murmur3.Sum64WithSeed(item, 1)
	hash3 := murmur3.Sum64WithSeed(item, 2)

	// Check if all the required bits are set in the bitset.
	return bf.Bitset[hash1%uint64(bf.Size)] &&
		bf.Bitset[hash2%uint64(bf.Size)] &&
		bf.Bitset[hash3%uint64(bf.Size)]
}
