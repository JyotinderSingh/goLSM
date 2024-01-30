package golsm

import (
	"container/heap"

	"github.com/huandu/skiplist"
)

// Heap entry for the k-way merge algorithm.
type heapEntry struct {
	entry     *MemtableEntry
	listIndex int // index of the entry source.
	idx       int // index of the entry in the list.
	iterator  *SSTableIterator
}

// Heap implementation for the k-way merge algorithm.
type mergeHeap []heapEntry

func (h mergeHeap) Len() int {
	return len(h)
}

// Min heap based on timestamp. The entry with the smallest timestamp is at the
// top of the heap.
func (h mergeHeap) Less(i, j int) bool {
	return h[i].entry.Timestamp < h[j].entry.Timestamp
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(heapEntry))
}

// Pop the min entry from the heap.
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Performs a k-way merge on a list of possibly overlapping ranges and merges
// them into a single range without any duplicate entries.
// Deduplication is done by keeping track of the most recent entry for each key
// and discarding the older ones using the timestamp.
func mergeRanges(ranges [][]*MemtableEntry) [][]byte {
	minHeap := &mergeHeap{}
	heap.Init(minHeap)

	var results [][]byte

	// Keep track of the most recent entry for each key, in sorted order of keys.
	seen := skiplist.New(skiplist.String)

	// Add the first element from each list to the heap.
	for i, entries := range ranges {
		if len(entries) > 0 {
			heap.Push(minHeap, heapEntry{entry: entries[0], listIndex: i, idx: 0})
		}
	}

	for minHeap.Len() > 0 {
		// Pop the min entry from the heap.
		minEntry := heap.Pop(minHeap).(heapEntry)
		previousValue := seen.Get(minEntry.entry.Key)

		// Check if this key has been seen before.
		if previousValue != nil {
			// If the previous entry has a smaller timestamp, then we need to
			// replace it with the more recent entry.
			if previousValue.Value.(heapEntry).entry.Timestamp < minEntry.entry.Timestamp {
				seen.Set(minEntry.entry.Key, minEntry)
			}
		} else {
			// Add the entry to the seen list.
			seen.Set(minEntry.entry.Key, minEntry)
		}

		// Add the next element from the same list to the heap
		if minEntry.idx+1 < len(ranges[minEntry.listIndex]) {
			nextEntry := ranges[minEntry.listIndex][minEntry.idx+1]
			heap.Push(minHeap, heapEntry{entry: nextEntry, listIndex: minEntry.listIndex, idx: minEntry.idx + 1})
		}
	}

	// Iterate through the seen list and add the values to the results.
	iter := seen.Front()
	for iter != nil {
		entry := iter.Value.(heapEntry)
		if entry.entry.Command == Command_DELETE {
			iter = iter.Next()
			continue
		}
		results = append(results, entry.entry.Value)
		iter = iter.Next()
	}

	return results
}

// Performs a k-way merge on a list of possibly overlapping entries from the SSTs
// and merges them into a single range without any duplicate entries.
// Deduplication is done by keeping track of the most recent entry for each key
// and discarding the older ones using the timestamp.
func mergeEntriesFromSSTs(iterators []*SSTableIterator) []*MemtableEntry {
	minHeap := &mergeHeap{}
	heap.Init(minHeap)

	var results []*MemtableEntry

	// Keep track of the most recent entry for each key, in sorted order of keys.
	seen := skiplist.New(skiplist.String)

	// Add the iterators to the heap.
	for _, iterator := range iterators {
		heap.Push(minHeap, heapEntry{entry: iterator.Value, iterator: iterator})
	}

	for minHeap.Len() > 0 {
		// Pop the min entry from the heap.
		minEntry := heap.Pop(minHeap).(heapEntry)
		previousValue := seen.Get(minEntry.entry.Key)

		// Check if this key has been seen before.
		if previousValue != nil {
			// If the previous entry has a smaller timestamp, then we need to
			// replace it with the more recent entry.
			if previousValue.Value.(heapEntry).entry.Timestamp < minEntry.entry.Timestamp {
				seen.Set(minEntry.entry.Key, minEntry)
			}
		} else {
			// Add the entry to the seen list.
			seen.Set(minEntry.entry.Key, minEntry)
		}

		// Add the next element from the same list to the heap
		if minEntry.iterator.Next() != nil {
			nextEntry := minEntry.iterator.Value
			heap.Push(minHeap, heapEntry{entry: nextEntry, iterator: minEntry.iterator})
		}
	}

	// Iterate through the seen list and add the values to the results.
	iter := seen.Front()
	for iter != nil {
		entry := iter.Value.(heapEntry)
		if entry.entry.Command == Command_DELETE {
			iter = iter.Next()
			continue
		}
		results = append(results, entry.entry)
		iter = iter.Next()
	}

	return results
}
