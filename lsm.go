package golsm

import (
	"context"
	"fmt"
	"os"
	"sync"
)

const (
	SSTableFilePrefix = "sstable_"
)

type LSMTree struct {
	memtable        *Memtable
	mu              sync.RWMutex // Lock 1: for memtable.
	maxMemtableSize int64
	directory       string
	// --- Manage SSTables.
	sstables             []*SSTable
	sstablesMu           sync.RWMutex // Lock 2: for sstables.
	current_sst_sequence uint64
	// --- Manage flushing of memtables to SSTables.
	flushingQueue   []*Memtable
	flushingQueueMu sync.RWMutex // Lock 3: for flushingQueue.
	flushingChan    chan *Memtable
	// --- Go context for managing the compaction goroutine.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Create a new LSMTree.
func OpenLSMTree(directory string, maxMemtableSize int64) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())
	lsm := &LSMTree{
		memtable:             NewMemtable(),
		maxMemtableSize:      maxMemtableSize,
		directory:            directory,
		current_sst_sequence: 0,
		sstables:             make([]*SSTable, 0),
		flushingQueue:        make([]*Memtable, 10),
		flushingChan:         make(chan *Memtable),
		ctx:                  ctx,
		cancel:               cancel,
	}

	if err := os.MkdirAll(lsm.directory, 0755); err != nil {
		return nil, err
	}

	lsm.wg.Add(1)
	go lsm.flushMemtablesInQueue()
	return lsm, nil
}

func (l *LSMTree) Close() error {
	l.addCurrentMemtableToFlushQueue()
	l.cancel()
	l.wg.Wait()
	return nil
}

// Insert a key-value pair into the LSMTree.
func (l *LSMTree) Put(key string, value []byte) error {
	l.mu.Lock()
	l.memtable.Put(key, value)
	l.mu.Unlock()
	if l.memtable.size > l.maxMemtableSize {
		l.addCurrentMemtableToFlushQueue()
	}

	return nil
}

// Delete a key-value pair from the LSMTree.
func (l *LSMTree) Delete(key string) error {
	l.mu.Lock()
	l.memtable.Delete(key)
	l.mu.Unlock()
	if l.memtable.size > l.maxMemtableSize {
		l.addCurrentMemtableToFlushQueue()
	}

	return nil
}

func (l *LSMTree) addCurrentMemtableToFlushQueue() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.flushingQueueMu.Lock()
	l.flushingQueue = append(l.flushingQueue, l.memtable)
	l.flushingQueueMu.Unlock()

	l.flushingChan <- l.memtable

	l.memtable = NewMemtable()
}

// Continuously listen on flushingChan for memtables to flush. When a memtable
// is received, flush it to an SSTable. When the context is cancelled, return.
func (l *LSMTree) flushMemtablesInQueue() error {
	defer l.wg.Done()
	for {
		select {
		case <-l.ctx.Done():
			return nil
		case memtable := <-l.flushingChan:
			l.flushMemtable(memtable)
		}
	}
}

func (l *LSMTree) flushMemtable(memtable *Memtable) {
	l.current_sst_sequence++
	sstableFileName := l.getSSTableFilename()

	sst, err := SerializeToSSTable(memtable.GetSerializableEntries(),
		sstableFileName)
	if err != nil {
		panic(err)
	}

	l.sstablesMu.Lock()
	l.flushingQueueMu.Lock()

	l.sstables = append(l.sstables, sst)
	l.flushingQueue = l.flushingQueue[1:]

	l.flushingQueueMu.Unlock()
	l.sstablesMu.Unlock()
}

func (l *LSMTree) getSSTableFilename() string {
	return fmt.Sprintf("%s/%s%d", l.directory, SSTableFilePrefix, l.current_sst_sequence)
}

// Get the value for a given key from the LSMTree. Returns nil if the key is not
// found. This function will first look in the memtable, then in the SSTables.
func (l *LSMTree) Get(key string) ([]byte, error) {
	l.mu.RLock()
	value := l.memtable.Get(key)
	if value != nil {
		l.mu.RUnlock()
		return value, nil
	}
	l.mu.RUnlock()

	// Check flushing queue memtables in reverse order.
	l.flushingQueueMu.RLock()
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		value = l.flushingQueue[i].Get(key)
		if value != nil {
			l.flushingQueueMu.RUnlock()
			return value, nil
		}
	}
	l.flushingQueueMu.RUnlock()

	// Check SSTables in reverse order.
	l.sstablesMu.RLock()
	for i := len(l.sstables) - 1; i >= 0; i-- {
		value, err := l.sstables[i].Get(key)
		if err != nil {
			l.sstablesMu.RUnlock()
			return nil, err
		}
		if value != nil {
			l.sstablesMu.RUnlock()
			return value, nil
		}
	}
	l.sstablesMu.RUnlock()

	return nil, nil
}
