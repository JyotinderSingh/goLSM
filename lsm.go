package golsm

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
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
// directory: The directory where the SSTables will be stored.
// maxMemtableSize: The maximum size of the memtable before it is flushed to an
// SSTable.
// On startup, the LSMTree will load all the SSTables handles (if any) from disk
// into memory.
func OpenLSMTree(directory string, maxMemtableSize int64) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())
	lsm := &LSMTree{
		memtable:             NewMemtable(),
		maxMemtableSize:      maxMemtableSize,
		directory:            directory,
		current_sst_sequence: 0,
		sstables:             make([]*SSTable, 0),
		flushingQueue:        make([]*Memtable, 0),
		flushingChan:         make(chan *Memtable),
		ctx:                  ctx,
		cancel:               cancel,
	}

	if err := lsm.loadSSTables(); err != nil {
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
	close(l.flushingChan)
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
			if len(l.flushingChan) == 0 {
				return nil
			}
		case memtable := <-l.flushingChan:
			l.flushMemtable(memtable)
		}
	}
}

// Flush a memtable to an on-disk SSTable.
func (l *LSMTree) flushMemtable(memtable *Memtable) {
	l.current_sst_sequence++
	sstableFileName := l.getSSTableFilename()

	sst, err := SerializeToSSTable(memtable.GetEntries(),
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

// Get the filename for the next SSTable.
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
		return handleValue(value)
	}
	l.mu.RUnlock()

	// Check flushing queue memtables in reverse order.
	l.flushingQueueMu.RLock()
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		value = l.flushingQueue[i].Get(key)
		if value != nil {
			l.flushingQueueMu.RUnlock()
			return handleValue(value)
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
			return handleValue(value)
		}
	}
	l.sstablesMu.RUnlock()

	return nil, nil
}

// Loads all the SSTables from disk into memory. Also sorts the SSTables by
// sequence number. This function should be called on startup.
func (l *LSMTree) loadSSTables() error {
	if err := os.MkdirAll(l.directory, 0755); err != nil {
		return err
	}

	if err := l.loadSSTablesFromDisk(); err != nil {
		return err
	}

	l.sortSSTablesBySequenceNumber()

	if err := l.setCurrentSSTSequence(); err != nil {
		return err
	}

	return nil
}

// Load SSTables from disk. Loads all files in the directory that have the
// SSTable prefix.
func (l *LSMTree) loadSSTablesFromDisk() error {
	files, err := os.ReadDir(l.directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() || !isSSTableFile(file.Name()) {
			continue
		}

		sstable, err := OpenSSTable(l.directory + "/" + file.Name())
		if err != nil {
			return err
		}
		l.sstables = append(l.sstables, sstable)
	}

	return nil
}

// Sort the SSTables by sequence number.
// Example: sstable_1, sstable_2, sstable_3, sstable_4
func (l *LSMTree) sortSSTablesBySequenceNumber() {
	sort.Slice(l.sstables, func(i, j int) bool {
		iSequence := l.getSequenceNumber(l.sstables[i].file.Name())
		jSequence := l.getSequenceNumber(l.sstables[j].file.Name())
		return iSequence < jSequence
	})
}

// Get the sequence number from the SSTable filename.
// Example: sstable_123 -> 123
func (l *LSMTree) getSequenceNumber(filename string) uint64 {
	sequenceStr := filename[len(l.directory)+1+len(SSTableFilePrefix):]
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return sequence
}

// Set the current SST sequence number.
func (l *LSMTree) setCurrentSSTSequence() error {
	if len(l.sstables) > 0 {
		lastSSTable := l.sstables[len(l.sstables)-1]
		sequence := l.getSequenceNumber(lastSSTable.file.Name())
		l.current_sst_sequence = sequence
	}
	return nil
}
