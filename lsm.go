package golsm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	gw "github.com/JyotinderSingh/go-wal"
)

////////////////////////////////////////////////////////////////////////////////
// Constants
////////////////////////////////////////////////////////////////////////////////

const (
	SSTableFilePrefix  = "sstable_"
	WALDirectorySuffix = "_wal"
	maxLevels          = 6 // Maximum number of levels in the LSMTree.
)

// Maximum number of SSTables in each level before compaction is triggered.
var maxLevelSSTables = map[int]int{
	0: 4,
	1: 8,
	2: 16,
	3: 32,
	4: 64,
	5: 128,
	6: 256,
}

////////////////////////////////////////////////////////////////////////////////
// Types
////////////////////////////////////////////////////////////////////////////////

type level struct {
	sstables []*SSTable   // SSTables in this level
	mu       sync.RWMutex // [Lock 2]: for sstables in this level.
}

type KVPair struct {
	Key   string
	Value []byte
}

type LSMTree struct {
	memtable             *Memtable
	mu                   sync.RWMutex   // [Lock 1]: for memtable.
	maxMemtableSize      int64          // Maximum size of the memtable before it is flushed to an SSTable.
	directory            string         // Directory where the SSTables will be stored.
	wal                  *gw.WAL        // WAL for LSMTree
	inRecovery           bool           // True if the LSMTree is recovering entries from WAL.
	levels               []*level       // SSTables in each level.
	current_sst_sequence uint64         // Sequence number for the next SSTable.
	compactionChan       chan int       // Channel for triggering compaction at a level.
	flushingQueue        []*Memtable    // Queue of memtables that need to be flushed to SSTables. Used to serve reads.
	flushingQueueMu      sync.RWMutex   // [Lock 3]: for flushingQueue.
	flushingChan         chan *Memtable // Channel for triggering flushing of memtables to SSTables.
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
}

////////////////////////////////////////////////////////////////////////////////
// Public API
////////////////////////////////////////////////////////////////////////////////

// Opens an LSMTree. If the directory does not exist, it will be created.
// directory: The directory where the SSTables will be stored.
// maxMemtableSize: The maximum size of the memtable in bytes before it is
// flushed to an SSTable.
// runRecovery: If true, the LSMTree will recover from the WAL on startup.
// On startup, the LSMTree will load all the SSTables handles (if any) from disk
// into memory.
func Open(directory string, maxMemtableSize int64, recoverFromWAL bool) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Setup the WAL for the LSMTree.
	wal, err := gw.OpenWAL(directory+WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		cancel()
		return nil, err
	}

	levels := make([]*level, maxLevels)
	for i := 0; i < maxLevels; i++ {
		levels[i] = &level{
			sstables: make([]*SSTable, 0),
		}
	}

	lsm := &LSMTree{
		memtable:             NewMemtable(),
		maxMemtableSize:      maxMemtableSize,
		directory:            directory,
		wal:                  wal,
		inRecovery:           false,
		current_sst_sequence: 0,
		levels:               levels,
		compactionChan:       make(chan int, 100),
		flushingQueue:        make([]*Memtable, 0),
		flushingChan:         make(chan *Memtable, 100),
		ctx:                  ctx,
		cancel:               cancel,
	}

	if err := lsm.loadSSTables(); err != nil {
		return nil, err
	}

	lsm.wg.Add(2)
	go lsm.backgroundCompaction()
	go lsm.backgroundMemtableFlushing()

	// Recover any entries that were written to the WAL but not flushed to
	// SSTables.
	if recoverFromWAL {
		if err := lsm.recoverFromWAL(); err != nil {
			return nil, err
		}
	}

	return lsm, nil
}

func (l *LSMTree) Close() error {
	l.mu.Lock()
	l.flushingQueueMu.Lock()
	l.flushingQueue = append(l.flushingQueue, l.memtable)
	l.flushingQueueMu.Unlock()

	l.flushingChan <- l.memtable
	l.memtable = NewMemtable()

	l.mu.Unlock()

	l.cancel()
	l.wg.Wait()
	l.wal.Close()
	close(l.flushingChan)
	close(l.compactionChan)
	return nil
}

// Insert a key-value pair into the LSMTree.
func (l *LSMTree) Put(key string, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Write to WAL before writing to memtable. We don't need to write to WAL
	// while recovering from WAL.
	if !l.inRecovery {
		l.wal.WriteEntry(mustMarshal(&WALEntry{
			Key:       key,
			Command:   Command_PUT,
			Value:     value,
			Timestamp: time.Now().UnixNano(),
		}))
	}

	l.memtable.Put(key, value)

	// Check if the memtable has exceeded the maximum size and needs to be flushed
	// to an SSTable.
	if l.memtable.SizeInBytes() > l.maxMemtableSize {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()

		l.flushingChan <- l.memtable
		l.memtable = NewMemtable()
	}

	return nil
}

// Delete a key-value pair from the LSMTree.
func (l *LSMTree) Delete(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Write to WAL before writing to memtable. We don't need to write to WAL
	// while recovering from WAL.
	if !l.inRecovery {
		l.wal.WriteEntry(mustMarshal(&WALEntry{
			Key:       key,
			Command:   Command_DELETE,
			Timestamp: time.Now().UnixNano(),
		}))
	}

	l.memtable.Delete(key)

	// Check if the memtable has exceeded the maximum size and needs to be flushed
	// to an SSTable.
	if l.memtable.SizeInBytes() > l.maxMemtableSize {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()

		l.flushingChan <- l.memtable
		l.memtable = NewMemtable()
	}
	return nil
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

	// Iterate over all levels.
	for level := range l.levels {
		// Check SSTables at this level in reverse order.
		l.levels[level].mu.RLock()
		for i := len(l.levels[level].sstables) - 1; i >= 0; i-- {
			value, err := l.levels[level].sstables[i].Get(key)
			if err != nil {
				l.levels[level].mu.RUnlock()
				return nil, err
			}
			if value != nil {
				l.levels[level].mu.RUnlock()
				return handleValue(value)
			}
		}
		l.levels[level].mu.RUnlock()
	}

	return nil, nil
}

// RangeScan returns all the entries in the LSMTree that have keys in the range
// [startKey, endKey]. The entries are returned in sorted order of keys.
func (l *LSMTree) RangeScan(startKey string, endKey string) ([]KVPair, error) {
	ranges := [][]*MemtableEntry{}
	// We take all locks together to ensure a consistent view of the LSMTree for
	// the range scan.
	l.mu.RLock()
	defer l.mu.RUnlock()
	// lock all levels
	for _, level := range l.levels {
		level.mu.RLock()
		defer level.mu.RUnlock()
	}
	l.flushingQueueMu.RLock()
	defer l.flushingQueueMu.RUnlock()

	entries := l.memtable.RangeScan(startKey, endKey)
	ranges = append(ranges, entries)

	// Check flushing queue memtables in reverse order.
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		entries := l.flushingQueue[i].RangeScan(startKey, endKey)
		ranges = append(ranges, entries)
	}

	// Iterate through all levels.
	for _, level := range l.levels {
		// Check SSTables at this level in reverse order.
		for i := len(level.sstables) - 1; i >= 0; i-- {
			entries, err := level.sstables[i].RangeScan(startKey, endKey)
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, entries)
		}
	}

	return mergeRanges(ranges), nil
}

////////////////////////////////////////////////////////////////////////////////
// Utilities
////////////////////////////////////////////////////////////////////////////////

// Continuously listen on flushingChan for memtables to flush. When a memtable
// is received, flush it to an SSTable. When the context is cancelled, return.
func (l *LSMTree) backgroundMemtableFlushing() error {
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

// Continuously listen on compactionChan for levels that need to be compacted.
// Runs a tiered compaction on the LSMTree. When the context is cancelled,
// return.
func (l *LSMTree) backgroundCompaction() error {
	defer l.wg.Done()
	for {
		select {
		case <-l.ctx.Done():
			// Finish pending compactions.
			if readyToExit := l.checkAndTriggerCompaction(); readyToExit {
				return nil
			}

		case compactionCandidate := <-l.compactionChan:
			l.compactLevel(compactionCandidate)
		}
	}
}

// Check if all levels have less than the maximum number of SSTables.
// If any level has more than the maximum number of SSTables, trigger compaction.
// Returns true if all levels are ready to exit, false otherwise.
func (l *LSMTree) checkAndTriggerCompaction() bool {
	readyToExit := true
	for idx, level := range l.levels {
		level.mu.RLock()
		if len(level.sstables) > maxLevelSSTables[idx] {
			l.compactionChan <- idx
			readyToExit = false
		}
		level.mu.RUnlock()
	}
	return readyToExit
}

// Compact the SSTables in the compaction candidate level.
func (l *LSMTree) compactLevel(compactionCandidate int) error {
	if compactionCandidate == maxLevels-1 {
		// We don't need to compact the SSTables in the last level.
		return nil
	}

	// Lock the level while we check if we need to compact it.
	l.levels[compactionCandidate].mu.RLock()

	// Check if the number of SSTables in this level is less than the limit.
	// If yes, we don't need to compact this level.
	if len(l.levels[compactionCandidate].sstables) < maxLevelSSTables[compactionCandidate] {
		l.levels[compactionCandidate].mu.RUnlock()
		return nil
	}

	// 1. First get iterators for all the SSTables in this level.
	_, iterators := l.getSSTableHandlesAtLevel(compactionCandidate)

	// We can release the lock on the level now while we process the SSTables. This
	// is safe because these SSTables are immutable, and can only be deleted by
	// this function (which is single-threaded).
	l.levels[compactionCandidate].mu.RUnlock()

	// 2. Merge all the SSTables into a new SSTable.
	mergedSSTable, err := l.mergeSSTables(iterators, compactionCandidate+1)
	if err != nil {
		return err
	}

	// 3. Delete the old SSTables.
	l.deleteSSTablesAtLevel(compactionCandidate, iterators)

	// 4. Add the new SSTable to the next level.
	l.addSSTableToLevel(mergedSSTable, compactionCandidate+1)

	return nil
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
	l.initializeCurrentSequenceNumber()

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

		level := l.getLevelFromSSTableFilename(sstable.file.Name())
		l.levels[level].sstables = append(l.levels[level].sstables, sstable)
	}

	return nil
}

// Recover from the WAL. Read all entries from the WAL, after the last
// checkpoint. This will give us all the entries that were written to the
// memtable but not flushed to SSTables.
func (l *LSMTree) recoverFromWAL() error {
	l.inRecovery = true
	defer func() {
		l.inRecovery = false
	}()
	// Read all entries from WAL, after the last checkpoint. This will give us
	// all the entries that were written to the memtable but not flushed to
	// SSTables.
	entries, err := l.readEntriesFromWAL()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := l.processWALEntry(entry); err != nil {
			return err
		}
	}

	return nil
}

// Runs a merge on the iterators and creates a new SSTable from the merged
// entries.
func (l *LSMTree) mergeSSTables(iterators []*SSTableIterator, targetLevel int) (*SSTable, error) {
	mergedEntries := mergeIterators(iterators)

	sstableFileName := l.getSSTableFilename(targetLevel)

	sst, err := SerializeToSSTable(mergedEntries, sstableFileName)
	if err != nil {
		return nil, err
	}

	return sst, nil
}

// Get the SSTables and iterators for the SSTables at the level. Not thread-safe.
func (l *LSMTree) getSSTableHandlesAtLevel(level int) ([]*SSTable, []*SSTableIterator) {
	sstables := l.levels[level].sstables
	iterators := make([]*SSTableIterator, len(sstables))
	for i, sstable := range sstables {
		iterators[i] = sstable.Front()
	}
	return sstables, iterators
}

// Delete the SSTables identified by the iterators.
func (l *LSMTree) deleteSSTablesAtLevel(level int, iterators []*SSTableIterator) {
	l.levels[level].mu.Lock()
	l.levels[level].sstables = l.levels[level].sstables[len(iterators):]
	for _, it := range iterators {
		// Delete the file pointed to by the iterator.
		if err := os.Remove(it.s.file.Name()); err != nil {
			panic(err)
		}
	}
	l.levels[level].mu.Unlock()
}

// Add an SSTable to the level.
func (l *LSMTree) addSSTableToLevel(sst *SSTable, level int) {
	l.levels[level].mu.Lock()
	l.levels[level].sstables = append(l.levels[level].sstables, sst)
	// Send a signal on the compactionChan to indicate that a new SSTable has
	// been created.
	l.compactionChan <- level
	l.levels[level].mu.Unlock()
}

// Flush a memtable to an on-disk SSTable.
func (l *LSMTree) flushMemtable(memtable *Memtable) {
	if memtable.size == 0 {
		return
	}
	atomic.AddUint64(&l.current_sst_sequence, 1)
	sstableFileName := l.getSSTableFilename(0)

	sst, err := SerializeToSSTable(memtable.GetEntries(),
		sstableFileName)
	if err != nil {
		panic(err)
	}

	l.levels[0].mu.Lock()
	l.flushingQueueMu.Lock()

	l.wal.CreateCheckpoint(mustMarshal(&WALEntry{
		Key:       sstableFileName,
		Command:   Command_WRITE_SST,
		Timestamp: time.Now().UnixNano(),
	}))

	l.levels[0].sstables = append(l.levels[0].sstables, sst)
	l.flushingQueue = l.flushingQueue[1:]

	l.flushingQueueMu.Unlock()
	l.levels[0].mu.Unlock()

	// Send a signal on the compactionChan to indicate that a new SSTable has
	// been created.
	l.compactionChan <- 0
}

func (l *LSMTree) readEntriesFromWAL() ([]*gw.WAL_Entry, error) {
	entries, err := l.wal.ReadAllFromOffset(-1, true)
	if err != nil {
		// Attempt to repair the WAL.
		_, err = l.wal.Repair()
		if err != nil {
			return nil, err
		}
		entries, err = l.wal.ReadAllFromOffset(-1, true)
		if err != nil {
			return nil, err
		}
	}
	return entries, nil
}

// Process a WAL entry. This function is used to recover from the WAL.
// It reads the WAL entry and performs the corresponding operation on the
// LSMTree.
func (l *LSMTree) processWALEntry(entry *gw.WAL_Entry) error {
	if entry.GetIsCheckpoint() {
		// We may use this entry in the future to recover from more sophisticated
		// failures.
		return nil
	}

	walEntry := &WALEntry{}
	mustUnmarshal(entry.GetData(), walEntry)

	switch walEntry.Command {
	case Command_PUT:
		return l.Put(walEntry.Key, walEntry.Value)
	case Command_DELETE:
		return l.Delete(walEntry.Key)
	case Command_WRITE_SST:
		return errors.New("unexpected write sst command in WAL")
	default:
		return errors.New("unknown command in WAL")
	}
}

// Sort the SSTables by sequence number.
// Example: sstable_0_1, sstable_0_2, sstable_0_3, sstable_0_4
func (l *LSMTree) sortSSTablesBySequenceNumber() {
	for _, level := range l.levels {
		sort.Slice(level.sstables, func(i, j int) bool {
			iSequence := l.getSequenceNumber(level.sstables[i].file.Name())
			jSequence := l.getSequenceNumber(level.sstables[j].file.Name())
			return iSequence < jSequence
		})
	}
}

// Get the sequence number from the SSTable filename.
// Example: sstable_0_123 -> 123
func (l *LSMTree) getSequenceNumber(filename string) uint64 {
	// directory + "/" + sstable_ + level (single digit) + _ + 1
	sequenceStr := filename[len(l.directory)+1+2+len(SSTableFilePrefix):]
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return sequence
}

// Get the level from the SSTable filename.
// Example: sstable_0_123 -> 0
func (l *LSMTree) getLevelFromSSTableFilename(filename string) int {
	// directory + "/" + sstable_ + level (single digit) + _ + 1
	levelStr := filename[len(l.directory)+1+len(SSTableFilePrefix) : len(l.directory)+2+len(SSTableFilePrefix)]
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		panic(err)
	}
	return level
}

// Set the current_sst_sequence to the maximum sequence number found in any of
// the SSTables.
func (l *LSMTree) initializeCurrentSequenceNumber() error {
	var maxSequence uint64
	for _, level := range l.levels {
		if len(level.sstables) > 0 {
			lastSSTable := level.sstables[len(level.sstables)-1]
			sequence := l.getSequenceNumber(lastSSTable.file.Name())
			if sequence > maxSequence {
				maxSequence = sequence
			}
		}
	}

	atomic.StoreUint64(&l.current_sst_sequence, maxSequence)

	return nil
}

// Get the filename for the next SSTable.
func (l *LSMTree) getSSTableFilename(level int) string {
	return fmt.Sprintf("%s/%s%d_%d", l.directory, SSTableFilePrefix, level, atomic.LoadUint64(&l.current_sst_sequence))
}
