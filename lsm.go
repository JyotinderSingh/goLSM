package golsm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	gw "github.com/JyotinderSingh/go-wal"
)

const (
	SSTableFilePrefix  = "sstable_"
	WALDirectorySuffix = "_wal"
)

type LSMTree struct {
	memtable        *Memtable
	mu              sync.RWMutex // Lock 1: for memtable.
	maxMemtableSize int64
	directory       string
	wal             *gw.WAL // WAL for LSMTree
	inRecovery      bool
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
// runRecovery: If true, the LSMTree will recover from the WAL on startup.
// On startup, the LSMTree will load all the SSTables handles (if any) from disk
// into memory.
func OpenLSMTree(directory string, maxMemtableSize int64, runRecovery bool) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Setup the WAL for the LSMTree.
	wal, err := gw.OpenWAL(directory+WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		cancel()
		return nil, err
	}

	lsm := &LSMTree{
		memtable:             NewMemtable(),
		maxMemtableSize:      maxMemtableSize,
		directory:            directory,
		wal:                  wal,
		inRecovery:           false,
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

	// Recover any entries that were written to the WAL but not flushed to
	// SSTables.
	if runRecovery {
		if err := lsm.recoverFromWAL(); err != nil {
			return nil, err
		}
	}

	return lsm, nil
}

func (l *LSMTree) Close() error {
	l.addCurrentMemtableToFlushQueue()
	l.cancel()
	l.wg.Wait()
	l.wal.Close()
	close(l.flushingChan)
	return nil
}

// Insert a key-value pair into the LSMTree.
func (l *LSMTree) Put(key string, value []byte) error {
	l.mu.Lock()

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
	l.mu.Unlock()
	if l.memtable.size > l.maxMemtableSize {
		l.addCurrentMemtableToFlushQueue()
	}

	return nil
}

// Delete a key-value pair from the LSMTree.
func (l *LSMTree) Delete(key string) error {
	l.mu.Lock()

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

	l.wal.CreateCheckpoint(mustMarshal(&WALEntry{
		Key:       sstableFileName,
		Command:   Command_WRITE_SST,
		Timestamp: time.Now().UnixNano(),
	}))

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

// RangeScan returns all the entries in the LSMTree that have keys in the range
// [startKey, endKey]. The entries are returned in sorted order of keys.
func (l *LSMTree) RangeScan(startKey string, endKey string) ([][]byte, error) {
	ranges := [][]*MemtableEntry{}
	// We take all locks together to ensure a consistent view of the LSMTree for
	// the range scan.
	l.mu.RLock()
	defer l.mu.RUnlock()
	l.sstablesMu.RLock()
	defer l.sstablesMu.RUnlock()
	l.flushingQueueMu.RLock()
	defer l.flushingQueueMu.RUnlock()

	entries := l.memtable.RangeScan(startKey, endKey)
	ranges = append(ranges, entries)

	// Check flushing queue memtables in reverse order.
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		entries := l.flushingQueue[i].RangeScan(startKey, endKey)
		ranges = append(ranges, entries)
	}

	// Check SSTables in reverse order.
	for i := len(l.sstables) - 1; i >= 0; i-- {
		entries, err := l.sstables[i].RangeScan(startKey, endKey)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, entries)
	}

	return mergeRanges(ranges), nil
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
