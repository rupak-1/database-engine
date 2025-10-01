package storage

import (
	"database_engine/types"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DiskStorage implements the StorageEngine interface using disk-based storage
type DiskStorage struct {
	dataDir    string
	dataFile   *os.File
	indexFile  *os.File
	mu         sync.RWMutex
	closed     bool
	index      map[types.Key]int64 // Maps key to file offset
	nextOffset int64
}

// NewDiskStorage creates a new disk-based storage instance
func NewDiskStorage(dataDir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	dataPath := filepath.Join(dataDir, "data.db")
	indexPath := filepath.Join(dataDir, "index.db")

	// Open or create data file
	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	// Open or create index file
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	storage := &DiskStorage{
		dataDir:    dataDir,
		dataFile:   dataFile,
		indexFile:  indexFile,
		index:      make(map[types.Key]int64),
		nextOffset: 0,
		closed:     false,
	}

	// Load existing index
	if err := storage.loadIndex(); err != nil {
		storage.Close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	return storage, nil
}

// loadIndex loads the index from disk
func (s *DiskStorage) loadIndex() error {
	// Get file size to check if index file is empty
	stat, err := s.indexFile.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		return nil // Empty index file, start fresh
	}

	// Read index data
	indexData, err := io.ReadAll(s.indexFile)
	if err != nil {
		return err
	}

	// Parse JSON index
	if len(indexData) > 0 {
		if err := json.Unmarshal(indexData, &s.index); err != nil {
			return err
		}
	}

	// Calculate next offset based on data file size
	dataStat, err := s.dataFile.Stat()
	if err != nil {
		return err
	}
	s.nextOffset = dataStat.Size()

	return nil
}

// saveIndex saves the index to disk
func (s *DiskStorage) saveIndex() error {
	// Seek to beginning of index file
	if _, err := s.indexFile.Seek(0, 0); err != nil {
		return err
	}

	// Truncate file to remove old data
	if err := s.indexFile.Truncate(0); err != nil {
		return err
	}

	// Marshal index to JSON
	indexData, err := json.Marshal(s.index)
	if err != nil {
		return err
	}

	// Write index data
	_, err = s.indexFile.Write(indexData)
	return err
}

// writeEntry writes an entry to the data file
func (s *DiskStorage) writeEntry(entry *types.Entry) (int64, error) {
	// Serialize entry
	entryData, err := json.Marshal(entry)
	if err != nil {
		return 0, err
	}

	// Write length prefix
	length := uint32(len(entryData))
	if err := binary.Write(s.dataFile, binary.LittleEndian, length); err != nil {
		return 0, err
	}

	// Write entry data
	offset := s.nextOffset
	if _, err := s.dataFile.Write(entryData); err != nil {
		return 0, err
	}

	// Update next offset
	s.nextOffset += int64(4 + len(entryData)) // 4 bytes for length + data

	return offset, nil
}

// readEntry reads an entry from the data file at the given offset
func (s *DiskStorage) readEntry(offset int64) (*types.Entry, error) {
	// Seek to offset
	if _, err := s.dataFile.Seek(offset, 0); err != nil {
		return nil, err
	}

	// Read length prefix
	var length uint32
	if err := binary.Read(s.dataFile, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	// Read entry data
	entryData := make([]byte, length)
	if _, err := io.ReadFull(s.dataFile, entryData); err != nil {
		return nil, err
	}

	// Deserialize entry
	var entry types.Entry
	if err := json.Unmarshal(entryData, &entry); err != nil {
		return nil, err
	}

	return &entry, nil
}

// Get retrieves a value by key
func (s *DiskStorage) Get(key types.Key) (types.Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrDatabaseClosed
	}

	offset, exists := s.index[key]
	if !exists {
		return nil, types.ErrKeyNotFound
	}

	entry, err := s.readEntry(offset)
	if err != nil {
		return nil, err
	}

	// Check if entry has expired
	if entry.IsExpired() {
		// Clean up expired entry
		delete(s.index, key)
		s.saveIndex()
		return nil, types.ErrKeyExpired
	}

	return entry.Value, nil
}

// Set stores a key-value pair
func (s *DiskStorage) Set(key types.Key, value types.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrDatabaseClosed
	}

	entry := &types.Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
		TTL:       nil, // No TTL by default
	}

	offset, err := s.writeEntry(entry)
	if err != nil {
		return err
	}

	// Update index
	s.index[key] = offset

	// Save index
	return s.saveIndex()
}

// SetWithTTL stores a key-value pair with a time-to-live
func (s *DiskStorage) SetWithTTL(key types.Key, value types.Value, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrDatabaseClosed
	}

	entry := &types.Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
		TTL:       &ttl,
	}

	offset, err := s.writeEntry(entry)
	if err != nil {
		return err
	}

	// Update index
	s.index[key] = offset

	// Save index
	return s.saveIndex()
}

// Delete removes a key-value pair
func (s *DiskStorage) Delete(key types.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrDatabaseClosed
	}

	delete(s.index, key)
	return s.saveIndex()
}

// Exists checks if a key exists
func (s *DiskStorage) Exists(key types.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, types.ErrDatabaseClosed
	}

	offset, exists := s.index[key]
	if !exists {
		return false, nil
	}

	entry, err := s.readEntry(offset)
	if err != nil {
		return false, err
	}

	// Check if entry has expired
	if entry.IsExpired() {
		return false, nil
	}

	return true, nil
}

// BatchGet retrieves multiple values by keys
func (s *DiskStorage) BatchGet(keys []types.Key) (map[types.Key]types.Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrDatabaseClosed
	}

	result := make(map[types.Key]types.Value)

	for _, key := range keys {
		offset, exists := s.index[key]
		if exists {
			entry, err := s.readEntry(offset)
			if err == nil && !entry.IsExpired() {
				result[key] = entry.Value
			}
		}
	}

	return result, nil
}

// BatchSet stores multiple key-value pairs
func (s *DiskStorage) BatchSet(entries []types.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrDatabaseClosed
	}

	now := time.Now()
	for _, entry := range entries {
		// Create a copy of the entry to avoid pointer issues
		entryCopy := entry
		// Set timestamp if not already set
		if entryCopy.Timestamp.IsZero() {
			entryCopy.Timestamp = now
		}

		offset, err := s.writeEntry(&entryCopy)
		if err != nil {
			return err
		}

		s.index[entryCopy.Key] = offset
	}

	return s.saveIndex()
}

// BatchDelete removes multiple key-value pairs
func (s *DiskStorage) BatchDelete(keys []types.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrDatabaseClosed
	}

	for _, key := range keys {
		delete(s.index, key)
	}

	return s.saveIndex()
}

// Clear removes all key-value pairs
func (s *DiskStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrDatabaseClosed
	}

	// Clear index
	s.index = make(map[types.Key]int64)
	s.nextOffset = 0

	// Truncate data file
	if err := s.dataFile.Truncate(0); err != nil {
		return err
	}

	// Save empty index
	return s.saveIndex()
}

// Size returns the number of key-value pairs
func (s *DiskStorage) Size() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, types.ErrDatabaseClosed
	}

	// Count only non-expired entries
	count := int64(0)
	for _, offset := range s.index {
		entry, err := s.readEntry(offset)
		if err == nil && !entry.IsExpired() {
			count++
		}
	}

	return count, nil
}

// Keys returns all keys in the storage
func (s *DiskStorage) Keys() ([]types.Key, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrDatabaseClosed
	}

	var keys []types.Key
	for key, offset := range s.index {
		entry, err := s.readEntry(offset)
		if err == nil && !entry.IsExpired() {
			keys = append(keys, key)
		}
	}

	return keys, nil
}

// Close closes the storage
func (s *DiskStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	// Close files
	if err := s.dataFile.Close(); err != nil {
		return err
	}

	if err := s.indexFile.Close(); err != nil {
		return err
	}

	return nil
}

// IsClosed returns true if the storage is closed
func (s *DiskStorage) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.closed
}

// CleanupExpired removes all expired entries
func (s *DiskStorage) CleanupExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for key, offset := range s.index {
		entry, err := s.readEntry(offset)
		if err == nil && entry.IsExpired() {
			delete(s.index, key)
			count++
		}
	}

	if count > 0 {
		s.saveIndex()
	}

	return count
}

// GetDiskUsage returns approximate disk usage in bytes
func (s *DiskStorage) GetDiskUsage() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dataStat, err := s.dataFile.Stat()
	if err != nil {
		return 0, err
	}

	indexStat, err := s.indexFile.Stat()
	if err != nil {
		return 0, err
	}

	return dataStat.Size() + indexStat.Size(), nil
}

// Compact performs garbage collection by removing deleted entries
func (s *DiskStorage) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrDatabaseClosed
	}

	// Create temporary files for compaction
	tempDataPath := filepath.Join(s.dataDir, "data.db.tmp")
	tempIndexPath := filepath.Join(s.dataDir, "index.db.tmp")

	tempDataFile, err := os.Create(tempDataPath)
	if err != nil {
		return err
	}
	defer tempDataFile.Close()

	tempIndexFile, err := os.Create(tempIndexPath)
	if err != nil {
		return err
	}
	defer tempIndexFile.Close()

	// Write valid entries to temporary files
	newIndex := make(map[types.Key]int64)
	newOffset := int64(0)

	for key, offset := range s.index {
		entry, err := s.readEntry(offset)
		if err == nil && !entry.IsExpired() {
			// Write entry to temp file
			entryData, err := json.Marshal(entry)
			if err != nil {
				continue
			}

			length := uint32(len(entryData))
			binary.Write(tempDataFile, binary.LittleEndian, length)
			tempDataFile.Write(entryData)

			newIndex[key] = newOffset
			newOffset += int64(4 + len(entryData))
		}
	}

	// Save new index
	indexData, err := json.Marshal(newIndex)
	if err != nil {
		return err
	}
	tempIndexFile.Write(indexData)

	// Close temp files
	tempDataFile.Close()
	tempIndexFile.Close()

	// Close original files
	s.dataFile.Close()
	s.indexFile.Close()

	// Replace original files with compacted ones
	if err := os.Rename(tempDataPath, filepath.Join(s.dataDir, "data.db")); err != nil {
		return err
	}

	if err := os.Rename(tempIndexPath, filepath.Join(s.dataDir, "index.db")); err != nil {
		return err
	}

	// Reopen files
	dataPath := filepath.Join(s.dataDir, "data.db")
	indexPath := filepath.Join(s.dataDir, "index.db")

	s.dataFile, err = os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	s.indexFile, err = os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		s.dataFile.Close()
		return err
	}

	// Update state
	s.index = newIndex
	s.nextOffset = newOffset

	return nil
}
