package wal

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

// OperationType represents the type of operation in the WAL
type OperationType uint8

const (
	OpSet    OperationType = 1
	OpDelete OperationType = 2
)

// WALEntry represents a single entry in the Write-Ahead Log
type WALEntry struct {
	Type      OperationType `json:"type"`
	Key       types.Key     `json:"key"`
	Value     types.Value   `json:"value,omitempty"`
	Timestamp time.Time     `json:"timestamp"`
	TTL       *time.Duration `json:"ttl,omitempty"`
}

// WAL represents the Write-Ahead Log
type WAL struct {
	file        *os.File
	mu          sync.RWMutex
	closed      bool
	filePath    string
	maxSize     int64
	currentSize int64
}

// NewWAL creates a new Write-Ahead Log
func NewWAL(filePath string, maxSize int64) (*WAL, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Open or create WAL file
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get WAL file stats: %w", err)
	}

	wal := &WAL{
		file:        file,
		filePath:    filePath,
		maxSize:     maxSize,
		currentSize: stat.Size(),
		closed:      false,
	}

	return wal, nil
}

// writeEntry writes a WAL entry to the file
func (w *WAL) writeEntry(entry *WALEntry) error {
	// Serialize entry
	entryData, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	// Write length prefix (4 bytes)
	length := uint32(len(entryData))
	if err := binary.Write(w.file, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("failed to write WAL entry length: %w", err)
	}

	// Write entry data
	if _, err := w.file.Write(entryData); err != nil {
		return fmt.Errorf("failed to write WAL entry data: %w", err)
	}

	// Update current size
	w.currentSize += int64(4 + len(entryData))

	// Sync to disk for durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL to disk: %w", err)
	}

	return nil
}

// LogSet logs a SET operation
func (w *WAL) LogSet(key types.Key, value types.Value, ttl *time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	entry := &WALEntry{
		Type:      OpSet,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
		TTL:       ttl,
	}

	return w.writeEntry(entry)
}

// LogDelete logs a DELETE operation
func (w *WAL) LogDelete(key types.Key) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	entry := &WALEntry{
		Type:      OpDelete,
		Key:       key,
		Timestamp: time.Now(),
	}

	return w.writeEntry(entry)
}

// ReadEntries reads all entries from the WAL file
func (w *WAL) ReadEntries() ([]*WALEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed {
		return nil, fmt.Errorf("WAL is closed")
	}

	// Seek to beginning of file
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to beginning of WAL: %w", err)
	}

	var entries []*WALEntry

	for {
		// Read length prefix
		var length uint32
		if err := binary.Read(w.file, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break // End of file
			}
			return nil, fmt.Errorf("failed to read WAL entry length: %w", err)
		}

		// Read entry data
		entryData := make([]byte, length)
		if _, err := io.ReadFull(w.file, entryData); err != nil {
			return nil, fmt.Errorf("failed to read WAL entry data: %w", err)
		}

		// Deserialize entry
		var entry WALEntry
		if err := json.Unmarshal(entryData, &entry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal WAL entry: %w", err)
		}

		entries = append(entries, &entry)
	}

	return entries, nil
}

// ReplayEntries replays WAL entries to a storage engine
func (w *WAL) ReplayEntries(storage types.StorageEngine) error {
	entries, err := w.ReadEntries()
	if err != nil {
		return fmt.Errorf("failed to read WAL entries: %w", err)
	}

	for _, entry := range entries {
		switch entry.Type {
		case OpSet:
			// Use SetWithTTL if TTL is provided, otherwise use Set
			if entry.TTL != nil {
				if setter, ok := storage.(interface {
					SetWithTTL(key types.Key, value types.Value, ttl time.Duration) error
				}); ok {
					if err := setter.SetWithTTL(entry.Key, entry.Value, *entry.TTL); err != nil {
						return fmt.Errorf("failed to replay SET operation for key %s: %w", entry.Key, err)
					}
				} else {
					if err := storage.Set(entry.Key, entry.Value); err != nil {
						return fmt.Errorf("failed to replay SET operation for key %s: %w", entry.Key, err)
					}
				}
			} else {
				if err := storage.Set(entry.Key, entry.Value); err != nil {
					return fmt.Errorf("failed to replay SET operation for key %s: %w", entry.Key, err)
				}
			}

		case OpDelete:
			if err := storage.Delete(entry.Key); err != nil {
				return fmt.Errorf("failed to replay DELETE operation for key %s: %w", entry.Key, err)
			}

		default:
			return fmt.Errorf("unknown WAL operation type: %d", entry.Type)
		}
	}

	return nil
}

// Clear clears the WAL file
func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Close current file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Remove the file
	if err := os.Remove(w.filePath); err != nil {
		return fmt.Errorf("failed to remove WAL file: %w", err)
	}

	// Create new empty file
	file, err := os.Create(w.filePath)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %w", err)
	}

	w.file = file
	w.currentSize = 0

	return nil
}

// ShouldRotate checks if the WAL should be rotated based on size
func (w *WAL) ShouldRotate() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.currentSize >= w.maxSize
}

// Rotate rotates the WAL file
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Generate new file path with timestamp
	timestamp := time.Now().Format("20060102_150405")
	newPath := fmt.Sprintf("%s.%s", w.filePath, timestamp)

	// Close current file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close current WAL file: %w", err)
	}

	// Rename current file to archived name
	if err := os.Rename(w.filePath, newPath); err != nil {
		return fmt.Errorf("failed to rename WAL file: %w", err)
	}

	// Create new WAL file
	file, err := os.Create(w.filePath)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %w", err)
	}

	w.file = file
	w.currentSize = 0

	return nil
}

// GetSize returns the current size of the WAL file
func (w *WAL) GetSize() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.currentSize
}

// GetMaxSize returns the maximum size before rotation
func (w *WAL) GetMaxSize() int64 {
	return w.maxSize
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true
	return w.file.Close()
}

// IsClosed returns true if the WAL is closed
func (w *WAL) IsClosed() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.closed
}
