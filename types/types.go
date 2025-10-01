package types

import (
	"errors"
	"time"
)

// Key represents a database key
type Key string

// Value represents a database value
type Value []byte

// Entry represents a key-value pair with metadata
type Entry struct {
	Key       Key
	Value     Value
	Timestamp time.Time
	TTL       *time.Duration // Optional time-to-live
}

// IsExpired checks if the entry has expired based on TTL
func (e *Entry) IsExpired() bool {
	if e.TTL == nil {
		return false
	}
	return time.Since(e.Timestamp) > *e.TTL
}

// Database errors
var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrKeyExpired     = errors.New("key has expired")
	ErrInvalidKey     = errors.New("invalid key")
	ErrInvalidValue   = errors.New("invalid value")
	ErrDatabaseClosed = errors.New("database is closed")
	ErrTransactionAborted = errors.New("transaction aborted")
)

// StorageEngine represents the interface for different storage engines
type StorageEngine interface {
	// Basic operations
	Get(key Key) (Value, error)
	Set(key Key, value Value) error
	Delete(key Key) error
	Exists(key Key) (bool, error)
	
	// Batch operations
	BatchGet(keys []Key) (map[Key]Value, error)
	BatchSet(entries []Entry) error
	BatchDelete(keys []Key) error
	
	// Utility operations
	Clear() error
	Size() (int64, error)
	Keys() ([]Key, error)
	
	// Lifecycle
	Close() error
	IsClosed() bool
}

// Transaction represents a database transaction
type Transaction interface {
	Get(key Key) (Value, error)
	Set(key Key, value Value) error
	Delete(key Key) error
	Commit() error
	Rollback() error
}

// Database represents the main database interface
type Database interface {
	StorageEngine
	
	// Transaction support
	Begin() (Transaction, error)
	
	// Configuration
	SetConfig(config Config) error
	GetConfig() Config
}

// Config represents database configuration
type Config struct {
	// Storage settings
	MaxMemorySize    int64         // Maximum memory usage in bytes
	MaxKeySize       int           // Maximum key size in bytes
	MaxValueSize     int           // Maximum value size in bytes
	
	// Performance settings
	WriteBufferSize  int           // Write buffer size
	ReadBufferSize   int           // Read buffer size
	
	// Persistence settings
	EnablePersistence bool         // Enable disk persistence
	DataDirectory     string       // Directory for persistent data
	WALEnabled        bool         // Enable write-ahead logging
	
	// Cleanup settings
	EnableTTL         bool         // Enable TTL support
	CleanupInterval   time.Duration // TTL cleanup interval
	
	// Logging
	LogLevel          string       // Log level (debug, info, warn, error)
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		MaxMemorySize:    1024 * 1024 * 1024, // 1GB
		MaxKeySize:       1024,               // 1KB
		MaxValueSize:     1024 * 1024,        // 1MB
		WriteBufferSize:  64 * 1024,          // 64KB
		ReadBufferSize:   64 * 1024,          // 64KB
		EnablePersistence: false,
		DataDirectory:    "./data",
		WALEnabled:       false,
		EnableTTL:        true,
		CleanupInterval:  time.Minute * 5,
		LogLevel:         "info",
	}
}
