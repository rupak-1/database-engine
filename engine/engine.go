package engine

import (
	"database_engine/storage"
	"database_engine/types"
	"sync"
)

// Database represents the main database implementation
type Database struct {
	storage types.StorageEngine
	config  types.Config
	mu      sync.RWMutex
	closed  bool
}

// NewInMemoryDB creates a new in-memory database
func NewInMemoryDB() *Database {
	config := types.DefaultConfig()
	storage := storage.NewInMemoryStorage()
	
	return &Database{
		storage: storage,
		config:  config,
		closed:  false,
	}
}

// NewInMemoryDBWithConfig creates a new in-memory database with custom config
func NewInMemoryDBWithConfig(config types.Config) *Database {
	storage := storage.NewInMemoryStorage()
	
	return &Database{
		storage: storage,
		config:  config,
		closed:  false,
	}
}

// Get retrieves a value by key
func (db *Database) Get(key types.Key) (types.Value, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	if db.closed {
		return nil, types.ErrDatabaseClosed
	}
	
	if err := db.validateKey(key); err != nil {
		return nil, err
	}
	
	return db.storage.Get(key)
}

// Set stores a key-value pair
func (db *Database) Set(key types.Key, value types.Value) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if db.closed {
		return types.ErrDatabaseClosed
	}
	
	if err := db.validateKey(key); err != nil {
		return err
	}
	
	if err := db.validateValue(value); err != nil {
		return err
	}
	
	return db.storage.Set(key, value)
}

// Delete removes a key-value pair
func (db *Database) Delete(key types.Key) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if db.closed {
		return types.ErrDatabaseClosed
	}
	
	if err := db.validateKey(key); err != nil {
		return err
	}
	
	return db.storage.Delete(key)
}

// Exists checks if a key exists
func (db *Database) Exists(key types.Key) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	if db.closed {
		return false, types.ErrDatabaseClosed
	}
	
	if err := db.validateKey(key); err != nil {
		return false, err
	}
	
	return db.storage.Exists(key)
}

// BatchGet retrieves multiple values by keys
func (db *Database) BatchGet(keys []types.Key) (map[types.Key]types.Value, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	if db.closed {
		return nil, types.ErrDatabaseClosed
	}
	
	for _, key := range keys {
		if err := db.validateKey(key); err != nil {
			return nil, err
		}
	}
	
	return db.storage.BatchGet(keys)
}

// BatchSet stores multiple key-value pairs
func (db *Database) BatchSet(entries []types.Entry) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if db.closed {
		return types.ErrDatabaseClosed
	}
	
	for _, entry := range entries {
		if err := db.validateKey(entry.Key); err != nil {
			return err
		}
		if err := db.validateValue(entry.Value); err != nil {
			return err
		}
	}
	
	return db.storage.BatchSet(entries)
}

// BatchDelete removes multiple key-value pairs
func (db *Database) BatchDelete(keys []types.Key) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if db.closed {
		return types.ErrDatabaseClosed
	}
	
	for _, key := range keys {
		if err := db.validateKey(key); err != nil {
			return err
		}
	}
	
	return db.storage.BatchDelete(keys)
}

// Clear removes all key-value pairs
func (db *Database) Clear() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if db.closed {
		return types.ErrDatabaseClosed
	}
	
	return db.storage.Clear()
}

// Size returns the number of key-value pairs
func (db *Database) Size() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	if db.closed {
		return 0, types.ErrDatabaseClosed
	}
	
	return db.storage.Size()
}

// Keys returns all keys in the database
func (db *Database) Keys() ([]types.Key, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	if db.closed {
		return nil, types.ErrDatabaseClosed
	}
	
	return db.storage.Keys()
}

// Begin starts a new transaction (placeholder for future implementation)
func (db *Database) Begin() (types.Transaction, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	if db.closed {
		return nil, types.ErrDatabaseClosed
	}
	
	// TODO: Implement transaction support
	return nil, types.ErrTransactionAborted
}

// SetConfig updates the database configuration
func (db *Database) SetConfig(config types.Config) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if db.closed {
		return types.ErrDatabaseClosed
	}
	
	db.config = config
	return nil
}

// GetConfig returns the current database configuration
func (db *Database) GetConfig() types.Config {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	return db.config
}

// Close closes the database
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if db.closed {
		return nil
	}
	
	db.closed = true
	return db.storage.Close()
}

// IsClosed returns true if the database is closed
func (db *Database) IsClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	return db.closed
}

// validateKey validates a key
func (db *Database) validateKey(key types.Key) error {
	if len(key) == 0 {
		return types.ErrInvalidKey
	}
	
	if len(key) > db.config.MaxKeySize {
		return types.ErrInvalidKey
	}
	
	return nil
}

// validateValue validates a value
func (db *Database) validateValue(value types.Value) error {
	if len(value) > db.config.MaxValueSize {
		return types.ErrInvalidValue
	}
	
	return nil
}
