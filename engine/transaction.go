package engine

import (
	"database_engine/types"
	"fmt"
	"sync"
)

// TransactionState represents the state of a transaction
type TransactionState int

const (
	TransactionActive TransactionState = iota
	TransactionCommitted
	TransactionRolledBack
)

// Transaction represents a database transaction with ACID properties
type Transaction struct {
	db             *Database
	mu             sync.RWMutex
	state          TransactionState
	readSet        map[types.Key]types.Value // Keys read during transaction
	writeSet       map[types.Key]types.Value // Keys written during transaction
	deleteSet      map[types.Key]bool        // Keys deleted during transaction
	originalValues map[types.Key]types.Value // Original values for rollback
	closed         bool
}

// Begin starts a new transaction
func (db *Database) Begin() (types.Transaction, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, types.ErrDatabaseClosed
	}

	tx := &Transaction{
		db:             db,
		state:          TransactionActive,
		readSet:        make(map[types.Key]types.Value),
		writeSet:       make(map[types.Key]types.Value),
		deleteSet:      make(map[types.Key]bool),
		originalValues: make(map[types.Key]types.Value),
		closed:         false,
	}

	return tx, nil
}

// Get retrieves a value by key within the transaction
func (tx *Transaction) Get(key types.Key) (types.Value, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return nil, fmt.Errorf("transaction is closed")
	}

	if tx.state != TransactionActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	// Check if key was written in this transaction
	if value, ok := tx.writeSet[key]; ok {
		tx.readSet[key] = value
		return value, nil
	}

	// Check if key was deleted in this transaction
	if tx.deleteSet[key] {
		return nil, types.ErrKeyNotFound
	}

	// Check if we've already read this key (for isolation)
	if value, ok := tx.readSet[key]; ok {
		return value, nil
	}

	// Read from underlying storage (with read lock)
	tx.db.mu.RLock()
	value, err := tx.db.storage.Get(key)
	tx.db.mu.RUnlock()

	if err != nil {
		if err == types.ErrKeyNotFound {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	// Store in read set for isolation
	tx.readSet[key] = value
	return value, nil
}

// Set stores a key-value pair within the transaction
func (tx *Transaction) Set(key types.Key, value types.Value) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return fmt.Errorf("transaction is closed")
	}

	if tx.state != TransactionActive {
		return fmt.Errorf("transaction is not active")
	}

	// Validate key
	if err := tx.db.validateKey(key); err != nil {
		return err
	}

	// Validate value
	if err := tx.db.validateValue(value); err != nil {
		return err
	}

	// Store original value for rollback if not already stored
	if _, exists := tx.originalValues[key]; !exists {
		if originalValue, err := tx.db.storage.Get(key); err == nil {
			tx.originalValues[key] = originalValue
		}
	}

	// Add to write set
	tx.writeSet[key] = value
	// Remove from delete set if it was deleted
	delete(tx.deleteSet, key)

	return nil
}

// Delete removes a key-value pair within the transaction
func (tx *Transaction) Delete(key types.Key) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return fmt.Errorf("transaction is closed")
	}

	if tx.state != TransactionActive {
		return fmt.Errorf("transaction is not active")
	}

	// Validate key
	if err := tx.db.validateKey(key); err != nil {
		return err
	}

	// Store original value for rollback if not already stored
	if _, exists := tx.originalValues[key]; !exists {
		if originalValue, err := tx.db.storage.Get(key); err == nil {
			tx.originalValues[key] = originalValue
		}
	}

	// Add to delete set
	tx.deleteSet[key] = true
	// Remove from write set if it was written
	delete(tx.writeSet, key)

	return nil
}

// Commit commits the transaction and applies all changes
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return fmt.Errorf("transaction is closed")
	}

	if tx.state != TransactionActive {
		return fmt.Errorf("transaction is not active")
	}

	// Lock the database for commit
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	// Validate consistency: check if any read keys have been modified
	for key, originalValue := range tx.readSet {
		// Skip if this key was modified in this transaction
		if _, ok := tx.writeSet[key]; ok {
			continue
		}
		if tx.deleteSet[key] {
			continue
		}

		// Check if key still exists with same value
		currentValue, err := tx.db.storage.Get(key)
		if err == types.ErrKeyNotFound {
			// Key was in readSet, so it existed when we read it
			// If it's now not found, it was deleted by another transaction
			tx.state = TransactionRolledBack
			tx.closed = true
			return fmt.Errorf("transaction aborted: key %s was deleted by another transaction", key)
		}
		if err != nil {
			tx.state = TransactionRolledBack
			tx.closed = true
			return fmt.Errorf("transaction aborted: failed to validate key %s: %w", key, err)
		}

		// Check if value has changed
		if !equalValues(originalValue, currentValue) {
			tx.state = TransactionRolledBack
			tx.closed = true
			return fmt.Errorf("transaction aborted: key %s was modified by another transaction", key)
		}
	}

	// Apply all writes atomically
	for key, value := range tx.writeSet {
		if err := tx.db.storage.Set(key, value); err != nil {
			// Rollback on error
			tx.rollbackInternal()
			return fmt.Errorf("failed to commit: %w", err)
		}
	}

	// Apply all deletes atomically
	for key := range tx.deleteSet {
		if err := tx.db.storage.Delete(key); err != nil {
			// Rollback on error
			tx.rollbackInternal()
			return fmt.Errorf("failed to commit: %w", err)
		}
	}

	// Mark transaction as committed
	tx.state = TransactionCommitted
	tx.closed = true

	return nil
}

// Rollback rolls back the transaction and discards all changes
func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return fmt.Errorf("transaction is closed")
	}

	if tx.state != TransactionActive {
		return fmt.Errorf("transaction is not active")
	}

	return tx.rollbackInternal()
}

// rollbackInternal performs the actual rollback (must be called with lock held)
func (tx *Transaction) rollbackInternal() error {
	// No need to restore anything since we're using write-ahead logging
	// The transaction changes were never applied to the storage

	tx.state = TransactionRolledBack
	tx.closed = true

	// Clear all sets
	tx.readSet = nil
	tx.writeSet = nil
	tx.deleteSet = nil
	tx.originalValues = nil

	return nil
}

// IsActive returns true if the transaction is active
func (tx *Transaction) IsActive() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.state == TransactionActive && !tx.closed
}

// IsCommitted returns true if the transaction is committed
func (tx *Transaction) IsCommitted() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.state == TransactionCommitted
}

// IsRolledBack returns true if the transaction is rolled back
func (tx *Transaction) IsRolledBack() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.state == TransactionRolledBack
}

// Helper function to compare values
func equalValues(a, b types.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
