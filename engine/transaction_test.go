package engine_test

import (
	"database_engine/engine"
	"database_engine/types"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionBegin(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, tx)
}

func TestTransactionBasicOperations(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Set initial data
	err := db.Set("key1", []byte("value1"))
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Read within transaction
	value, err := tx.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1"), value)

	// Write within transaction
	err = tx.Set("key2", []byte("value2"))
	assert.NoError(t, err)

	// Read written value
	value, err = tx.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value2"), value)

	// Delete within transaction
	err = tx.Delete("key1")
	assert.NoError(t, err)

	// Verify delete in transaction
	_, err = tx.Get("key1")
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)

	// Commit transaction
	err = tx.Commit()
	assert.NoError(t, err)

	// Verify changes are persisted
	_, err = db.Get("key1")
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)

	value, err = db.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value2"), value)
}

func TestTransactionRollback(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Set initial data
	err := db.Set("key1", []byte("value1"))
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Make changes
	err = tx.Set("key2", []byte("value2"))
	assert.NoError(t, err)

	err = tx.Delete("key1")
	assert.NoError(t, err)

	// Rollback transaction
	err = tx.Rollback()
	assert.NoError(t, err)

	// Verify changes are not persisted
	value, err := db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1"), value)

	_, err = db.Get("key2")
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)
}

func TestTransactionIsolation(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Set initial data
	err := db.Set("key1", []byte("value1"))
	require.NoError(t, err)

	// Begin transaction 1
	tx1, err := db.Begin()
	require.NoError(t, err)

	// Begin transaction 2
	tx2, err := db.Begin()
	require.NoError(t, err)

	// Modify in transaction 1
	err = tx1.Set("key1", []byte("value1_modified"))
	assert.NoError(t, err)

	// Read in transaction 2 (should see original value)
	value, err := tx2.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1"), value)

	// Commit transaction 1
	err = tx1.Commit()
	assert.NoError(t, err)

	// Transaction 2 should still see original value
	value, err = tx2.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1"), value)

	// Commit transaction 2 (should fail due to conflict)
	err = tx2.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "modified by another transaction")
}

func TestTransactionConsistency(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Set initial data
	err := db.Set("key1", []byte("value1"))
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Read key
	_, err = tx.Get("key1")
	assert.NoError(t, err)

	// Modify outside transaction
	err = db.Set("key1", []byte("value1_modified"))
	require.NoError(t, err)

	// Try to commit (should fail due to consistency check)
	err = tx.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "modified by another transaction")
}

func TestTransactionAtomicity(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Make multiple changes
	err = tx.Set("key1", []byte("value1"))
	assert.NoError(t, err)

	err = tx.Set("key2", []byte("value2"))
	assert.NoError(t, err)

	err = tx.Set("key3", []byte("value3"))
	assert.NoError(t, err)

	// Commit transaction
	err = tx.Commit()
	assert.NoError(t, err)

	// Verify all changes are applied atomically
	value1, err := db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1"), value1)

	value2, err := db.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value2"), value2)

	value3, err := db.Get("key3")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value3"), value3)
}

func TestTransactionConcurrentTransactions(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Set initial data
	err := db.Set("counter", []byte("0"))
	require.NoError(t, err)

	// Create multiple transactions
	done := make(chan bool, 5)

	for i := 0; i < 5; i++ {
		go func(i int) {
			tx, err := db.Begin()
			if err != nil {
				done <- false
				return
			}

			// Read counter
			value, err := tx.Get("counter")
			if err != nil {
				tx.Rollback()
				done <- false
				return
			}

			// Increment
			newValue := []byte(string(value) + "1")

			// Write back
			err = tx.Set("counter", newValue)
			if err != nil {
				tx.Rollback()
				done <- false
				return
			}

			// Commit
			err = tx.Commit()
			if err != nil {
				tx.Rollback()
				done <- false
				return
			}

			done <- true
		}(i)
	}

	// Wait for all transactions
	successCount := 0
	for i := 0; i < 5; i++ {
		if <-done {
			successCount++
		}
	}

	// Some transactions should succeed, some should fail due to conflicts
	assert.Greater(t, successCount, 0)
	assert.LessOrEqual(t, successCount, 5)
}

func TestTransactionClosedDatabase(t *testing.T) {
	db := engine.NewInMemoryDB()
	db.Close()

	tx, err := db.Begin()
	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Equal(t, types.ErrDatabaseClosed, err)
}

func TestTransactionDoubleCommit(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	err = tx.Set("key1", []byte("value1"))
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)

	// Try to commit again
	err = tx.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestTransactionCommitAfterRollback(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	err = tx.Set("key1", []byte("value1"))
	assert.NoError(t, err)

	err = tx.Rollback()
	assert.NoError(t, err)

	// Try to commit after rollback
	err = tx.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestTransactionOperationsAfterCommit(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)

	// Try to perform operations after commit
	_, err = tx.Get("key1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	err = tx.Set("key1", []byte("value1"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	err = tx.Delete("key1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestTransactionOperationsAfterRollback(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	err = tx.Rollback()
	assert.NoError(t, err)

	// Try to perform operations after rollback
	_, err = tx.Get("key1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	err = tx.Set("key1", []byte("value1"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	err = tx.Delete("key1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestTransactionReadWriteSet(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Set initial data
	err := db.Set("key1", []byte("value1"))
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	// Read key
	value, err := tx.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1"), value)

	// Write same key
	err = tx.Set("key1", []byte("value1_modified"))
	assert.NoError(t, err)

	// Read should return modified value
	value, err = tx.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1_modified"), value)

	// Commit
	err = tx.Commit()
	assert.NoError(t, err)

	// Verify final value
	value, err = db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1_modified"), value)
}

func TestTransactionDeleteThenWrite(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Set initial data
	err := db.Set("key1", []byte("value1"))
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	// Delete key
	err = tx.Delete("key1")
	assert.NoError(t, err)

	// Write same key
	err = tx.Set("key1", []byte("value1_new"))
	assert.NoError(t, err)

	// Commit
	err = tx.Commit()
	assert.NoError(t, err)

	// Verify final value
	value, err := db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value1_new"), value)
}

func TestTransactionWriteThenDelete(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	// Write key
	err = tx.Set("key1", []byte("value1"))
	assert.NoError(t, err)

	// Delete same key
	err = tx.Delete("key1")
	assert.NoError(t, err)

	// Commit
	err = tx.Commit()
	assert.NoError(t, err)

	// Verify key doesn't exist
	_, err = db.Get("key1")
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)
}

func TestTransactionWithDiskStorage(t *testing.T) {
	tempDir := t.TempDir()

	db, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	defer db.Close()

	// Set initial data
	err = db.Set("key1", []byte("value1"))
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Make changes
	err = tx.Set("key2", []byte("value2"))
	assert.NoError(t, err)

	err = tx.Delete("key1")
	assert.NoError(t, err)

	// Commit
	err = tx.Commit()
	assert.NoError(t, err)

	// Verify changes persisted
	_, err = db.Get("key1")
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)

	value, err := db.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value2"), value)
}
