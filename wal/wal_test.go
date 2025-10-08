package wal_test

import (
	"database_engine/storage"
	"database_engine/types"
	"database_engine/wal"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWAL(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	w, err := wal.NewWAL(walPath, 1024*1024) // 1MB max size
	assert.NoError(t, err)
	assert.NotNil(t, w)
	assert.False(t, w.IsClosed())
	assert.Equal(t, int64(1024*1024), w.GetMaxSize())
	assert.Equal(t, int64(0), w.GetSize())

	err = w.Close()
	assert.NoError(t, err)
	assert.True(t, w.IsClosed())
}

func TestWALLogSet(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	w, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Test logging a SET operation
	key := types.Key("test-key")
	value := types.Value("test-value")
	ttl := time.Hour

	err = w.LogSet(key, value, &ttl)
	assert.NoError(t, err)

	// Verify file size increased
	size := w.GetSize()
	assert.Greater(t, size, int64(0))

	// Test logging without TTL
	err = w.LogSet("key2", []byte("value2"), nil)
	assert.NoError(t, err)
}

func TestWALLogDelete(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	w, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Test logging a DELETE operation
	key := types.Key("test-key")

	err = w.LogDelete(key)
	assert.NoError(t, err)

	// Verify file size increased
	size := w.GetSize()
	assert.Greater(t, size, int64(0))
}

func TestWALReadEntries(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	w, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Log some operations
	key1 := types.Key("key1")
	value1 := types.Value("value1")
	ttl1 := time.Hour

	key2 := types.Key("key2")
	value2 := types.Value("value2")

	err = w.LogSet(key1, value1, &ttl1)
	assert.NoError(t, err)

	err = w.LogSet(key2, value2, nil)
	assert.NoError(t, err)

	err = w.LogDelete(key1)
	assert.NoError(t, err)

	// Read entries
	entries, err := w.ReadEntries()
	assert.NoError(t, err)
	assert.Len(t, entries, 3)

	// Verify first entry (SET with TTL)
	assert.Equal(t, wal.OpSet, entries[0].Type)
	assert.Equal(t, key1, entries[0].Key)
	assert.Equal(t, value1, entries[0].Value)
	assert.NotNil(t, entries[0].TTL)
	assert.Equal(t, ttl1, *entries[0].TTL)

	// Verify second entry (SET without TTL)
	assert.Equal(t, wal.OpSet, entries[1].Type)
	assert.Equal(t, key2, entries[1].Key)
	assert.Equal(t, value2, entries[1].Value)
	assert.Nil(t, entries[1].TTL)

	// Verify third entry (DELETE)
	assert.Equal(t, wal.OpDelete, entries[2].Type)
	assert.Equal(t, key1, entries[2].Key)
	assert.Nil(t, entries[2].Value)
}

func TestWALReplayEntries(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	w, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Create a storage engine to replay to
	storage := storage.NewInMemoryStorage()

	// Log some operations
	key1 := types.Key("key1")
	value1 := types.Value("value1")
	ttl1 := time.Hour

	key2 := types.Key("key2")
	value2 := types.Value("value2")

	err = w.LogSet(key1, value1, &ttl1)
	assert.NoError(t, err)

	err = w.LogSet(key2, value2, nil)
	assert.NoError(t, err)

	err = w.LogDelete(key1)
	assert.NoError(t, err)

	// Replay entries
	err = w.ReplayEntries(storage)
	assert.NoError(t, err)

	// Verify storage state
	// key1 should not exist (deleted)
	_, err = storage.Get(key1)
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)

	// key2 should exist
	value, err := storage.Get(key2)
	assert.NoError(t, err)
	assert.Equal(t, value2, value)

	// Verify size
	size, err := storage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)
}

func TestWALReplayEntriesWithDiskStorage(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")
	storageDir := filepath.Join(tempDir, "storage")

	w, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Create a disk storage engine to replay to
	diskStorage, err := storage.NewDiskStorage(storageDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	// Log some operations
	key1 := types.Key("key1")
	value1 := types.Value("value1")
	ttl1 := time.Hour

	key2 := types.Key("key2")
	value2 := types.Value("value2")

	err = w.LogSet(key1, value1, &ttl1)
	assert.NoError(t, err)

	err = w.LogSet(key2, value2, nil)
	assert.NoError(t, err)

	err = w.LogDelete(key1)
	assert.NoError(t, err)

	// Replay entries
	err = w.ReplayEntries(diskStorage)
	assert.NoError(t, err)

	// Verify storage state
	// key1 should not exist (deleted)
	_, err = diskStorage.Get(key1)
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)

	// key2 should exist
	value, err := diskStorage.Get(key2)
	assert.NoError(t, err)
	assert.Equal(t, value2, value)

	// Verify size
	size, err := diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)
}

func TestWALClear(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	w, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Log some operations
	err = w.LogSet("key1", []byte("value1"), nil)
	assert.NoError(t, err)

	err = w.LogSet("key2", []byte("value2"), nil)
	assert.NoError(t, err)

	// Verify file has content
	size := w.GetSize()
	assert.Greater(t, size, int64(0))

	// Clear WAL
	err = w.Clear()
	assert.NoError(t, err)

	// Verify file is empty
	size = w.GetSize()
	assert.Equal(t, int64(0), size)

	// Verify we can still log after clear
	err = w.LogSet("key3", []byte("value3"), nil)
	assert.NoError(t, err)

	size = w.GetSize()
	assert.Greater(t, size, int64(0))
}

func TestWALRotation(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	// Create WAL with very small max size to trigger rotation
	w, err := wal.NewWAL(walPath, 100) // 100 bytes max size
	require.NoError(t, err)
	defer w.Close()

	// Log operations until we exceed max size
	for i := 0; i < 10; i++ {
		key := types.Key(fmt.Sprintf("key-%d", i))
		value := types.Value(fmt.Sprintf("value-%d", i))
		err = w.LogSet(key, value, nil)
		assert.NoError(t, err)

		if w.ShouldRotate() {
			break
		}
	}

	// Verify rotation is needed
	assert.True(t, w.ShouldRotate())

	// Perform rotation
	err = w.Rotate()
	assert.NoError(t, err)

	// Verify new file is empty
	size := w.GetSize()
	assert.Equal(t, int64(0), size)

	// Verify old file exists
	files, err := filepath.Glob(filepath.Join(tempDir, "test.wal.*"))
	assert.NoError(t, err)
	assert.Len(t, files, 1)
}

func TestWALClosedOperations(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	w, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)

	// Close the WAL
	err = w.Close()
	assert.NoError(t, err)
	assert.True(t, w.IsClosed())

	// Test operations on closed WAL
	err = w.LogSet("key", []byte("value"), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WAL is closed")

	err = w.LogDelete("key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WAL is closed")

	_, err = w.ReadEntries()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WAL is closed")

	err = w.Clear()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WAL is closed")

	err = w.Rotate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WAL is closed")
}

func TestWALConcurrentOperations(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	w, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)
	defer w.Close()

	// Test concurrent writes
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(i int) {
			key := types.Key(fmt.Sprintf("concurrent-key-%d", i))
			value := types.Value(fmt.Sprintf("concurrent-value-%d", i))
			
			err := w.LogSet(key, value, nil)
			assert.NoError(t, err)
			
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all entries were written
	entries, err := w.ReadEntries()
	assert.NoError(t, err)
	assert.Len(t, entries, 10)
}

func TestWALPersistence(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	// Create WAL and log operations
	w1, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)

	err = w1.LogSet("persistent-key", []byte("persistent-value"), nil)
	assert.NoError(t, err)

	err = w1.LogDelete("persistent-key")
	assert.NoError(t, err)

	err = w1.Close()
	assert.NoError(t, err)

	// Create new WAL instance and verify data persists
	w2, err := wal.NewWAL(walPath, 1024*1024)
	require.NoError(t, err)
	defer w2.Close()

	entries, err := w2.ReadEntries()
	assert.NoError(t, err)
	assert.Len(t, entries, 2)

	assert.Equal(t, wal.OpSet, entries[0].Type)
	assert.Equal(t, types.Key("persistent-key"), entries[0].Key)
	assert.Equal(t, types.Value("persistent-value"), entries[0].Value)

	assert.Equal(t, wal.OpDelete, entries[1].Type)
	assert.Equal(t, types.Key("persistent-key"), entries[1].Key)
}
