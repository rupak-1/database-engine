package storage_test

import (
	"database_engine/storage"
	"database_engine/types"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDiskStorage(t *testing.T) {
	tempDir := t.TempDir()

	diskStorage, err := storage.NewDiskStorage(tempDir)
	assert.NoError(t, err)
	assert.NotNil(t, diskStorage)
	assert.False(t, diskStorage.IsClosed())

	// Check that files were created
	dataFile := filepath.Join(tempDir, "data.db")
	indexFile := filepath.Join(tempDir, "index.db")

	assert.FileExists(t, dataFile)
	assert.FileExists(t, indexFile)

	err = diskStorage.Close()
	assert.NoError(t, err)
	assert.True(t, diskStorage.IsClosed())
}

func TestDiskStorageBasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	key := types.Key("test-key")
	value := types.Value("test-value")

	// Test Set
	err = diskStorage.Set(key, value)
	assert.NoError(t, err)

	// Test Get
	retrievedValue, err := diskStorage.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Test Exists
	exists, err := diskStorage.Exists(key)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Test Size
	size, err := diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)

	// Test Keys
	keys, err := diskStorage.Keys()
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
	assert.Equal(t, key, keys[0])

	// Test Delete
	err = diskStorage.Delete(key)
	assert.NoError(t, err)

	// Test Get after delete
	_, err = diskStorage.Get(key)
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)

	// Test Exists after delete
	exists, err = diskStorage.Exists(key)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Test Size after delete
	size, err = diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)
}

func TestDiskStorageBatchOperations(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	// Test BatchSet
	entries := []types.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
	}

	err = diskStorage.BatchSet(entries)
	assert.NoError(t, err)

	// Test Size
	size, err := diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), size)

	// Test BatchGet
	keys := []types.Key{"key1", "key2", "key3"}
	values, err := diskStorage.BatchGet(keys)
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, types.Value("value1"), values["key1"])
	assert.Equal(t, types.Value("value2"), values["key2"])
	assert.Equal(t, types.Value("value3"), values["key3"])

	// Test BatchDelete
	err = diskStorage.BatchDelete([]types.Key{"key1", "key2"})
	assert.NoError(t, err)

	// Test Size after batch delete
	size, err = diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)

	// Test remaining key
	value, err := diskStorage.Get("key3")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value3"), value)
}

func TestDiskStoragePersistence(t *testing.T) {
	tempDir := t.TempDir()

	// Create storage and add data
	diskStorage1, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage1.Set("persistent-key", []byte("persistent-value"))
	assert.NoError(t, err)

	err = diskStorage1.Close()
	assert.NoError(t, err)

	// Create new storage instance and verify data persists
	diskStorage2, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage2.Close()

	value, err := diskStorage2.Get("persistent-key")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("persistent-value"), value)

	exists, err := diskStorage2.Exists("persistent-key")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestDiskStorageTTL(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	// Set with TTL
	ttl := time.Millisecond * 100
	err = diskStorage.SetWithTTL("ttl-key", []byte("ttl-value"), ttl)
	assert.NoError(t, err)

	// Should exist initially
	exists, err := diskStorage.Exists("ttl-key")
	assert.NoError(t, err)
	assert.True(t, exists)

	// Wait for expiration
	time.Sleep(ttl + time.Millisecond*50)

	// Should be expired
	_, err = diskStorage.Get("ttl-key")
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyExpired, err)

	exists, err = diskStorage.Exists("ttl-key")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestDiskStorageClear(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	// Add some data
	err = diskStorage.Set("key1", []byte("value1"))
	assert.NoError(t, err)
	err = diskStorage.Set("key2", []byte("value2"))
	assert.NoError(t, err)

	// Verify data exists
	size, err := diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), size)

	// Clear all data
	err = diskStorage.Clear()
	assert.NoError(t, err)

	// Verify data is cleared
	size, err = diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)

	keys, err := diskStorage.Keys()
	assert.NoError(t, err)
	assert.Len(t, keys, 0)
}

func TestDiskStorageCleanupExpired(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	// Add some data with TTL
	ttl := time.Millisecond * 100
	err = diskStorage.SetWithTTL("expired-key", []byte("expired-value"), ttl)
	assert.NoError(t, err)

	// Add some data without TTL
	err = diskStorage.Set("normal-key", []byte("normal-value"))
	assert.NoError(t, err)

	// Wait for expiration
	time.Sleep(ttl + time.Millisecond*50)

	// Cleanup expired entries
	count := diskStorage.CleanupExpired()
	assert.Equal(t, 1, count)

	// Verify only non-expired entries remain
	size, err := diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)

	keys, err := diskStorage.Keys()
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
	assert.Equal(t, types.Key("normal-key"), keys[0])
}

func TestDiskStorageCompact(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	// Add some data
	err = diskStorage.Set("key1", []byte("value1"))
	assert.NoError(t, err)
	err = diskStorage.Set("key2", []byte("value2"))
	assert.NoError(t, err)

	// Delete one key
	err = diskStorage.Delete("key1")
	assert.NoError(t, err)

	// Compact
	err = diskStorage.Compact()
	assert.NoError(t, err)

	// Verify remaining data
	size, err := diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)

	value, err := diskStorage.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value2"), value)
}

func TestDiskStorageGetDiskUsage(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	// Initially should have some usage (empty files)
	usage1, err := diskStorage.GetDiskUsage()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, usage1, int64(0))

	// Add some data
	err = diskStorage.Set("large-key", []byte("large-value-with-more-data"))
	assert.NoError(t, err)

	// Usage should increase
	usage2, err := diskStorage.GetDiskUsage()
	assert.NoError(t, err)
	assert.Greater(t, usage2, usage1)
}

func TestDiskStorageClosedOperations(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	// Close the storage
	err = diskStorage.Close()
	assert.NoError(t, err)
	assert.True(t, diskStorage.IsClosed())

	// Test operations on closed storage
	_, err = diskStorage.Get("key")
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)

	err = diskStorage.Set("key", []byte("value"))
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)

	err = diskStorage.Delete("key")
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)

	_, err = diskStorage.Exists("key")
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)

	err = diskStorage.Clear()
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)

	_, err = diskStorage.Size()
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)

	_, err = diskStorage.Keys()
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
}

func TestDiskStorageConcurrentOperations(t *testing.T) {
	tempDir := t.TempDir()
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)
	defer diskStorage.Close()

	// Test concurrent writes
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(i int) {
			key := types.Key(fmt.Sprintf("concurrent-key-%d", i))
			value := types.Value(fmt.Sprintf("concurrent-value-%d", i))

			err := diskStorage.Set(key, value)
			assert.NoError(t, err)

			retrievedValue, err := diskStorage.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, value, retrievedValue)

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all data is present
	size, err := diskStorage.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
}
