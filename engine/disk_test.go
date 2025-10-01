package engine_test

import (
	"database_engine/engine"
	"database_engine/types"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDiskDB(t *testing.T) {
	tempDir := t.TempDir()
	
	db, err := engine.NewDiskDB(tempDir)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.False(t, db.IsClosed())
	
	// Test default config
	config := db.GetConfig()
	assert.True(t, config.EnablePersistence)
	assert.Equal(t, tempDir, config.DataDirectory)
	
	err = db.Close()
	assert.NoError(t, err)
	assert.True(t, db.IsClosed())
}

func TestNewDiskDBWithConfig(t *testing.T) {
	tempDir := t.TempDir()
	
	config := types.Config{
		MaxKeySize:       512,
		MaxValueSize:     1024,
		EnablePersistence: true,
		DataDirectory:    tempDir,
		EnableTTL:        true,
	}
	
	db, err := engine.NewDiskDBWithConfig(config)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.False(t, db.IsClosed())
	
	// Test custom config
	retrievedConfig := db.GetConfig()
	assert.Equal(t, config.MaxKeySize, retrievedConfig.MaxKeySize)
	assert.Equal(t, config.MaxValueSize, retrievedConfig.MaxValueSize)
	assert.Equal(t, config.EnablePersistence, retrievedConfig.EnablePersistence)
	assert.Equal(t, config.DataDirectory, retrievedConfig.DataDirectory)
	
	err = db.Close()
	assert.NoError(t, err)
}

func TestDiskDBBasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	defer db.Close()
	
	key := types.Key("test-key")
	value := types.Value("test-value")
	
	// Test Set
	err = db.Set(key, value)
	assert.NoError(t, err)
	
	// Test Get
	retrievedValue, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)
	
	// Test Exists
	exists, err := db.Exists(key)
	assert.NoError(t, err)
	assert.True(t, exists)
	
	// Test Size
	size, err := db.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)
	
	// Test Keys
	keys, err := db.Keys()
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
	assert.Equal(t, key, keys[0])
	
	// Test Delete
	err = db.Delete(key)
	assert.NoError(t, err)
	
	// Test Get after delete
	_, err = db.Get(key)
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)
	
	// Test Exists after delete
	exists, err = db.Exists(key)
	assert.NoError(t, err)
	assert.False(t, exists)
	
	// Test Size after delete
	size, err = db.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)
}

func TestDiskDBPersistence(t *testing.T) {
	tempDir := t.TempDir()
	
	// Create database and add data
	db1, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	
	err = db1.Set("persistent-key", []byte("persistent-value"))
	assert.NoError(t, err)
	
	err = db1.Close()
	assert.NoError(t, err)
	
	// Create new database instance and verify data persists
	db2, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	defer db2.Close()
	
	value, err := db2.Get("persistent-key")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("persistent-value"), value)
	
	exists, err := db2.Exists("persistent-key")
	assert.NoError(t, err)
	assert.True(t, exists)
	
	size, err := db2.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)
}

func TestDiskDBCompact(t *testing.T) {
	tempDir := t.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	defer db.Close()
	
	// Add some data
	err = db.Set("key1", []byte("value1"))
	assert.NoError(t, err)
	err = db.Set("key2", []byte("value2"))
	assert.NoError(t, err)
	
	// Delete one key
	err = db.Delete("key1")
	assert.NoError(t, err)
	
	// Compact
	err = db.Compact()
	assert.NoError(t, err)
	
	// Verify remaining data
	size, err := db.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)
	
	value, err := db.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value2"), value)
}

func TestDiskDBGetDiskUsage(t *testing.T) {
	tempDir := t.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	defer db.Close()
	
	// Initially should have some usage (empty files)
	usage1, err := db.GetDiskUsage()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, usage1, int64(0))
	
	// Add some data
	err = db.Set("large-key", []byte("large-value-with-more-data"))
	assert.NoError(t, err)
	
	// Usage should increase
	usage2, err := db.GetDiskUsage()
	assert.NoError(t, err)
	assert.Greater(t, usage2, usage1)
}

func TestDiskDBCleanupExpired(t *testing.T) {
	tempDir := t.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	defer db.Close()
	
	// Add some data with TTL (we'll simulate this by directly using storage)
	// For now, just test the cleanup method exists
	count := db.CleanupExpired()
	assert.GreaterOrEqual(t, count, 0)
}

func TestDiskDBBatchOperations(t *testing.T) {
	tempDir := t.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	defer db.Close()
	
	// Test BatchSet
	entries := []types.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
	}
	
	err = db.BatchSet(entries)
	assert.NoError(t, err)
	
	// Test Size
	size, err := db.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), size)
	
	// Test BatchGet
	keys := []types.Key{"key1", "key2", "key3"}
	values, err := db.BatchGet(keys)
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, types.Value("value1"), values["key1"])
	assert.Equal(t, types.Value("value2"), values["key2"])
	assert.Equal(t, types.Value("value3"), values["key3"])
	
	// Test BatchDelete
	err = db.BatchDelete([]types.Key{"key1", "key2"})
	assert.NoError(t, err)
	
	// Test Size after batch delete
	size, err = db.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)
	
	// Test remaining key
	value, err := db.Get("key3")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("value3"), value)
}

func TestDiskDBConcurrentOperations(t *testing.T) {
	tempDir := t.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	defer db.Close()
	
	// Test concurrent writes
	done := make(chan bool, 10)
	
	for i := 0; i < 10; i++ {
		go func(i int) {
			key := types.Key(fmt.Sprintf("concurrent-key-%d", i))
			value := types.Value(fmt.Sprintf("concurrent-value-%d", i))
			
			err := db.Set(key, value)
			assert.NoError(t, err)
			
			retrievedValue, err := db.Get(key)
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
	size, err := db.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), size)
}

func TestDiskDBInvalidConfig(t *testing.T) {
	tempDir := t.TempDir()
	
	// Test with persistence disabled
	config := types.Config{
		EnablePersistence: false,
		DataDirectory:     tempDir,
	}
	
	_, err := engine.NewDiskDBWithConfig(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "persistence must be enabled")
}

func TestDiskDBClosedOperations(t *testing.T) {
	tempDir := t.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	require.NoError(t, err)
	
	// Close the database
	err = db.Close()
	assert.NoError(t, err)
	assert.True(t, db.IsClosed())
	
	// Test operations on closed database
	_, err = db.Get("key")
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
	
	err = db.Set("key", []byte("value"))
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
	
	err = db.Delete("key")
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
	
	_, err = db.Exists("key")
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
	
	err = db.Clear()
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
	
	_, err = db.Size()
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
	
	_, err = db.Keys()
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
	
	err = db.Compact()
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
	
	_, err = db.GetDiskUsage()
	assert.Error(t, err)
	assert.Equal(t, types.ErrDatabaseClosed, err)
}
