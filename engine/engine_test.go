package engine_test

import (
	"database_engine/engine"
	"database_engine/types"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInMemoryDB(t *testing.T) {
	db := engine.NewInMemoryDB()
	assert.NotNil(t, db)
	assert.False(t, db.IsClosed())

	// Test default config
	config := db.GetConfig()
	assert.Equal(t, types.DefaultConfig(), config)

	err := db.Close()
	assert.NoError(t, err)
	assert.True(t, db.IsClosed())
}

func TestNewInMemoryDBWithConfig(t *testing.T) {
	config := types.Config{
		MaxKeySize:   512,
		MaxValueSize: 1024,
		EnableTTL:    true,
	}

	db := engine.NewInMemoryDBWithConfig(config)
	assert.NotNil(t, db)
	assert.False(t, db.IsClosed())

	// Test custom config
	retrievedConfig := db.GetConfig()
	assert.Equal(t, config.MaxKeySize, retrievedConfig.MaxKeySize)
	assert.Equal(t, config.MaxValueSize, retrievedConfig.MaxValueSize)
	assert.Equal(t, config.EnableTTL, retrievedConfig.EnableTTL)

	err := db.Close()
	assert.NoError(t, err)
}

func TestBasicOperations(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	key := types.Key("test-key")
	value := types.Value("test-value")

	// Test Set
	err := db.Set(key, value)
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

func TestBatchOperations(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Test BatchSet
	entries := []types.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
	}

	err := db.BatchSet(entries)
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

func TestClear(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Add some data
	err := db.Set("key1", []byte("value1"))
	assert.NoError(t, err)
	err = db.Set("key2", []byte("value2"))
	assert.NoError(t, err)

	// Verify data exists
	size, err := db.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), size)

	// Clear all data
	err = db.Clear()
	assert.NoError(t, err)

	// Verify data is cleared
	size, err = db.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)

	keys, err := db.Keys()
	assert.NoError(t, err)
	assert.Len(t, keys, 0)
}

func TestValidation(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Test empty key
	err := db.Set("", []byte("value"))
	assert.Error(t, err)
	assert.Equal(t, types.ErrInvalidKey, err)

	// Test key too large
	largeKey := string(make([]byte, 2048)) // Larger than default MaxKeySize
	err = db.Set(types.Key(largeKey), []byte("value"))
	assert.Error(t, err)
	assert.Equal(t, types.ErrInvalidKey, err)

	// Test value too large
	largeValue := make([]byte, 2*1024*1024) // Larger than default MaxValueSize
	err = db.Set("key", largeValue)
	assert.Error(t, err)
	assert.Equal(t, types.ErrInvalidValue, err)
}

func TestClosedDatabase(t *testing.T) {
	db := engine.NewInMemoryDB()

	// Close the database
	err := db.Close()
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
}

func TestTransactionPlaceholder(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Test transaction begin (should return error as not implemented)
	tx, err := db.Begin()
	assert.Error(t, err)
	assert.Equal(t, types.ErrTransactionAborted, err)
	assert.Nil(t, tx)
}

func TestConcurrentOperations(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Test concurrent writes
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(i int) {
			key := types.Key(fmt.Sprintf("key-%d", i))
			value := types.Value(fmt.Sprintf("value-%d", i))

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

func TestConfigUpdate(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Get initial config
	initialConfig := db.GetConfig()

	// Update config
	newConfig := types.Config{
		MaxKeySize:   256,
		MaxValueSize: 512,
		EnableTTL:    false,
	}

	err := db.SetConfig(newConfig)
	assert.NoError(t, err)

	// Verify config was updated
	updatedConfig := db.GetConfig()
	assert.Equal(t, newConfig.MaxKeySize, updatedConfig.MaxKeySize)
	assert.Equal(t, newConfig.MaxValueSize, updatedConfig.MaxValueSize)
	assert.Equal(t, newConfig.EnableTTL, updatedConfig.EnableTTL)
	assert.NotEqual(t, initialConfig.MaxKeySize, updatedConfig.MaxKeySize)
}

func TestErrorHandling(t *testing.T) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Test getting non-existent key
	_, err := db.Get("non-existent-key")
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)

	// Test deleting non-existent key (should not error)
	err = db.Delete("non-existent-key")
	assert.NoError(t, err)

	// Test batch operations with empty slices
	values, err := db.BatchGet([]types.Key{})
	assert.NoError(t, err)
	assert.Len(t, values, 0)

	err = db.BatchSet([]types.Entry{})
	assert.NoError(t, err)

	err = db.BatchDelete([]types.Key{})
	assert.NoError(t, err)
}
