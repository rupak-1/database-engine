package storage

import (
	"database_engine/types"
	"sync"
	"time"
)

// InMemoryStorage implements the StorageEngine interface using in-memory storage
type InMemoryStorage struct {
	data map[types.Key]*types.Entry
	mu   sync.RWMutex
}

// NewInMemoryStorage creates a new in-memory storage instance
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data: make(map[types.Key]*types.Entry),
	}
}

// Get retrieves a value by key
func (s *InMemoryStorage) Get(key types.Key) (types.Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists {
		return nil, types.ErrKeyNotFound
	}

	// Check if entry has expired
	if entry.IsExpired() {
		// Clean up expired entry
		delete(s.data, key)
		return nil, types.ErrKeyExpired
	}

	return entry.Value, nil
}

// Set stores a key-value pair
func (s *InMemoryStorage) Set(key types.Key, value types.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := &types.Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
		TTL:       nil, // No TTL by default
	}

	s.data[key] = entry
	return nil
}

// SetWithTTL stores a key-value pair with a time-to-live
func (s *InMemoryStorage) SetWithTTL(key types.Key, value types.Value, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := &types.Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
		TTL:       &ttl,
	}

	s.data[key] = entry
	return nil
}

// Delete removes a key-value pair
func (s *InMemoryStorage) Delete(key types.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}

// Exists checks if a key exists
func (s *InMemoryStorage) Exists(key types.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists {
		return false, nil
	}

	// Check if entry has expired
	if entry.IsExpired() {
		return false, nil
	}

	return true, nil
}

// BatchGet retrieves multiple values by keys
func (s *InMemoryStorage) BatchGet(keys []types.Key) (map[types.Key]types.Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[types.Key]types.Value)

	for _, key := range keys {
		entry, exists := s.data[key]
		if exists && !entry.IsExpired() {
			result[key] = entry.Value
		}
	}

	return result, nil
}

// BatchSet stores multiple key-value pairs
func (s *InMemoryStorage) BatchSet(entries []types.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, entry := range entries {
		// Create a copy of the entry to avoid pointer issues
		entryCopy := entry
		// Set timestamp if not already set
		if entryCopy.Timestamp.IsZero() {
			entryCopy.Timestamp = now
		}

		s.data[entryCopy.Key] = &entryCopy
	}

	return nil
}

// BatchDelete removes multiple key-value pairs
func (s *InMemoryStorage) BatchDelete(keys []types.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		delete(s.data, key)
	}

	return nil
}

// Clear removes all key-value pairs
func (s *InMemoryStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[types.Key]*types.Entry)
	return nil
}

// Size returns the number of key-value pairs
func (s *InMemoryStorage) Size() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Count only non-expired entries
	count := int64(0)
	for _, entry := range s.data {
		if !entry.IsExpired() {
			count++
		}
	}

	return count, nil
}

// Keys returns all keys in the storage
func (s *InMemoryStorage) Keys() ([]types.Key, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []types.Key
	for key, entry := range s.data {
		if !entry.IsExpired() {
			keys = append(keys, key)
		}
	}

	return keys, nil
}

// Close closes the storage (no-op for in-memory storage)
func (s *InMemoryStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear all data
	s.data = make(map[types.Key]*types.Entry)
	return nil
}

// IsClosed returns false for in-memory storage (always available)
func (s *InMemoryStorage) IsClosed() bool {
	return false
}

// CleanupExpired removes all expired entries
func (s *InMemoryStorage) CleanupExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for key, entry := range s.data {
		if entry.IsExpired() {
			delete(s.data, key)
			count++
		}
	}

	return count
}

// GetMemoryUsage returns approximate memory usage in bytes
func (s *InMemoryStorage) GetMemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var total int64
	for key, entry := range s.data {
		total += int64(len(key))
		total += int64(len(entry.Value))
		total += 64 // Approximate overhead per entry
	}

	return total
}
