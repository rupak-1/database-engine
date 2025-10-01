package engine_test

import (
	"database_engine/engine"
	"database_engine/types"
	"fmt"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := types.Key(fmt.Sprintf("key-%d", i))
		value := types.Value(fmt.Sprintf("value-%d", i))
		db.Set(key, value)
	}
}

func BenchmarkGet(b *testing.B) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("key-%d", i))
		value := types.Value(fmt.Sprintf("value-%d", i))
		db.Set(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := types.Key(fmt.Sprintf("key-%d", i%1000))
		db.Get(key)
	}
}

func BenchmarkDelete(b *testing.B) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Set a key first
		key := types.Key(fmt.Sprintf("key-%d", i))
		value := types.Value(fmt.Sprintf("value-%d", i))
		db.Set(key, value)

		// Then delete it
		db.Delete(key)
	}
}

func BenchmarkBatchSet(b *testing.B) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := make([]types.Entry, 10)
		for j := 0; j < 10; j++ {
			entries[j] = types.Entry{
				Key:   types.Key(fmt.Sprintf("batch-key-%d-%d", i, j)),
				Value: types.Value(fmt.Sprintf("batch-value-%d-%d", i, j)),
			}
		}
		db.BatchSet(entries)
	}
}

func BenchmarkBatchGet(b *testing.B) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("key-%d", i))
		value := types.Value(fmt.Sprintf("value-%d", i))
		db.Set(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys := make([]types.Key, 10)
		for j := 0; j < 10; j++ {
			keys[j] = types.Key(fmt.Sprintf("key-%d", (i*10+j)%1000))
		}
		db.BatchGet(keys)
	}
}

func BenchmarkConcurrentSet(b *testing.B) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := types.Key(fmt.Sprintf("concurrent-key-%d", i))
			value := types.Value(fmt.Sprintf("concurrent-value-%d", i))
			db.Set(key, value)
			i++
		}
	})
}

func BenchmarkConcurrentGet(b *testing.B) {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("key-%d", i))
		value := types.Value(fmt.Sprintf("value-%d", i))
		db.Set(key, value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := types.Key(fmt.Sprintf("key-%d", i%1000))
			db.Get(key)
			i++
		}
	})
}
