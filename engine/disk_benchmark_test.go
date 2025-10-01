package engine_test

import (
	"database_engine/engine"
	"database_engine/types"
	"fmt"
	"testing"
)

func BenchmarkDiskSet(b *testing.B) {
	tempDir := b.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		b.Fatalf("Failed to create disk database: %v", err)
	}
	defer db.Close()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		value := types.Value(fmt.Sprintf("disk-value-%d", i))
		db.Set(key, value)
	}
}

func BenchmarkDiskGet(b *testing.B) {
	tempDir := b.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		b.Fatalf("Failed to create disk database: %v", err)
	}
	defer db.Close()
	
	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		value := types.Value(fmt.Sprintf("disk-value-%d", i))
		db.Set(key, value)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i%1000))
		db.Get(key)
	}
}

func BenchmarkDiskDelete(b *testing.B) {
	tempDir := b.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		b.Fatalf("Failed to create disk database: %v", err)
	}
	defer db.Close()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Set a key first
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		value := types.Value(fmt.Sprintf("disk-value-%d", i))
		db.Set(key, value)
		
		// Then delete it
		db.Delete(key)
	}
}

func BenchmarkDiskBatchSet(b *testing.B) {
	tempDir := b.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		b.Fatalf("Failed to create disk database: %v", err)
	}
	defer db.Close()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := make([]types.Entry, 10)
		for j := 0; j < 10; j++ {
			entries[j] = types.Entry{
				Key:   types.Key(fmt.Sprintf("disk-batch-key-%d-%d", i, j)),
				Value: types.Value(fmt.Sprintf("disk-batch-value-%d-%d", i, j)),
			}
		}
		db.BatchSet(entries)
	}
}

func BenchmarkDiskBatchGet(b *testing.B) {
	tempDir := b.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		b.Fatalf("Failed to create disk database: %v", err)
	}
	defer db.Close()
	
	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		value := types.Value(fmt.Sprintf("disk-value-%d", i))
		db.Set(key, value)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys := make([]types.Key, 10)
		for j := 0; j < 10; j++ {
			keys[j] = types.Key(fmt.Sprintf("disk-key-%d", (i*10+j)%1000))
		}
		db.BatchGet(keys)
	}
}

func BenchmarkDiskConcurrentSet(b *testing.B) {
	tempDir := b.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		b.Fatalf("Failed to create disk database: %v", err)
	}
	defer db.Close()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := types.Key(fmt.Sprintf("disk-concurrent-key-%d", i))
			value := types.Value(fmt.Sprintf("disk-concurrent-value-%d", i))
			db.Set(key, value)
			i++
		}
	})
}

func BenchmarkDiskConcurrentGet(b *testing.B) {
	tempDir := b.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		b.Fatalf("Failed to create disk database: %v", err)
	}
	defer db.Close()
	
	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		value := types.Value(fmt.Sprintf("disk-value-%d", i))
		db.Set(key, value)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := types.Key(fmt.Sprintf("disk-key-%d", i%1000))
			db.Get(key)
			i++
		}
	})
}

func BenchmarkDiskCompact(b *testing.B) {
	tempDir := b.TempDir()
	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		b.Fatalf("Failed to create disk database: %v", err)
	}
	defer db.Close()
	
	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		value := types.Value(fmt.Sprintf("disk-value-%d", i))
		db.Set(key, value)
	}
	
	// Delete half the data
	for i := 0; i < 500; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		db.Delete(key)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Compact()
	}
}

// Comparison benchmarks
func BenchmarkInMemoryVsDiskSet(b *testing.B) {
	b.Run("InMemory", func(b *testing.B) {
		db := engine.NewInMemoryDB()
		defer db.Close()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := types.Key(fmt.Sprintf("key-%d", i))
			value := types.Value(fmt.Sprintf("value-%d", i))
			db.Set(key, value)
		}
	})
	
	b.Run("Disk", func(b *testing.B) {
		tempDir := b.TempDir()
		db, err := engine.NewDiskDB(tempDir)
		if err != nil {
			b.Fatalf("Failed to create disk database: %v", err)
		}
		defer db.Close()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := types.Key(fmt.Sprintf("key-%d", i))
			value := types.Value(fmt.Sprintf("value-%d", i))
			db.Set(key, value)
		}
	})
}

func BenchmarkInMemoryVsDiskGet(b *testing.B) {
	b.Run("InMemory", func(b *testing.B) {
		db := engine.NewInMemoryDB()
		defer db.Close()
		
		// Pre-populate
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
	})
	
	b.Run("Disk", func(b *testing.B) {
		tempDir := b.TempDir()
		db, err := engine.NewDiskDB(tempDir)
		if err != nil {
			b.Fatalf("Failed to create disk database: %v", err)
		}
		defer db.Close()
		
		// Pre-populate
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
	})
}
