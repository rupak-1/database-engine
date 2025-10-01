package main

import (
	"database_engine/engine"
	"database_engine/types"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	fmt.Println("Custom Database Engine Demo - Phase 2")
	fmt.Println("=====================================")
	fmt.Println()

	// Demo 1: In-Memory Database
	fmt.Println("Demo 1: In-Memory Database")
	fmt.Println("---------------------------")
	demoInMemoryDB()

	fmt.Println()

	// Demo 2: Disk-Based Database
	fmt.Println("Demo 2: Disk-Based Database")
	fmt.Println("----------------------------")
	demoDiskDB()

	fmt.Println()

	// Demo 3: Persistence Test
	fmt.Println("Demo 3: Persistence Test")
	fmt.Println("------------------------")
	demoPersistence()

	fmt.Println()

	// Demo 4: Performance Comparison
	fmt.Println("Demo 4: Performance Comparison")
	fmt.Println("------------------------------")
	demoPerformanceComparison()

	fmt.Println()
	fmt.Println("Demo completed successfully!")
}

func demoInMemoryDB() {
	db := engine.NewInMemoryDB()
	defer db.Close()

	// Basic operations
	fmt.Println("Basic Operations:")
	err := db.Set("memory-key", []byte("memory-value"))
	if err != nil {
		log.Fatalf("Error setting key: %v", err)
	}

	value, err := db.Get("memory-key")
	if err != nil {
		log.Fatalf("Error getting key: %v", err)
	}
	fmt.Printf("Retrieved: %s = %s\n", "memory-key", string(value))

	size, err := db.Size()
	if err != nil {
		log.Fatalf("Error getting size: %v", err)
	}
	fmt.Printf("Database size: %d entries\n", size)

	// Memory usage (for in-memory storage)
	fmt.Printf("Database type: In-Memory\n")
}

func demoDiskDB() {
	tempDir := filepath.Join(os.TempDir(), "database_engine_demo")
	defer os.RemoveAll(tempDir)

	db, err := engine.NewDiskDB(tempDir)
	if err != nil {
		log.Fatalf("Error creating disk database: %v", err)
	}
	defer db.Close()

	// Basic operations
	fmt.Println("Basic Operations:")
	err = db.Set("disk-key", []byte("disk-value"))
	if err != nil {
		log.Fatalf("Error setting key: %v", err)
	}

	value, err := db.Get("disk-key")
	if err != nil {
		log.Fatalf("Error getting key: %v", err)
	}
	fmt.Printf("Retrieved: %s = %s\n", "disk-key", string(value))

	size, err := db.Size()
	if err != nil {
		log.Fatalf("Error getting size: %v", err)
	}
	fmt.Printf("Database size: %d entries\n", size)

	// Disk usage
	diskUsage, err := db.GetDiskUsage()
	if err != nil {
		log.Fatalf("Error getting disk usage: %v", err)
	}
	fmt.Printf("Disk usage: %d bytes\n", diskUsage)

	// Configuration
	config := db.GetConfig()
	fmt.Printf("Persistence enabled: %t\n", config.EnablePersistence)
	fmt.Printf("Data directory: %s\n", config.DataDirectory)
}

func demoPersistence() {
	tempDir := filepath.Join(os.TempDir(), "database_engine_persistence")
	defer os.RemoveAll(tempDir)

	// Create database and add data
	fmt.Println("Creating database and adding data...")
	db1, err := engine.NewDiskDB(tempDir)
	if err != nil {
		log.Fatalf("Error creating database: %v", err)
	}

	// Add multiple entries
	entries := []types.Entry{
		{Key: "user:1", Value: []byte("Alice Johnson")},
		{Key: "user:2", Value: []byte("Bob Smith")},
		{Key: "user:3", Value: []byte("Charlie Brown")},
		{Key: "config:theme", Value: []byte("dark")},
		{Key: "config:language", Value: []byte("en")},
	}

	err = db1.BatchSet(entries)
	if err != nil {
		log.Fatalf("Error batch setting: %v", err)
	}

	size1, err := db1.Size()
	if err != nil {
		log.Fatalf("Error getting size: %v", err)
	}
	fmt.Printf("Added %d entries\n", size1)

	err = db1.Close()
	if err != nil {
		log.Fatalf("Error closing database: %v", err)
	}

	// Create new database instance
	fmt.Println("Reopening database...")
	db2, err := engine.NewDiskDB(tempDir)
	if err != nil {
		log.Fatalf("Error reopening database: %v", err)
	}
	defer db2.Close()

	// Verify data persists
	size2, err := db2.Size()
	if err != nil {
		log.Fatalf("Error getting size: %v", err)
	}
	fmt.Printf("Retrieved %d entries\n", size2)

	// Retrieve some data
	value, err := db2.Get("user:1")
	if err != nil {
		log.Fatalf("Error getting user:1: %v", err)
	}
	fmt.Printf("User 1: %s\n", string(value))

	// Batch get
	keys := []types.Key{"config:theme", "config:language"}
	values, err := db2.BatchGet(keys)
	if err != nil {
		log.Fatalf("Error batch getting: %v", err)
	}
	fmt.Printf("Config values: %v\n", values)

	fmt.Println("Persistence test passed!")
}

func demoPerformanceComparison() {
	tempDir := filepath.Join(os.TempDir(), "database_engine_performance")
	defer os.RemoveAll(tempDir)

	// Test in-memory performance
	fmt.Println("Testing in-memory performance...")
	inMemoryDB := engine.NewInMemoryDB()
	defer inMemoryDB.Close()

	start := time.Now()
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("mem-key-%d", i))
		value := types.Value(fmt.Sprintf("mem-value-%d", i))
		err := inMemoryDB.Set(key, value)
		if err != nil {
			log.Fatalf("Error setting in-memory key: %v", err)
		}
	}
	inMemoryWriteTime := time.Since(start)

	start = time.Now()
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("mem-key-%d", i))
		_, err := inMemoryDB.Get(key)
		if err != nil {
			log.Fatalf("Error getting in-memory key: %v", err)
		}
	}
	inMemoryReadTime := time.Since(start)

	// Test disk performance
	fmt.Println("Testing disk performance...")
	diskDB, err := engine.NewDiskDB(tempDir)
	if err != nil {
		log.Fatalf("Error creating disk database: %v", err)
	}
	defer diskDB.Close()

	start = time.Now()
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		value := types.Value(fmt.Sprintf("disk-value-%d", i))
		err := diskDB.Set(key, value)
		if err != nil {
			log.Fatalf("Error setting disk key: %v", err)
		}
	}
	diskWriteTime := time.Since(start)

	start = time.Now()
	for i := 0; i < 1000; i++ {
		key := types.Key(fmt.Sprintf("disk-key-%d", i))
		_, err := diskDB.Get(key)
		if err != nil {
			log.Fatalf("Error getting disk key: %v", err)
		}
	}
	diskReadTime := time.Since(start)

	// Display results
	fmt.Println("Performance Results (1000 operations):")
	fmt.Printf("In-Memory Write Time: %v\n", inMemoryWriteTime)
	fmt.Printf("In-Memory Read Time:  %v\n", inMemoryReadTime)
	fmt.Printf("Disk Write Time:      %v\n", diskWriteTime)
	fmt.Printf("Disk Read Time:       %v\n", diskReadTime)

	// Calculate ratios
	writeRatio := float64(diskWriteTime) / float64(inMemoryWriteTime)
	readRatio := float64(diskReadTime) / float64(inMemoryReadTime)

	fmt.Printf("Disk write is %.1fx slower than in-memory\n", writeRatio)
	fmt.Printf("Disk read is %.1fx slower than in-memory\n", readRatio)

	// Disk usage
	diskUsage, err := diskDB.GetDiskUsage()
	if err != nil {
		log.Fatalf("Error getting disk usage: %v", err)
	}
	fmt.Printf("Disk usage: %d bytes\n", diskUsage)
}

// Helper function to get storage type (for demo purposes)
func getStorageType(db *engine.Database) string {
	// This is a simplified way to check storage type
	// In a real implementation, you might want to add a method to get storage type
	return "unknown"
}
