package main

import (
	"database_engine/engine"
	"database_engine/types"
	"fmt"
	"log"
)

func main() {
	// Create a new in-memory database
	db := engine.NewInMemoryDB()
	defer db.Close()

	fmt.Println("🚀 Custom Database Engine Demo")
	fmt.Println("==============================")

	// Basic operations
	fmt.Println("\n📝 Basic Operations:")

	// Set some key-value pairs
	keys := []string{"user:1", "user:2", "user:3"}
	values := []string{"Alice Johnson", "Bob Smith", "Charlie Brown"}

	for i, key := range keys {
		err := db.Set(types.Key(key), []byte(values[i]))
		if err != nil {
			log.Fatalf("Error setting %s: %v", key, err)
		}
		fmt.Printf("✅ Set %s = %s\n", key, values[i])
	}

	// Get database size
	size, err := db.Size()
	if err != nil {
		log.Fatalf("Error getting size: %v", err)
	}
	fmt.Printf("📊 Database size: %d entries\n", size)

	// Retrieve values
	fmt.Println("\n🔍 Retrieving Values:")
	for _, key := range keys {
		value, err := db.Get(types.Key(key))
		if err != nil {
			log.Fatalf("Error getting %s: %v", key, err)
		}
		fmt.Printf("✅ Get %s = %s\n", key, string(value))
	}

	// Check if keys exist
	fmt.Println("\n🔎 Checking Key Existence:")
	testKeys := []string{"user:1", "user:4", "user:2"}
	for _, key := range testKeys {
		exists, err := db.Exists(types.Key(key))
		if err != nil {
			log.Fatalf("Error checking existence of %s: %v", key, err)
		}
		status := "❌ Not found"
		if exists {
			status = "✅ Found"
		}
		fmt.Printf("%s %s\n", status, key)
	}

	// Batch operations
	fmt.Println("\n📦 Batch Operations:")

	// Batch get
	batchKeys := []types.Key{"user:1", "user:3"}
	batchValues, err := db.BatchGet(batchKeys)
	if err != nil {
		log.Fatalf("Error in batch get: %v", err)
	}
	fmt.Printf("✅ Batch get %v: %d results\n", batchKeys, len(batchValues))
	for key, value := range batchValues {
		fmt.Printf("   %s = %s\n", key, string(value))
	}

	// Batch delete
	err = db.BatchDelete([]types.Key{"user:1", "user:2"})
	if err != nil {
		log.Fatalf("Error in batch delete: %v", err)
	}
	fmt.Printf("✅ Batch deleted user:1 and user:2\n")

	// Check final state
	size, err = db.Size()
	if err != nil {
		log.Fatalf("Error getting final size: %v", err)
	}
	fmt.Printf("📊 Final database size: %d entries\n", size)

	// Get all remaining keys
	allKeys, err := db.Keys()
	if err != nil {
		log.Fatalf("Error getting keys: %v", err)
	}
	fmt.Printf("🔑 Remaining keys: %v\n", allKeys)

	// Configuration demo
	fmt.Println("\n⚙️ Configuration Demo:")
	config := db.GetConfig()
	fmt.Printf("Max key size: %d bytes\n", config.MaxKeySize)
	fmt.Printf("Max value size: %d bytes\n", config.MaxValueSize)
	fmt.Printf("TTL enabled: %t\n", config.EnableTTL)

	fmt.Println("\n🎉 Demo completed successfully!")
}
