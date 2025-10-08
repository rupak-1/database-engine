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
	fmt.Println("=== Database Engine WAL Demo ===")
	fmt.Println()

	// Create temporary directory for demo
	tempDir := "wal_demo_data"
	defer os.RemoveAll(tempDir)

	// Test 1: Basic WAL functionality
	fmt.Println("1. Testing Basic WAL Functionality")
	fmt.Println("-----------------------------------")
	
	db, err := engine.NewDiskDBWithWAL(tempDir, 1024*1024) // 1MB max WAL size
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Check WAL status
	fmt.Printf("WAL Enabled: %t\n", db.IsWALEnabled())
	walSize, err := db.GetWALSize()
	if err != nil {
		log.Printf("Error getting WAL size: %v", err)
	} else {
		fmt.Printf("Initial WAL Size: %d bytes\n", walSize)
	}

	// Perform some operations
	fmt.Println("\nPerforming operations...")
	
	// Set some data
	err = db.Set("user:1", []byte("Alice"))
	if err != nil {
		log.Printf("Error setting user:1: %v", err)
	}

	err = db.Set("user:2", []byte("Bob"))
	if err != nil {
		log.Printf("Error setting user:2: %v", err)
	}

	err = db.SetWithTTL("session:abc123", []byte("active"), time.Hour)
	if err != nil {
		log.Printf("Error setting session: %v", err)
	}

	// Delete a key
	err = db.Delete("user:2")
	if err != nil {
		log.Printf("Error deleting user:2: %v", err)
	}

	// Check WAL size after operations
	walSize, err = db.GetWALSize()
	if err != nil {
		log.Printf("Error getting WAL size: %v", err)
	} else {
		fmt.Printf("WAL Size after operations: %d bytes\n", walSize)
	}

	// Test 2: WAL Recovery
	fmt.Println("\n2. Testing WAL Recovery")
	fmt.Println("----------------------")
	
	// Close current database
	db.Close()

	// Create new database instance - should recover from WAL
	fmt.Println("Creating new database instance (should recover from WAL)...")
	db2, err := engine.NewDiskDBWithWAL(tempDir, 1024*1024)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db2.Close()

	// Verify data was recovered
	fmt.Println("Verifying recovered data...")
	
	// user:1 should exist
	value, err := db2.Get("user:1")
	if err != nil {
		fmt.Printf("user:1 not found: %v\n", err)
	} else {
		fmt.Printf("user:1 recovered: %s\n", string(value))
	}

	// user:2 should not exist (was deleted)
	_, err = db2.Get("user:2")
	if err != nil {
		fmt.Printf("user:2 correctly not found: %v\n", err)
	}

	// session:abc123 should exist
	value, err = db2.Get("session:abc123")
	if err != nil {
		fmt.Printf("session:abc123 not found: %v\n", err)
	} else {
		fmt.Printf("session:abc123 recovered: %s\n", string(value))
	}

	// Test 3: WAL Rotation
	fmt.Println("\n3. Testing WAL Rotation")
	fmt.Println("------------------------")
	
	// Create database with very small WAL size to trigger rotation
	smallWalDir := "small_wal_demo"
	defer os.RemoveAll(smallWalDir)
	
	db3, err := engine.NewDiskDBWithWAL(smallWalDir, 100) // Very small WAL
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db3.Close()

	fmt.Println("Adding data to trigger WAL rotation...")
	
	// Add data until WAL rotation is needed
	for i := 0; i < 20; i++ {
		key := types.Key(fmt.Sprintf("key-%d", i))
		value := fmt.Sprintf("value-%d-with-some-additional-data-to-make-it-larger", i)
		
		err = db3.Set(key, []byte(value))
		if err != nil {
			log.Printf("Error setting %s: %v", key, err)
		}
		
		walSize, err := db3.GetWALSize()
		if err != nil {
			log.Printf("Error getting WAL size: %v", err)
		} else {
			fmt.Printf("WAL Size: %d bytes\n", walSize)
		}
		
		// Check if rotation is needed
		if walSize >= 100 {
			fmt.Println("WAL rotation needed, performing rotation...")
			err = db3.RotateWAL()
			if err != nil {
				log.Printf("Error rotating WAL: %v", err)
			} else {
				fmt.Println("WAL rotated successfully")
			}
			break
		}
	}

	// Test 4: WAL Clear
	fmt.Println("\n4. Testing WAL Clear")
	fmt.Println("--------------------")
	
	walSize, err = db3.GetWALSize()
	if err != nil {
		log.Printf("Error getting WAL size: %v", err)
	} else {
		fmt.Printf("WAL Size before clear: %d bytes\n", walSize)
	}

	fmt.Println("Clearing WAL...")
	err = db3.ClearWAL()
	if err != nil {
		log.Printf("Error clearing WAL: %v", err)
	} else {
		fmt.Println("WAL cleared successfully")
	}

	walSize, err = db3.GetWALSize()
	if err != nil {
		log.Printf("Error getting WAL size: %v", err)
	} else {
		fmt.Printf("WAL Size after clear: %d bytes\n", walSize)
	}

	// Test 5: Performance Comparison
	fmt.Println("\n5. Performance Comparison")
	fmt.Println("-------------------------")
	
	// Test with WAL enabled
	walPerfDir := "wal_perf_demo"
	defer os.RemoveAll(walPerfDir)
	
	dbWal, err := engine.NewDiskDBWithWAL(walPerfDir, 10*1024*1024) // 10MB WAL
	if err != nil {
		log.Fatalf("Failed to create WAL database: %v", err)
	}
	defer dbWal.Close()

	// Test without WAL
	noWalPerfDir := "no_wal_perf_demo"
	defer os.RemoveAll(noWalPerfDir)
	
	dbNoWal, err := engine.NewDiskDB(noWalPerfDir)
	if err != nil {
		log.Fatalf("Failed to create no-WAL database: %v", err)
	}
	defer dbNoWal.Close()

	numOps := 1000
	fmt.Printf("Performing %d operations...\n", numOps)

	// Test with WAL
	start := time.Now()
	for i := 0; i < numOps; i++ {
		key := types.Key(fmt.Sprintf("perf-key-%d", i))
		value := fmt.Sprintf("perf-value-%d", i)
		err = dbWal.Set(key, []byte(value))
		if err != nil {
			log.Printf("Error setting %s: %v", key, err)
		}
	}
	walDuration := time.Since(start)
	fmt.Printf("With WAL: %v\n", walDuration)

	// Test without WAL
	start = time.Now()
	for i := 0; i < numOps; i++ {
		key := types.Key(fmt.Sprintf("perf-key-%d", i))
		value := fmt.Sprintf("perf-value-%d", i)
		err = dbNoWal.Set(key, []byte(value))
		if err != nil {
			log.Printf("Error setting %s: %v", key, err)
		}
	}
	noWalDuration := time.Since(start)
	fmt.Printf("Without WAL: %v\n", noWalDuration)

	overhead := float64(walDuration-noWalDuration) / float64(noWalDuration) * 100
	fmt.Printf("WAL Overhead: %.2f%%\n", overhead)

	// Test 6: File Structure
	fmt.Println("\n6. WAL File Structure")
	fmt.Println("---------------------")
	
	fmt.Println("Files created:")
	files, err := filepath.Glob(filepath.Join(tempDir, "*"))
	if err != nil {
		log.Printf("Error listing files: %v", err)
	} else {
		for _, file := range files {
			info, err := os.Stat(file)
			if err != nil {
				continue
			}
			fmt.Printf("  %s (%d bytes)\n", filepath.Base(file), info.Size())
		}
	}

	fmt.Println("\n=== WAL Demo Complete ===")
	fmt.Println("Key Benefits of WAL:")
	fmt.Println("- Durability: All operations are logged before being applied")
	fmt.Println("- Recovery: Database can recover from crashes by replaying WAL")
	fmt.Println("- Consistency: Ensures data integrity even during failures")
	fmt.Println("- Performance: Minimal overhead for significant durability gains")
}