package main

import (
	"database_engine/engine"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	fmt.Println("=== Database Engine Persistence & Recovery Demo ===")
	fmt.Println()

	// Create temporary directory for demo
	tempDir := "persistence_demo_data"
	defer os.RemoveAll(tempDir)

	// Test 1: Basic Backup and Restore
	fmt.Println("1. Testing Basic Backup and Restore")
	fmt.Println("-----------------------------------")

	db, err := engine.NewDiskDBWithWAL(tempDir, 1024*1024)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Check persistence support
	fmt.Printf("Backup Supported: %t\n", db.IsBackupSupported())
	fmt.Printf("Recovery Supported: %t\n", db.IsRecoverySupported())

	// Add some test data
	fmt.Println("\nAdding test data...")
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

	// Create backup
	fmt.Println("\nCreating backup...")
	metadata, err := db.CreateBackup("Initial data backup")
	if err != nil {
		log.Printf("Error creating backup: %v", err)
	} else {
		fmt.Printf("Backup created: %s\n", metadata.Timestamp.Format("2006-01-02 15:04:05"))
		fmt.Printf("Entry count: %d\n", metadata.EntryCount)
		fmt.Printf("Data size: %d bytes\n", metadata.DataSize)
		fmt.Printf("Checksum: %s\n", metadata.Checksum)
	}

	// Test 2: List and Manage Backups
	fmt.Println("\n2. Testing Backup Management")
	fmt.Println("----------------------------")

	// List backups
	backups, err := db.ListBackups()
	if err != nil {
		log.Printf("Error listing backups: %v", err)
	} else {
		fmt.Printf("Found %d backups:\n", len(backups))
		for i, backup := range backups {
			fmt.Printf("  %d. %s - %s (%d entries, %d bytes)\n",
				i+1,
				backup.Timestamp.Format("2006-01-02 15:04:05"),
				backup.Description,
				backup.EntryCount,
				backup.DataSize)
		}
	}

	// Test 3: Data Integrity Validation
	fmt.Println("\n3. Testing Data Integrity Validation")
	fmt.Println("------------------------------------")

	isValid, issues, err := db.ValidateDataIntegrity()
	if err != nil {
		log.Printf("Error validating data integrity: %v", err)
	} else {
		fmt.Printf("Data Integrity: %t\n", isValid)
		if len(issues) > 0 {
			fmt.Println("Issues found:")
			for _, issue := range issues {
				fmt.Printf("  - %s\n", issue)
			}
		} else {
			fmt.Println("No issues found")
		}
	}

	// Test 4: Recovery Point Creation
	fmt.Println("\n4. Testing Recovery Point Creation")
	fmt.Println("----------------------------------")

	// Create recovery point before risky operation
	recoveryPoint, err := db.CreateRecoveryPoint("Before risky operation")
	if err != nil {
		log.Printf("Error creating recovery point: %v", err)
	} else {
		fmt.Printf("Recovery point created: %s\n", recoveryPoint.Timestamp.Format("2006-01-02 15:04:05"))
	}

	// Test 5: Recovery Operations
	fmt.Println("\n5. Testing Recovery Operations")
	fmt.Println("-------------------------------")

	// Get recovery state
	recoveryState := db.GetRecoveryState()
	if recoveryState != nil {
		fmt.Printf("Recovery Mode: %s\n", recoveryState.RecoveryMode)
		fmt.Printf("Recovery Count: %d\n", recoveryState.RecoveryCount)
		fmt.Printf("Data Integrity: %t\n", recoveryState.DataIntegrity)
		fmt.Printf("WAL Recovery: %t\n", recoveryState.WALRecovery)
		fmt.Printf("Backup Recovery: %t\n", recoveryState.BackupRecovery)
	}

	// Test 6: Backup Restore
	fmt.Println("\n6. Testing Backup Restore")
	fmt.Println("-------------------------")

	// Modify data after backup
	fmt.Println("Modifying data after backup...")
	err = db.Set("user:3", []byte("Charlie"))
	if err != nil {
		log.Printf("Error setting user:3: %v", err)
	}

	err = db.Delete("user:1")
	if err != nil {
		log.Printf("Error deleting user:1: %v", err)
	}

	// Verify current state
	fmt.Println("Current state after modifications:")
	keys, err := db.Keys()
	if err != nil {
		log.Printf("Error getting keys: %v", err)
	} else {
		for _, key := range keys {
			value, err := db.Get(key)
			if err != nil {
				fmt.Printf("  %s: ERROR\n", key)
			} else {
				fmt.Printf("  %s: %s\n", key, string(value))
			}
		}
	}

	// Restore from backup
	if len(backups) > 0 {
		backupName := backups[0].Timestamp.Format("20060102_150405")
		fmt.Printf("\nRestoring from backup: %s\n", backupName)

		err = db.RestoreFromBackup(backupName)
		if err != nil {
			log.Printf("Error restoring from backup: %v", err)
		} else {
			fmt.Println("Restore completed successfully")

			// Verify restored state
			fmt.Println("State after restore:")
			keys, err := db.Keys()
			if err != nil {
				log.Printf("Error getting keys: %v", err)
			} else {
				for _, key := range keys {
					value, err := db.Get(key)
					if err != nil {
						fmt.Printf("  %s: ERROR\n", key)
					} else {
						fmt.Printf("  %s: %s\n", key, string(value))
					}
				}
			}
		}
	}

	// Test 7: Recovery Mode Management
	fmt.Println("\n7. Testing Recovery Mode Management")
	fmt.Println("-----------------------------------")

	// Test different recovery modes
	modes := []string{"auto", "manual", "backup"}
	for _, mode := range modes {
		err = db.SetRecoveryMode(mode)
		if err != nil {
			log.Printf("Error setting recovery mode to %s: %v", mode, err)
		} else {
			fmt.Printf("Recovery mode set to: %s\n", mode)

			// Verify mode was set
			state := db.GetRecoveryState()
			if state != nil {
				fmt.Printf("  Confirmed mode: %s\n", state.RecoveryMode)
			}
		}
	}

	// Test 8: Backup Information
	fmt.Println("\n8. Testing Backup Information")
	fmt.Println("-----------------------------")

	if len(backups) > 0 {
		backupName := backups[0].Timestamp.Format("20060102_150405")
		info, err := db.GetBackupInfo(backupName)
		if err != nil {
			log.Printf("Error getting backup info: %v", err)
		} else {
			fmt.Printf("Backup Information for %s:\n", backupName)
			fmt.Printf("  Timestamp: %s\n", info.Timestamp.Format("2006-01-02 15:04:05"))
			fmt.Printf("  Version: %s\n", info.Version)
			fmt.Printf("  Entry Count: %d\n", info.EntryCount)
			fmt.Printf("  Data Size: %d bytes\n", info.DataSize)
			fmt.Printf("  Index Size: %d bytes\n", info.IndexSize)
			fmt.Printf("  WAL Size: %d bytes\n", info.WALSize)
			fmt.Printf("  Checksum: %s\n", info.Checksum)
			fmt.Printf("  Backup Type: %s\n", info.BackupType)
			fmt.Printf("  Description: %s\n", info.Description)
		}
	}

	// Test 9: File Structure
	fmt.Println("\n9. File Structure")
	fmt.Println("-----------------")

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
			if info.IsDir() {
				fmt.Printf("  %s/ (directory)\n", filepath.Base(file))
			} else {
				fmt.Printf("  %s (%d bytes)\n", filepath.Base(file), info.Size())
			}
		}
	}

	// Test 10: Cleanup
	fmt.Println("\n10. Testing Backup Cleanup")
	fmt.Println("--------------------------")

	// List backups before cleanup
	backupsBefore, err := db.ListBackups()
	if err != nil {
		log.Printf("Error listing backups: %v", err)
	} else {
		fmt.Printf("Backups before cleanup: %d\n", len(backupsBefore))
	}

	// Delete a backup (if we have more than one)
	if len(backupsBefore) > 1 {
		backupToDelete := backupsBefore[0].Timestamp.Format("20060102_150405")
		fmt.Printf("Deleting backup: %s\n", backupToDelete)

		err = db.DeleteBackup(backupToDelete)
		if err != nil {
			log.Printf("Error deleting backup: %v", err)
		} else {
			fmt.Println("Backup deleted successfully")
		}
	}

	// List backups after cleanup
	backupsAfter, err := db.ListBackups()
	if err != nil {
		log.Printf("Error listing backups: %v", err)
	} else {
		fmt.Printf("Backups after cleanup: %d\n", len(backupsAfter))
	}

	fmt.Println("\n=== Persistence & Recovery Demo Complete ===")
	fmt.Println("Key Features Demonstrated:")
	fmt.Println("- Full backup creation and management")
	fmt.Println("- Data integrity validation")
	fmt.Println("- Recovery point creation")
	fmt.Println("- Backup restore operations")
	fmt.Println("- Recovery mode management")
	fmt.Println("- Comprehensive error handling")
	fmt.Println("- File structure and cleanup")
}
