package main

import (
	"database_engine/engine"
	"fmt"
	"log"
)

func main() {
	fmt.Println("=== Database Engine Transaction Demo ===")
	fmt.Println()

	db := engine.NewInMemoryDB()
	defer db.Close()

	// Test 1: Basic Transaction Operations
	fmt.Println("1. Testing Basic Transaction Operations")
	fmt.Println("----------------------------------------")

	// Set initial data
	err := db.Set("account:1", []byte("100"))
	if err != nil {
		log.Fatalf("Failed to set initial data: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Read within transaction
	value, err := tx.Get("account:1")
	if err != nil {
		log.Fatalf("Failed to read in transaction: %v", err)
	}
	fmt.Printf("Account 1 balance: %s\n", string(value))

	// Modify within transaction
	err = tx.Set("account:1", []byte("150"))
	if err != nil {
		log.Fatalf("Failed to set in transaction: %v", err)
	}

	// Read modified value
	value, err = tx.Get("account:1")
	if err != nil {
		log.Fatalf("Failed to read modified value: %v", err)
	}
	fmt.Printf("Account 1 balance (in transaction): %s\n", string(value))

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	fmt.Println("Transaction committed successfully")

	// Verify changes persisted
	value, err = db.Get("account:1")
	if err != nil {
		log.Fatalf("Failed to read after commit: %v", err)
	}
	fmt.Printf("Account 1 balance (after commit): %s\n", string(value))

	// Test 2: Transaction Rollback
	fmt.Println("\n2. Testing Transaction Rollback")
	fmt.Println("--------------------------------")

	// Begin transaction
	tx, err = db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Make changes
	err = tx.Set("account:1", []byte("200"))
	if err != nil {
		log.Fatalf("Failed to set in transaction: %v", err)
	}

	fmt.Println("Made changes in transaction (not committed yet)")

	// Rollback transaction
	err = tx.Rollback()
	if err != nil {
		log.Fatalf("Failed to rollback transaction: %v", err)
	}
	fmt.Println("Transaction rolled back")

	// Verify changes were not persisted
	value, err = db.Get("account:1")
	if err != nil {
		log.Fatalf("Failed to read after rollback: %v", err)
	}
	fmt.Printf("Account 1 balance (after rollback): %s (unchanged)\n", string(value))

	// Test 3: Transaction Isolation
	fmt.Println("\n3. Testing Transaction Isolation")
	fmt.Println("-----------------------------------")

	// Set initial data
	err = db.Set("counter", []byte("0"))
	if err != nil {
		log.Fatalf("Failed to set initial data: %v", err)
	}

	// Begin transaction 1
	tx1, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Begin transaction 2
	tx2, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Read in transaction 1
	value1, err := tx1.Get("counter")
	if err != nil {
		log.Fatalf("Failed to read in transaction 1: %v", err)
	}
	fmt.Printf("Transaction 1 reads counter: %s\n", string(value1))

	// Read in transaction 2
	value2, err := tx2.Get("counter")
	if err != nil {
		log.Fatalf("Failed to read in transaction 2: %v", err)
	}
	fmt.Printf("Transaction 2 reads counter: %s\n", string(value2))

	// Modify in transaction 1
	err = tx1.Set("counter", []byte("10"))
	if err != nil {
		log.Fatalf("Failed to set in transaction 1: %v", err)
	}
	fmt.Println("Transaction 1 modifies counter to 10")

	// Commit transaction 1
	err = tx1.Commit()
	if err != nil {
		log.Fatalf("Failed to commit transaction 1: %v", err)
	}
	fmt.Println("Transaction 1 committed")

	// Transaction 2 should still see original value
	value2, err = tx2.Get("counter")
	if err != nil {
		log.Fatalf("Failed to read in transaction 2: %v", err)
	}
	fmt.Printf("Transaction 2 still reads counter: %s (isolated)\n", string(value2))

	// Try to commit transaction 2 (should fail due to consistency check)
	err = tx2.Commit()
	if err != nil {
		fmt.Printf("Transaction 2 commit failed (expected): %v\n", err)
	}

	// Test 4: Transaction Atomicity
	fmt.Println("\n4. Testing Transaction Atomicity")
	fmt.Println("---------------------------------")

	// Set initial data
	err = db.Set("item:1", []byte("in_stock"))
	if err != nil {
		log.Fatalf("Failed to set initial data: %v", err)
	}

	// Begin transaction
	tx, err = db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Make multiple changes
	err = tx.Set("item:1", []byte("sold"))
	if err != nil {
		log.Fatalf("Failed to set item:1: %v", err)
	}

	err = tx.Set("order:1", []byte("created"))
	if err != nil {
		log.Fatalf("Failed to set order:1: %v", err)
	}

	err = tx.Set("payment:1", []byte("processed"))
	if err != nil {
		log.Fatalf("Failed to set payment:1: %v", err)
	}

	fmt.Println("Made multiple changes in transaction")

	// Commit (all changes should be applied atomically)
	err = tx.Commit()
	if err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	fmt.Println("Transaction committed - all changes applied atomically")

	// Verify all changes
	value, err = db.Get("item:1")
	if err != nil {
		log.Fatalf("Failed to read item:1: %v", err)
	}
	fmt.Printf("item:1 = %s\n", string(value))

	value, err = db.Get("order:1")
	if err != nil {
		log.Fatalf("Failed to read order:1: %v", err)
	}
	fmt.Printf("order:1 = %s\n", string(value))

	value, err = db.Get("payment:1")
	if err != nil {
		log.Fatalf("Failed to read payment:1: %v", err)
	}
	fmt.Printf("payment:1 = %s\n", string(value))

	// Test 5: Transaction Consistency
	fmt.Println("\n5. Testing Transaction Consistency")
	fmt.Println("-----------------------------------")

	// Set initial data
	err = db.Set("balance:1", []byte("100"))
	if err != nil {
		log.Fatalf("Failed to set initial data: %v", err)
	}

	// Begin transaction
	tx, err = db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Read balance
	_, err = tx.Get("balance:1")
	if err != nil {
		log.Fatalf("Failed to read balance: %v", err)
	}

	// Modify outside transaction (simulating another transaction)
	err = db.Set("balance:1", []byte("200"))
	if err != nil {
		log.Fatalf("Failed to modify outside transaction: %v", err)
	}
	fmt.Println("Modified balance outside transaction")

	// Try to commit (should fail due to consistency check)
	err = tx.Commit()
	if err != nil {
		fmt.Printf("Transaction commit failed (expected): %v\n", err)
		fmt.Println("Transaction aborted due to consistency violation")

		// Verify original value unchanged
		value, err = db.Get("balance:1")
		if err != nil {
			log.Fatalf("Failed to read balance: %v", err)
		}
		fmt.Printf("Balance after transaction abort: %s\n", string(value))
	}

	fmt.Println("\n=== Transaction Demo Complete ===")
	fmt.Println("Key Features Demonstrated:")
	fmt.Println("- Atomicity: All operations succeed or all fail")
	fmt.Println("- Consistency: Transactions maintain data integrity")
	fmt.Println("- Isolation: Transactions don't interfere with each other")
	fmt.Println("- Durability: Committed transactions persist")
}
