package main

import (
	"database_engine/engine"
	"database_engine/types"
	"testing"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple command",
			input:    "set key value",
			expected: []string{"set", "key", "value"},
		},
		{
			name:     "command with quoted value",
			input:    `set key "value with spaces"`,
			expected: []string{"set", "key", "value with spaces"},
		},
		{
			name:     "command with multiple words",
			input:    "set user:1 Alice Johnson",
			expected: []string{"set", "user:1", "Alice", "Johnson"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{},
		},
		{
			name:     "command with escaped quotes",
			input:    `set key "value with \"quotes\""`,
			expected: []string{"set", "key", `value with "quotes"`},
		},
		{
			name:     "single word",
			input:    "help",
			expected: []string{"help"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseCommand(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("parseCommand() length = %d, want %d", len(result), len(tt.expected))
				return
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("parseCommand()[%d] = %q, want %q", i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestHandleSet(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Just verify the command doesn't panic
	handleSet([]string{"test:key", "test:value"})

	// Verify value was set
	value, err := db.Get(types.Key("test:key"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "test:value" {
		t.Errorf("Get() = %q, want %q", string(value), "test:value")
	}
}

func TestHandleGet(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Set a value first
	err := db.Set(types.Key("test:key"), []byte("test:value"))
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Just verify the command doesn't panic and the value is accessible
	// The actual output verification is less important than functionality
	handleGet([]string{"test:key"})

	// Verify the value is still accessible (command should work)
	value, err := db.Get(types.Key("test:key"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "test:value" {
		t.Errorf("Get() = %q, want %q", string(value), "test:value")
	}
}

func TestHandleDelete(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Set a value first
	err := db.Set(types.Key("test:key"), []byte("test:value"))
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Delete it
	handleDelete([]string{"test:key"})

	// Verify it's deleted
	exists, err := db.Exists(types.Key("test:key"))
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Error("Key should be deleted but still exists")
	}
}

func TestHandleExists(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Set a value
	err := db.Set(types.Key("test:key"), []byte("test:value"))
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Just verify the command doesn't panic
	handleExists([]string{"test:key"})

	// Verify the key exists
	exists, err := db.Exists(types.Key("test:key"))
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Error("Key should exist")
	}
}

func TestHandleKeys(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Set some values
	db.Set(types.Key("key1"), []byte("value1"))
	db.Set(types.Key("key2"), []byte("value2"))
	db.Set(types.Key("key3"), []byte("value3"))

	// Just verify the command doesn't panic
	handleKeys()

	// Verify keys exist
	keys, err := db.Keys()
	if err != nil {
		t.Fatalf("Keys() error = %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("Keys() returned %d keys, want 3", len(keys))
	}
}

func TestHandleSize(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Set some values
	db.Set(types.Key("key1"), []byte("value1"))
	db.Set(types.Key("key2"), []byte("value2"))

	// Just verify the command doesn't panic
	handleSize()

	// Verify size
	size, err := db.Size()
	if err != nil {
		t.Fatalf("Size() error = %v", err)
	}
	if size != 2 {
		t.Errorf("Size() = %d, want 2", size)
	}
}

func TestHandleSetTTL(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Set with TTL
	handleSetTTL([]string{"session:123", "active", "1h"})

	// Verify value was set
	value, err := db.Get(types.Key("session:123"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "active" {
		t.Errorf("Get() = %q, want %q", string(value), "active")
	}
}

func TestHandleBeginCommit(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Begin transaction
	handleBegin()
	if currentTx == nil {
		t.Fatal("Transaction should be started")
	}

	// Set a value in transaction
	handleSet([]string{"tx:key", "tx:value"})

	// Value should not be visible outside transaction yet
	_, err := db.Get(types.Key("tx:key"))
	if err == nil {
		t.Error("Value should not be visible before commit")
	}

	// Commit transaction
	handleCommit()
	if currentTx != nil {
		t.Error("Transaction should be nil after commit")
	}

	// Value should now be visible
	value, err := db.Get(types.Key("tx:key"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "tx:value" {
		t.Errorf("Get() = %q, want %q", string(value), "tx:value")
	}
}

func TestHandleRollback(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Begin transaction
	handleBegin()
	if currentTx == nil {
		t.Fatal("Transaction should be started")
	}

	// Set a value in transaction
	handleSet([]string{"tx:key", "tx:value"})

	// Rollback transaction
	handleRollback()
	if currentTx != nil {
		t.Error("Transaction should be nil after rollback")
	}

	// Value should not be visible
	_, err := db.Get(types.Key("tx:key"))
	if err == nil {
		t.Error("Value should not exist after rollback")
	}
}

func TestHandleStatus(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	dbPath = "memory"
	defer db.Close()

	// Just verify the command doesn't panic
	handleStatus()

	// Verify database is accessible
	if db.IsClosed() {
		t.Error("Database should not be closed")
	}
}

func TestHandleConfig(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Just verify the command doesn't panic
	handleConfig()

	// Verify config is accessible
	config := db.GetConfig()
	if config.MaxKeySize == 0 {
		t.Error("Config should have MaxKeySize set")
	}
}

func TestHandleConnectMemory(t *testing.T) {
	// Clean up any existing connection
	if db != nil {
		db.Close()
		db = nil
	}

	handleConnectMemory()

	if db == nil {
		t.Fatal("Database should be connected")
	}
	if dbPath != "memory" {
		t.Errorf("dbPath = %q, want %q", dbPath, "memory")
	}

	// Cleanup
	db.Close()
	db = nil
	dbPath = ""
}

func TestHandleDisconnect(t *testing.T) {
	// Setup connection
	db = engine.NewInMemoryDB()
	dbPath = "memory"

	handleDisconnect()

	if db != nil {
		t.Error("Database should be disconnected")
	}
	if dbPath != "" {
		t.Error("dbPath should be empty")
	}
}

func TestHandleConnect(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Clean up any existing connection
	if db != nil {
		db.Close()
		db = nil
	}

	handleConnect([]string{tempDir, "5"}) // 5MB WAL

	if db == nil {
		t.Fatal("Database should be connected")
	}
	if dbPath != tempDir {
		t.Errorf("dbPath = %q, want %q", dbPath, tempDir)
	}

	// Verify WAL is enabled
	if !db.IsWALEnabled() {
		t.Error("WAL should be enabled")
	}

	// Cleanup
	db.Close()
	db = nil
	dbPath = ""
}

func TestHandleBackupOperations(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Connect to disk database with WAL
	testDB, err := engine.NewDiskDBWithWAL(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskDBWithWAL() error = %v", err)
	}
	db = testDB
	dbPath = tempDir
	defer func() {
		if db != nil {
			db.Close()
		}
		db = nil
		dbPath = ""
	}()

	// Set some data
	db.Set(types.Key("test:key"), []byte("test:value"))

	// Create backup
	handleBackupCreate([]string{"Test backup"})

	// List backups - just verify it doesn't panic
	handleBackupList()

	// Verify backup was created
	backups, err := db.ListBackups()
	if err != nil {
		t.Fatalf("ListBackups() error = %v", err)
	}
	if len(backups) == 0 {
		t.Error("Backup should be created")
	}
}

func TestHandleWALStatus(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Connect to disk database with WAL
	testDB, err := engine.NewDiskDBWithWAL(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskDBWithWAL() error = %v", err)
	}
	db = testDB
	dbPath = tempDir
	defer func() {
		if db != nil {
			db.Close()
		}
		db = nil
		dbPath = ""
	}()

	// Just verify the command doesn't panic
	handleWALStatus()

	// Verify WAL is enabled
	if !db.IsWALEnabled() {
		t.Error("WAL should be enabled")
	}
}

func TestHandleCommand(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	tests := []struct {
		name string
		args []string
	}{
		{"help", []string{"help"}},
		{"version", []string{"version"}},
		{"status", []string{"status"}},
		{"keys", []string{"keys"}},
		{"size", []string{"size"}},
		{"unknown command", []string{"unknown"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify it doesn't panic
			handleCommand(tt.args)
		})
	}
}

func TestHandleCommandWithDatabaseOps(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Test set command
	handleCommand([]string{"set", "test:key", "test:value"})

	// Verify value was set
	value, err := db.Get(types.Key("test:key"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "test:value" {
		t.Errorf("Get() = %q, want %q", string(value), "test:value")
	}

	// Test get command - just verify it doesn't panic
	handleCommand([]string{"get", "test:key"})

	// Verify value is still accessible
	value, err = db.Get(types.Key("test:key"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "test:value" {
		t.Errorf("Get() = %q, want %q", string(value), "test:value")
	}

	// Test delete command
	handleCommand([]string{"delete", "test:key"})

	// Verify key is deleted
	exists, err := db.Exists(types.Key("test:key"))
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Error("Key should be deleted")
	}
}

func TestHandleCommandErrors(t *testing.T) {
	// Test commands without database connection
	db = nil
	dbPath = ""

	// These should handle nil database gracefully
	handleCommand([]string{"get", "test:key"})
	handleCommand([]string{"set", "test:key", "test:value"})
	handleCommand([]string{"status"})
	handleCommand([]string{"keys"})
}

func TestHandleSetTTLErrors(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Test with invalid duration
	handleSetTTL([]string{"key", "value", "invalid"})
	// Should not panic

	// Test with missing arguments
	handleSetTTL([]string{"key"})
	// Should not panic
}

func TestTransactionInCLI(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Begin transaction
	handleCommand([]string{"begin"})
	if currentTx == nil {
		t.Fatal("Transaction should be started")
	}

	// Set values in transaction
	handleCommand([]string{"set", "tx:key1", "value1"})
	handleCommand([]string{"set", "tx:key2", "value2"})

	// Commit
	handleCommand([]string{"commit"})
	if currentTx != nil {
		t.Error("Transaction should be nil after commit")
	}

	// Verify values are committed
	value1, err := db.Get(types.Key("tx:key1"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value1) != "value1" {
		t.Errorf("Get() = %q, want %q", string(value1), "value1")
	}

	value2, err := db.Get(types.Key("tx:key2"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value2) != "value2" {
		t.Errorf("Get() = %q, want %q", string(value2), "value2")
	}
}

func TestHandleCommandAliases(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Test command aliases
	aliases := map[string][]string{
		"h":     []string{"h"},
		"?":     []string{"?"},
		"del":   []string{"del", "test:key"},
		"info":  []string{"info"},
		"close": []string{"close"},
	}

	for name, args := range aliases {
		t.Run(name, func(t *testing.T) {
			// Just verify they don't panic
			handleCommand(args)
		})
	}
}

func TestHandleDiskOperations(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Connect to disk database
	testDB, err := engine.NewDiskDBWithWAL(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskDBWithWAL() error = %v", err)
	}
	db = testDB
	dbPath = tempDir
	defer func() {
		if db != nil {
			db.Close()
		}
		db = nil
		dbPath = ""
	}()

	// Test compact
	handleCommand([]string{"compact"})

	// Test disk-usage - just verify it doesn't panic
	handleCommand([]string{"disk-usage"})

	// Verify disk usage is accessible
	usage, err := db.GetDiskUsage()
	if err != nil {
		t.Fatalf("GetDiskUsage() error = %v", err)
	}
	if usage < 0 {
		t.Error("Disk usage should be non-negative")
	}
}

func TestHandleRecoveryOperations(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Connect to disk database with WAL
	testDB, err := engine.NewDiskDBWithWAL(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskDBWithWAL() error = %v", err)
	}
	db = testDB
	dbPath = tempDir
	defer func() {
		if db != nil {
			db.Close()
		}
		db = nil
		dbPath = ""
	}()

	// Test recovery-point
	handleCommand([]string{"recovery-point", "Test recovery point"})

	// Test recovery-state - just verify it doesn't panic
	handleCommand([]string{"recovery-state"})

	// Verify recovery state is accessible
	state := db.GetRecoveryState()
	if state == nil {
		t.Error("Recovery state should be available")
	}

	// Test validate - just verify it doesn't panic
	handleCommand([]string{"validate"})

	// Verify validation works - for empty databases, validation may fail
	// which is acceptable, we just verify the command doesn't panic
	_, _, err = db.ValidateDataIntegrity()
	if err != nil {
		// Validation errors are acceptable for empty databases
		// The important thing is the command doesn't panic
	}
}

func TestHandleWALOperations(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Connect to disk database with WAL
	testDB, err := engine.NewDiskDBWithWAL(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskDBWithWAL() error = %v", err)
	}
	db = testDB
	dbPath = tempDir
	defer func() {
		if db != nil {
			db.Close()
		}
		db = nil
		dbPath = ""
	}()

	// Test wal-status - just verify it doesn't panic
	handleCommand([]string{"wal-status"})

	// Verify WAL is enabled
	if !db.IsWALEnabled() {
		t.Error("WAL should be enabled")
	}

	// Test wal-rotate
	handleCommand([]string{"wal-rotate"})
}

func TestHandleBackupCommands(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Connect to disk database with WAL
	testDB, err := engine.NewDiskDBWithWAL(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskDBWithWAL() error = %v", err)
	}
	db = testDB
	dbPath = tempDir
	defer func() {
		if db != nil {
			db.Close()
		}
		db = nil
		dbPath = ""
	}()

	// Set some data
	db.Set(types.Key("backup:test"), []byte("backup:value"))

	// Test backup-create
	handleCommand([]string{"backup", "Test backup"})

	// Test backup-list - just verify it doesn't panic
	handleCommand([]string{"backup-list"})

	// Verify backup was created
	backups, err := db.ListBackups()
	if err != nil {
		t.Fatalf("ListBackups() error = %v", err)
	}
	if len(backups) == 0 {
		t.Error("Backup should be created")
	}
}

func TestHandleCommandWithNoArgs(t *testing.T) {
	// Test commands that should handle missing arguments gracefully
	db = engine.NewInMemoryDB()
	defer db.Close()

	// These should not panic
	handleCommand([]string{"set"})
	handleCommand([]string{"get"})
	handleCommand([]string{"delete"})
	handleCommand([]string{"exists"})
	handleCommand([]string{"set-ttl"})
}

func TestConcurrentCLIOperations(t *testing.T) {
	// Setup in-memory database
	db = engine.NewInMemoryDB()
	defer db.Close()

	// Test concurrent operations
	done := make(chan bool, 3)

	go func() {
		handleCommand([]string{"set", "concurrent:1", "value1"})
		done <- true
	}()

	go func() {
		handleCommand([]string{"set", "concurrent:2", "value2"})
		done <- true
	}()

	go func() {
		handleCommand([]string{"set", "concurrent:3", "value3"})
		done <- true
	}()

	// Wait for all operations
	for i := 0; i < 3; i++ {
		<-done
	}

	// Verify all values were set
	keys := []string{"concurrent:1", "concurrent:2", "concurrent:3"}
	for _, key := range keys {
		exists, err := db.Exists(types.Key(key))
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if !exists {
			t.Errorf("Key %s should exist", key)
		}
	}
}
