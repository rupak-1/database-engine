package main

import (
	"bufio"
	"database_engine/engine"
	"database_engine/types"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	version = "1.0.0"
)

var (
	db         *engine.Database
	currentTx  types.Transaction
	dbPath     string
	walMaxSize int64 = 10 * 1024 * 1024 // 10MB default
)

func main() {
	if len(os.Args) > 1 {
		// Command-line mode
		handleCommand(os.Args[1:])
	} else {
		// Interactive mode
		runInteractive()
	}
}

func runInteractive() {
	fmt.Printf("Database Engine CLI v%s\n", version)
	fmt.Println("Type 'help' for available commands or 'exit' to quit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	prompt := "db> "

	for {
		fmt.Print(prompt)
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if line == "exit" || line == "quit" {
			if db != nil {
				db.Close()
			}
			fmt.Println("Goodbye!")
			break
		}

		args := parseCommand(line)
		if len(args) == 0 {
			continue
		}

		handleCommand(args)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
}

func parseCommand(line string) []string {
	var args []string
	var current strings.Builder
	inQuotes := false
	escape := false

	for _, char := range line {
		if escape {
			current.WriteRune(char)
			escape = false
			continue
		}

		if char == '\\' {
			escape = true
			continue
		}

		if char == '"' {
			inQuotes = !inQuotes
			continue
		}

		if char == ' ' && !inQuotes {
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
			continue
		}

		current.WriteRune(char)
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

func handleCommand(args []string) {
	if len(args) == 0 {
		return
	}

	cmd := strings.ToLower(args[0])

	switch cmd {
	case "help", "h", "?":
		printHelp()
	case "connect", "open":
		handleConnect(args[1:])
	case "connect-memory", "open-memory":
		handleConnectMemory()
	case "disconnect", "close":
		handleDisconnect()
	case "status", "info":
		handleStatus()
	case "set":
		handleSet(args[1:])
	case "get":
		handleGet(args[1:])
	case "delete", "del":
		handleDelete(args[1:])
	case "exists":
		handleExists(args[1:])
	case "keys":
		handleKeys()
	case "size":
		handleSize()
	case "clear":
		handleClear()
	case "setttl", "set-ttl":
		handleSetTTL(args[1:])
	case "begin", "tx":
		handleBegin()
	case "commit":
		handleCommit()
	case "rollback":
		handleRollback()
	case "backup", "backup-create":
		handleBackupCreate(args[1:])
	case "backup-list", "backups":
		handleBackupList()
	case "backup-restore", "restore":
		handleBackupRestore(args[1:])
	case "backup-delete":
		handleBackupDelete(args[1:])
	case "backup-info":
		handleBackupInfo(args[1:])
	case "recovery-point":
		handleRecoveryPoint(args[1:])
	case "recovery-state":
		handleRecoveryState()
	case "validate":
		handleValidate()
	case "wal-status":
		handleWALStatus()
	case "wal-rotate":
		handleWALRotate()
	case "wal-clear":
		handleWALClear()
	case "compact":
		handleCompact()
	case "disk-usage":
		handleDiskUsage()
	case "config":
		handleConfig()
	case "version", "v":
		fmt.Printf("Database Engine CLI v%s\n", version)
	default:
		fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", cmd)
	}
}

func printHelp() {
	fmt.Println("Available Commands:")
	fmt.Println()
	fmt.Println("Connection:")
	fmt.Println("  connect <path> [wal-size]  - Connect to disk database (with optional WAL size in MB)")
	fmt.Println("  connect-memory             - Connect to in-memory database")
	fmt.Println("  disconnect                 - Close current database connection")
	fmt.Println()
	fmt.Println("Database Operations:")
	fmt.Println("  set <key> <value>          - Set a key-value pair")
	fmt.Println("  get <key>                  - Get value for a key")
	fmt.Println("  delete <key>               - Delete a key")
	fmt.Println("  exists <key>               - Check if key exists")
	fmt.Println("  keys                       - List all keys")
	fmt.Println("  size                       - Get number of entries")
	fmt.Println("  clear                      - Clear all data")
	fmt.Println("  set-ttl <key> <value> <duration> - Set key with TTL (e.g., 1h, 30m)")
	fmt.Println()
	fmt.Println("Transactions:")
	fmt.Println("  begin                      - Start a transaction")
	fmt.Println("  commit                     - Commit current transaction")
	fmt.Println("  rollback                   - Rollback current transaction")
	fmt.Println()
	fmt.Println("Backup & Recovery:")
	fmt.Println("  backup [description]       - Create a backup")
	fmt.Println("  backup-list                - List all backups")
	fmt.Println("  backup-restore <name>      - Restore from backup")
	fmt.Println("  backup-delete <name>       - Delete a backup")
	fmt.Println("  backup-info <name>         - Get backup information")
	fmt.Println("  recovery-point [desc]      - Create a recovery point")
	fmt.Println("  recovery-state             - Get recovery state")
	fmt.Println("  validate                   - Validate data integrity")
	fmt.Println()
	fmt.Println("WAL Operations:")
	fmt.Println("  wal-status                 - Get WAL status")
	fmt.Println("  wal-rotate                 - Rotate WAL")
	fmt.Println("  wal-clear                  - Clear WAL")
	fmt.Println()
	fmt.Println("Maintenance:")
	fmt.Println("  compact                    - Compact database (disk only)")
	fmt.Println("  disk-usage                 - Get disk usage (disk only)")
	fmt.Println()
	fmt.Println("Information:")
	fmt.Println("  status                     - Show database status")
	fmt.Println("  config                     - Show database configuration")
	fmt.Println("  version                    - Show version")
	fmt.Println("  help                       - Show this help")
}

func handleConnect(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: connect <path> [wal-size-mb]")
		return
	}

	path := args[0]
	walSize := walMaxSize

	if len(args) > 1 {
		sizeMB, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			fmt.Printf("Error: Invalid WAL size: %v\n", err)
			return
		}
		walSize = sizeMB * 1024 * 1024
	}

	if db != nil {
		db.Close()
	}

	var err error
	db, err = engine.NewDiskDBWithWAL(path, walSize)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		return
	}

	dbPath = path
	fmt.Printf("Connected to database at: %s (WAL: %d MB)\n", path, walSize/(1024*1024))
}

func handleConnectMemory() {
	if db != nil {
		db.Close()
	}

	db = engine.NewInMemoryDB()
	dbPath = "memory"
	fmt.Println("Connected to in-memory database")
}

func handleDisconnect() {
	if db == nil {
		fmt.Println("No database connected")
		return
	}

	if currentTx != nil {
		fmt.Println("Warning: Active transaction will be rolled back")
		currentTx.Rollback()
		currentTx = nil
	}

	db.Close()
	db = nil
	dbPath = ""
	fmt.Println("Disconnected from database")
}

func handleStatus() {
	if db == nil {
		fmt.Println("No database connected")
		return
	}

	fmt.Println("Database Status:")
	fmt.Printf("  Path: %s\n", dbPath)
	fmt.Printf("  Closed: %t\n", db.IsClosed())

	size, err := db.Size()
	if err != nil {
		fmt.Printf("  Size: Error - %v\n", err)
	} else {
		fmt.Printf("  Size: %d entries\n", size)
	}

	if db.IsWALEnabled() {
		walSize, err := db.GetWALSize()
		if err == nil {
			fmt.Printf("  WAL Enabled: true\n")
			fmt.Printf("  WAL Size: %d bytes (%.2f MB)\n", walSize, float64(walSize)/(1024*1024))
		}
	} else {
		fmt.Printf("  WAL Enabled: false\n")
	}

	if db.IsBackupSupported() {
		fmt.Printf("  Backup Supported: true\n")
	} else {
		fmt.Printf("  Backup Supported: false\n")
	}

	if db.IsRecoverySupported() {
		fmt.Printf("  Recovery Supported: true\n")
	} else {
		fmt.Printf("  Recovery Supported: false\n")
	}

	if currentTx != nil {
		fmt.Printf("  Active Transaction: true\n")
	} else {
		fmt.Printf("  Active Transaction: false\n")
	}

	// Disk usage for disk-based storage
	if dbPath != "memory" {
		usage, err := db.GetDiskUsage()
		if err == nil {
			fmt.Printf("  Disk Usage: %d bytes (%.2f MB)\n", usage, float64(usage)/(1024*1024))
		}
	}
}

func handleSet(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if len(args) < 2 {
		fmt.Println("Usage: set <key> <value>")
		return
	}

	key := types.Key(args[0])
	value := []byte(strings.Join(args[1:], " "))

	var err error
	if currentTx != nil {
		err = currentTx.Set(key, value)
		if err == nil {
			fmt.Printf("Set (in transaction): %s = %s\n", key, value)
		}
	} else {
		err = db.Set(key, value)
		if err == nil {
			fmt.Printf("Set: %s = %s\n", key, value)
		}
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func handleGet(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if len(args) < 1 {
		fmt.Println("Usage: get <key>")
		return
	}

	key := types.Key(args[0])

	var value types.Value
	var err error
	if currentTx != nil {
		value, err = currentTx.Get(key)
	} else {
		value, err = db.Get(key)
	}

	if err != nil {
		if err == types.ErrKeyNotFound {
			fmt.Printf("Key not found: %s\n", key)
		} else {
			fmt.Printf("Error: %v\n", err)
		}
		return
	}

	fmt.Printf("%s = %s\n", key, string(value))
}

func handleDelete(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if len(args) < 1 {
		fmt.Println("Usage: delete <key>")
		return
	}

	key := types.Key(args[0])

	var err error
	if currentTx != nil {
		err = currentTx.Delete(key)
		if err == nil {
			fmt.Printf("Deleted (in transaction): %s\n", key)
		}
	} else {
		err = db.Delete(key)
		if err == nil {
			fmt.Printf("Deleted: %s\n", key)
		}
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func handleExists(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if len(args) < 1 {
		fmt.Println("Usage: exists <key>")
		return
	}

	key := types.Key(args[0])
	exists, err := db.Exists(key)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if exists {
		fmt.Printf("Key exists: %s\n", key)
	} else {
		fmt.Printf("Key does not exist: %s\n", key)
	}
}

func handleKeys() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	keys, err := db.Keys()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(keys) == 0 {
		fmt.Println("No keys found")
		return
	}

	fmt.Printf("Keys (%d):\n", len(keys))
	for _, key := range keys {
		fmt.Printf("  %s\n", key)
	}
}

func handleSize() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	size, err := db.Size()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Database size: %d entries\n", size)
}

func handleClear() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if currentTx != nil {
		fmt.Println("Error: Cannot clear database while transaction is active")
		return
	}

	fmt.Print("Are you sure you want to clear all data? (yes/no): ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}

	response := strings.ToLower(strings.TrimSpace(scanner.Text()))
	if response != "yes" && response != "y" {
		fmt.Println("Clear cancelled")
		return
	}

	err := db.Clear()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Database cleared")
}

func handleSetTTL(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if len(args) < 3 {
		fmt.Println("Usage: set-ttl <key> <value> <duration>")
		fmt.Println("Example: set-ttl session:123 active 1h")
		return
	}

	key := types.Key(args[0])
	value := []byte(args[1])
	durationStr := args[2]

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		fmt.Printf("Error: Invalid duration: %v\n", err)
		return
	}

	err = db.SetWithTTL(key, value, duration)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Set with TTL: %s = %s (expires in %s)\n", key, value, duration)
}

func handleBegin() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if currentTx != nil {
		fmt.Println("Error: Transaction already active")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	currentTx = tx
	fmt.Println("Transaction started")
}

func handleCommit() {
	if currentTx == nil {
		fmt.Println("Error: No active transaction")
		return
	}

	err := currentTx.Commit()
	if err != nil {
		fmt.Printf("Error committing transaction: %v\n", err)
		currentTx = nil
		return
	}

	currentTx = nil
	fmt.Println("Transaction committed")
}

func handleRollback() {
	if currentTx == nil {
		fmt.Println("Error: No active transaction")
		return
	}

	err := currentTx.Rollback()
	if err != nil {
		fmt.Printf("Error rolling back transaction: %v\n", err)
	}

	currentTx = nil
	fmt.Println("Transaction rolled back")
}

func handleBackupCreate(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsBackupSupported() {
		fmt.Println("Error: Backup not supported for this database type")
		return
	}

	description := ""
	if len(args) > 0 {
		description = strings.Join(args, " ")
	}

	metadata, err := db.CreateBackup(description)
	if err != nil {
		fmt.Printf("Error creating backup: %v\n", err)
		return
	}

	fmt.Printf("Backup created: %s\n", metadata.Timestamp.Format("20060102_150405"))
	fmt.Printf("  Description: %s\n", metadata.Description)
	fmt.Printf("  Timestamp: %s\n", metadata.Timestamp.Format("2006-01-02 15:04:05"))
}

func handleBackupList() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsBackupSupported() {
		fmt.Println("Error: Backup not supported for this database type")
		return
	}

	backups, err := db.ListBackups()
	if err != nil {
		fmt.Printf("Error listing backups: %v\n", err)
		return
	}

	if len(backups) == 0 {
		fmt.Println("No backups found")
		return
	}

	fmt.Printf("Backups (%d):\n", len(backups))
	for _, backup := range backups {
		name := backup.Timestamp.Format("20060102_150405")
		fmt.Printf("  %s - %s\n", name, backup.Description)
		fmt.Printf("    Created: %s\n", backup.Timestamp.Format("2006-01-02 15:04:05"))
	}
}

func handleBackupRestore(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsBackupSupported() {
		fmt.Println("Error: Backup not supported for this database type")
		return
	}

	if len(args) < 1 {
		fmt.Println("Usage: backup-restore <backup-name>")
		return
	}

	backupName := args[0]

	fmt.Print("Warning: This will overwrite current data. Continue? (yes/no): ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}

	response := strings.ToLower(strings.TrimSpace(scanner.Text()))
	if response != "yes" && response != "y" {
		fmt.Println("Restore cancelled")
		return
	}

	err := db.RestoreFromBackup(backupName)
	if err != nil {
		fmt.Printf("Error restoring backup: %v\n", err)
		return
	}

	fmt.Printf("Backup restored: %s\n", backupName)
}

func handleBackupDelete(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsBackupSupported() {
		fmt.Println("Error: Backup not supported for this database type")
		return
	}

	if len(args) < 1 {
		fmt.Println("Usage: backup-delete <backup-name>")
		return
	}

	backupName := args[0]

	err := db.DeleteBackup(backupName)
	if err != nil {
		fmt.Printf("Error deleting backup: %v\n", err)
		return
	}

	fmt.Printf("Backup deleted: %s\n", backupName)
}

func handleBackupInfo(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsBackupSupported() {
		fmt.Println("Error: Backup not supported for this database type")
		return
	}

	if len(args) < 1 {
		fmt.Println("Usage: backup-info <backup-name>")
		return
	}

	backupName := args[0]

	info, err := db.GetBackupInfo(backupName)
	if err != nil {
		fmt.Printf("Error getting backup info: %v\n", err)
		return
	}

	fmt.Printf("Backup Information:\n")
	fmt.Printf("  Name: %s\n", backupName)
	fmt.Printf("  Description: %s\n", info.Description)
	fmt.Printf("  Timestamp: %s\n", info.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("  Checksum: %s\n", info.Checksum)
}

func handleRecoveryPoint(args []string) {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsRecoverySupported() {
		fmt.Println("Error: Recovery not supported for this database type")
		return
	}

	description := ""
	if len(args) > 0 {
		description = strings.Join(args, " ")
	}

	metadata, err := db.CreateRecoveryPoint(description)
	if err != nil {
		fmt.Printf("Error creating recovery point: %v\n", err)
		return
	}

	fmt.Printf("Recovery point created: %s\n", metadata.Timestamp.Format("20060102_150405"))
	fmt.Printf("  Description: %s\n", metadata.Description)
}

func handleRecoveryState() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsRecoverySupported() {
		fmt.Println("Error: Recovery not supported for this database type")
		return
	}

	state := db.GetRecoveryState()
	if state == nil {
		fmt.Println("No recovery state available")
		return
	}

	fmt.Printf("Recovery State:\n")
	fmt.Printf("  Mode: %s\n", state.RecoveryMode)
	fmt.Printf("  Last Recovery: %s\n", state.LastRecovery.Format("2006-01-02 15:04:05"))
	fmt.Printf("  Recovery Count: %d\n", state.RecoveryCount)
	fmt.Printf("  Data Integrity: %t\n", state.DataIntegrity)
	fmt.Printf("  WAL Recovery: %t\n", state.WALRecovery)
	fmt.Printf("  Backup Recovery: %t\n", state.BackupRecovery)
}

func handleValidate() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsRecoverySupported() {
		fmt.Println("Error: Recovery not supported for this database type")
		return
	}

	isValid, issues, err := db.ValidateDataIntegrity()
	if err != nil {
		fmt.Printf("Error validating data: %v\n", err)
		return
	}

	if isValid {
		fmt.Println("Data integrity: OK")
	} else {
		fmt.Println("Data integrity: FAILED")
		if len(issues) > 0 {
			fmt.Println("Issues found:")
			for _, issue := range issues {
				fmt.Printf("  - %s\n", issue)
			}
		}
	}
}

func handleWALStatus() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsWALEnabled() {
		fmt.Println("WAL is not enabled")
		return
	}

	walSize, err := db.GetWALSize()
	if err != nil {
		fmt.Printf("Error getting WAL size: %v\n", err)
		return
	}

	fmt.Printf("WAL Status:\n")
	fmt.Printf("  Enabled: true\n")
	fmt.Printf("  Size: %d bytes (%.2f MB)\n", walSize, float64(walSize)/(1024*1024))
}

func handleWALRotate() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsWALEnabled() {
		fmt.Println("Error: WAL is not enabled")
		return
	}

	err := db.RotateWAL()
	if err != nil {
		fmt.Printf("Error rotating WAL: %v\n", err)
		return
	}

	fmt.Println("WAL rotated successfully")
}

func handleWALClear() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if !db.IsWALEnabled() {
		fmt.Println("Error: WAL is not enabled")
		return
	}

	fmt.Print("Warning: This will clear the WAL. Continue? (yes/no): ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}

	response := strings.ToLower(strings.TrimSpace(scanner.Text()))
	if response != "yes" && response != "y" {
		fmt.Println("WAL clear cancelled")
		return
	}

	err := db.ClearWAL()
	if err != nil {
		fmt.Printf("Error clearing WAL: %v\n", err)
		return
	}

	fmt.Println("WAL cleared successfully")
}

func handleCompact() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if dbPath == "memory" {
		fmt.Println("Error: Compaction not supported for in-memory database")
		return
	}

	err := db.Compact()
	if err != nil {
		fmt.Printf("Error compacting database: %v\n", err)
		return
	}

	fmt.Println("Database compacted successfully")
}

func handleDiskUsage() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	if dbPath == "memory" {
		fmt.Println("Error: Disk usage not available for in-memory database")
		return
	}

	usage, err := db.GetDiskUsage()
	if err != nil {
		fmt.Printf("Error getting disk usage: %v\n", err)
		return
	}

	fmt.Printf("Disk Usage: %d bytes (%.2f MB)\n", usage, float64(usage)/(1024*1024))
}

func handleConfig() {
	if db == nil {
		fmt.Println("Error: No database connected")
		return
	}

	config := db.GetConfig()
	configJSON, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Database Configuration:")
	fmt.Println(string(configJSON))
}
