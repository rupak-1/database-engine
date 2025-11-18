# Database Engine - Comprehensive User Manual

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Database Types](#database-types)
5. [Basic Operations](#basic-operations)
6. [Advanced Features](#advanced-features)
7. [Transactions](#transactions)
8. [Backup and Recovery](#backup-and-recovery)
9. [Write-Ahead Logging (WAL)](#write-ahead-logging-wal)
10. [CLI Tool](#cli-tool)
11. [Programmatic API](#programmatic-api)
12. [Configuration](#configuration)
13. [Performance Considerations](#performance-considerations)
14. [Troubleshooting](#troubleshooting)
15. [Examples](#examples)

---

## Introduction

The Database Engine is a high-performance key-value store written in Go, supporting both in-memory and disk-based storage with advanced features including:

- **In-Memory Storage**: Fast, volatile storage for temporary data
- **Disk-Based Storage**: Persistent storage with automatic compaction
- **Write-Ahead Logging (WAL)**: Ensures data durability and crash recovery
- **ACID Transactions**: Full transaction support with atomicity, consistency, isolation, and durability
- **Backup and Recovery**: Comprehensive backup management and data recovery
- **TTL Support**: Automatic expiration of keys with time-to-live
- **Thread-Safe Operations**: All operations are safe for concurrent access
- **CLI Tool**: Interactive command-line interface for database management

---

## Installation

### Prerequisites

- Go 1.16 or later
- Git (for cloning the repository)

### Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd database_engine

# Install dependencies
make deps

# Build the project
make build

# Build the CLI tool
make cli-build
```

The CLI tool will be available at `bin/dbcli`.

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Run benchmarks
make run-benchmarks
```

---

## Quick Start

### Using the CLI Tool

```bash
# Start the CLI in interactive mode
./bin/dbcli

# Or use make
make cli
```

### Basic CLI Session

```
db> connect-memory
Connected to in-memory database

db> set user:1 "Alice Johnson"
Set: user:1 = Alice Johnson

db> get user:1
user:1 = Alice Johnson

db> status
Database Status:
  Path: memory
  Closed: false
  Size: 1 entries
  WAL Enabled: false
```

### Using in Your Go Code

```go
package main

import (
    "database_engine/engine"
    "fmt"
)

func main() {
    // Create an in-memory database
    db := engine.NewInMemoryDB()
    defer db.Close()
    
    // Set a value
    db.Set("key1", []byte("value1"))
    
    // Get a value
    value, _ := db.Get("key1")
    fmt.Println(string(value)) // Output: value1
}
```

---

## Database Types

The database engine supports three types of storage:

### 1. In-Memory Database

Fast, volatile storage that exists only in RAM. Data is lost when the application terminates.

**Use Cases:**
- Caching
- Temporary data storage
- High-performance read/write operations
- Testing and development

**Creation:**
```go
db := engine.NewInMemoryDB()
defer db.Close()
```

**Features:**
- ✅ Fastest performance
- ✅ Thread-safe
- ✅ TTL support
- ✅ Batch operations
- ❌ No persistence
- ❌ No backup/recovery
- ❌ No WAL

### 2. Disk-Based Database

Persistent storage that saves data to disk. Data survives application restarts.

**Use Cases:**
- Long-term data storage
- Data that must persist across restarts
- Applications requiring durability

**Creation:**
```go
db, err := engine.NewDiskDB("./data")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

**Features:**
- ✅ Data persistence
- ✅ Automatic compaction
- ✅ Disk usage tracking
- ✅ TTL support
- ✅ Thread-safe
- ❌ Slower than in-memory
- ❌ No WAL (no crash recovery)
- ❌ No backup/recovery

### 3. Disk-Based Database with WAL

Persistent storage with Write-Ahead Logging for enhanced durability and crash recovery.

**Use Cases:**
- Production applications
- Critical data requiring crash recovery
- Applications needing backup/restore
- High-reliability requirements

**Creation:**
```go
// Create with 10MB WAL
db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

**Features:**
- ✅ All disk-based features
- ✅ Write-Ahead Logging
- ✅ Crash recovery
- ✅ Backup and recovery
- ✅ Data integrity validation
- ✅ Recovery points
- ❌ Slower than disk-only (WAL overhead)
- ❌ Requires more disk space

---

## Basic Operations

### Set

Store a key-value pair in the database.

**CLI:**
```
db> set user:1 "John Doe"
Set: user:1 = John Doe
```

**Go API:**
```go
err := db.Set("user:1", []byte("John Doe"))
if err != nil {
    log.Fatal(err)
}
```

**Parameters:**
- `key`: String key (max 1KB by default)
- `value`: Byte array value (max 1MB by default)

**Errors:**
- `ErrInvalidKey`: Key is empty or exceeds max size
- `ErrInvalidValue`: Value exceeds max size
- `ErrDatabaseClosed`: Database is closed

### Get

Retrieve a value by key.

**CLI:**
```
db> get user:1
user:1 = John Doe
```

**Go API:**
```go
value, err := db.Get("user:1")
if err != nil {
    if err == types.ErrKeyNotFound {
        fmt.Println("Key not found")
    } else {
        log.Fatal(err)
    }
} else {
    fmt.Println(string(value))
}
```

**Returns:**
- `Value`: The value as a byte array
- `error`: Error if key not found or database closed

**Errors:**
- `ErrKeyNotFound`: Key does not exist
- `ErrKeyExpired`: Key has expired (TTL)
- `ErrDatabaseClosed`: Database is closed

### Delete

Remove a key-value pair from the database.

**CLI:**
```
db> delete user:1
Deleted: user:1
```

**Go API:**
```go
err := db.Delete("user:1")
if err != nil {
    log.Fatal(err)
}
```

**Errors:**
- `ErrKeyNotFound`: Key does not exist
- `ErrDatabaseClosed`: Database is closed

### Exists

Check if a key exists in the database.

**CLI:**
```
db> exists user:1
Key exists: user:1
```

**Go API:**
```go
exists, err := db.Exists("user:1")
if err != nil {
    log.Fatal(err)
}
if exists {
    fmt.Println("Key exists")
}
```

### Keys

List all keys in the database.

**CLI:**
```
db> keys
Keys (3):
  user:1
  user:2
  user:3
```

**Go API:**
```go
keys, err := db.Keys()
if err != nil {
    log.Fatal(err)
}
for _, key := range keys {
    fmt.Println(key)
}
```

### Size

Get the number of entries in the database.

**CLI:**
```
db> size
Database size: 3 entries
```

**Go API:**
```go
size, err := db.Size()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Database has %d entries\n", size)
```

### Clear

Remove all data from the database.

**CLI:**
```
db> clear
Are you sure you want to clear all data? (yes/no): yes
Database cleared
```

**Go API:**
```go
err := db.Clear()
if err != nil {
    log.Fatal(err)
}
```

**Note:** Cannot be used while a transaction is active.

---

## Advanced Features

### Time-to-Live (TTL)

Set keys with automatic expiration using TTL.

**CLI:**
```
db> set-ttl session:123 active 1h
Set with TTL: session:123 = active (expires in 1h0m0s)
```

**Go API:**
```go
err := db.SetWithTTL("session:123", []byte("active"), time.Hour)
if err != nil {
    log.Fatal(err)
}
```

**Duration Format:**
- `1h` - 1 hour
- `30m` - 30 minutes
- `2h30m` - 2 hours 30 minutes
- `24h` - 24 hours
- `7d` - 7 days (168 hours)

**Automatic Cleanup:**
Expired keys are automatically cleaned up. You can also manually trigger cleanup:

```go
count := db.CleanupExpired()
fmt.Printf("Cleaned up %d expired keys\n", count)
```

### Batch Operations

Perform multiple operations efficiently in a single call.

#### Batch Get

**Go API:**
```go
keys := []types.Key{"user:1", "user:2", "user:3"}
values, err := db.BatchGet(keys)
if err != nil {
    log.Fatal(err)
}

for key, value := range values {
    fmt.Printf("%s = %s\n", key, string(value))
}
```

#### Batch Set

**Go API:**
```go
entries := []types.Entry{
    {Key: "user:1", Value: []byte("Alice"), Timestamp: time.Now()},
    {Key: "user:2", Value: []byte("Bob"), Timestamp: time.Now()},
    {Key: "user:3", Value: []byte("Charlie"), Timestamp: time.Now()},
}
err := db.BatchSet(entries)
if err != nil {
    log.Fatal(err)
}
```

#### Batch Delete

**Go API:**
```go
keys := []types.Key{"user:1", "user:2", "user:3"}
err := db.BatchDelete(keys)
if err != nil {
    log.Fatal(err)
}
```

**Performance:** Batch operations are more efficient than individual operations, especially for disk-based storage.

---

## Transactions

The database engine supports ACID transactions for atomic operations.

### Transaction Properties

- **Atomicity**: All operations in a transaction succeed or all fail
- **Consistency**: Transactions maintain data integrity
- **Isolation**: Transactions don't interfere with each other (snapshot isolation)
- **Durability**: Committed transactions persist to storage

### Using Transactions

**CLI:**
```
db> begin
Transaction started

db> set user:1 "Alice"
Set (in transaction): user:1 = Alice

db> set user:2 "Bob"
Set (in transaction): user:2 = Bob

db> commit
Transaction committed
```

**Go API:**
```go
// Begin transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Perform operations
err = tx.Set("user:1", []byte("Alice"))
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

err = tx.Set("user:2", []byte("Bob"))
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit transaction
err = tx.Commit()
if err != nil {
    log.Fatal(err)
}
```

### Rolling Back Transactions

**CLI:**
```
db> begin
Transaction started

db> set user:1 "Alice"
Set (in transaction): user:1 = Alice

db> rollback
Transaction rolled back
```

**Go API:**
```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Perform operations
err = tx.Set("user:1", []byte("Alice"))
if err != nil {
    tx.Rollback()
    return
}

// Rollback if needed
err = tx.Rollback()
if err != nil {
    log.Fatal(err)
}
```

### Transaction Limitations

- Only one transaction can be active at a time per database instance
- Cannot perform `Clear()` while a transaction is active
- Transactions are isolated using snapshot isolation
- Committed transactions are immediately durable (for disk-based storage)

---

## Backup and Recovery

Backup and recovery features are available only for disk-based databases with WAL enabled.

### Creating Backups

**CLI:**
```
db> backup "Daily backup before migration"
Backup created: 20240115_143022
  Description: Daily backup before migration
  Timestamp: 2024-01-15 14:30:22
```

**Go API:**
```go
metadata, err := db.CreateBackup("Daily backup before migration")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Backup created: %s\n", metadata.Timestamp.Format("20060102_150405"))
```

### Listing Backups

**CLI:**
```
db> backup-list
Backups (3):
  20240115_143022 - Daily backup before migration
    Created: 2024-01-15 14:30:22
  20240114_120000 - Weekly backup
    Created: 2024-01-14 12:00:00
  20240113_090000 - Initial backup
    Created: 2024-01-13 09:00:00
```

**Go API:**
```go
backups, err := db.ListBackups()
if err != nil {
    log.Fatal(err)
}

for _, backup := range backups {
    name := backup.Timestamp.Format("20060102_150405")
    fmt.Printf("%s - %s\n", name, backup.Description)
}
```

### Restoring from Backup

**CLI:**
```
db> backup-restore 20240115_143022
Warning: This will overwrite current data. Continue? (yes/no): yes
Backup restored: 20240115_143022
```

**Go API:**
```go
err := db.RestoreFromBackup("20240115_143022")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Backup restored successfully")
```

### Getting Backup Information

**CLI:**
```
db> backup-info 20240115_143022
Backup Information:
  Name: 20240115_143022
  Description: Daily backup before migration
  Timestamp: 2024-01-15 14:30:22
  Checksum: abc123def456...
```

**Go API:**
```go
info, err := db.GetBackupInfo("20240115_143022")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Description: %s\n", info.Description)
fmt.Printf("Checksum: %s\n", info.Checksum)
```

### Deleting Backups

**CLI:**
```
db> backup-delete 20240113_090000
Backup deleted: 20240113_090000
```

**Go API:**
```go
err := db.DeleteBackup("20240113_090000")
if err != nil {
    log.Fatal(err)
}
```

### Recovery Points

Create recovery points before risky operations:

**CLI:**
```
db> recovery-point "Before schema migration"
Recovery point created: 20240115_150000
  Description: Before schema migration
```

**Go API:**
```go
metadata, err := db.CreateRecoveryPoint("Before schema migration")
if err != nil {
    log.Fatal(err)
}
```

### Recovery State

Check the current recovery state:

**CLI:**
```
db> recovery-state
Recovery State:
  Mode: auto
  Last Recovery: 2024-01-15 10:00:00
  Recovery Count: 1
  Data Integrity: true
  WAL Recovery: true
  Backup Recovery: false
```

**Go API:**
```go
state := db.GetRecoveryState()
if state != nil {
    fmt.Printf("Recovery Mode: %s\n", state.RecoveryMode)
    fmt.Printf("Data Integrity: %t\n", state.DataIntegrity)
}
```

### Data Integrity Validation

Validate the integrity of your data:

**CLI:**
```
db> validate
Data integrity: OK
```

**Go API:**
```go
isValid, issues, err := db.ValidateDataIntegrity()
if err != nil {
    log.Fatal(err)
}

if isValid {
    fmt.Println("Data integrity: OK")
} else {
    fmt.Println("Data integrity: FAILED")
    for _, issue := range issues {
        fmt.Printf("  - %s\n", issue)
    }
}
```

### Automatic Recovery

The database automatically performs recovery on startup if WAL is enabled. Recovery includes:
- WAL replay to restore uncommitted operations
- Data integrity checks
- Automatic repair of minor inconsistencies

---

## Write-Ahead Logging (WAL)

Write-Ahead Logging ensures data durability and enables crash recovery. WAL is available only for disk-based databases.

### WAL Overview

WAL works by:
1. Writing all changes to a log file before applying them to the main database
2. Ensuring data is durable even if the application crashes
3. Replaying the log on startup to recover uncommitted operations

### Checking WAL Status

**CLI:**
```
db> wal-status
WAL Status:
  Enabled: true
  Size: 1024000 bytes (0.98 MB)
```

**Go API:**
```go
if db.IsWALEnabled() {
    walSize, err := db.GetWALSize()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("WAL Size: %d bytes\n", walSize)
}
```

### WAL Rotation

When the WAL reaches its maximum size, it should be rotated:

**CLI:**
```
db> wal-rotate
WAL rotated successfully
```

**Go API:**
```go
walSize, _ := db.GetWALSize()
if walSize > 5*1024*1024 { // 5MB threshold
    err := db.RotateWAL()
    if err != nil {
        log.Fatal(err)
    }
}
```

**Note:** WAL rotation is typically automatic, but you can manually trigger it.

### Clearing WAL

Clear the WAL (use with caution):

**CLI:**
```
db> wal-clear
Warning: This will clear the WAL. Continue? (yes/no): yes
WAL cleared successfully
```

**Go API:**
```go
err := db.ClearWAL()
if err != nil {
    log.Fatal(err)
}
```

**Warning:** Clearing the WAL may result in data loss if the database crashes before changes are flushed to disk.

### WAL Configuration

When creating a database with WAL, specify the maximum WAL size:

```go
// Create database with 10MB WAL
db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
```

**Recommendations:**
- Use 10-50MB for small to medium databases
- Use 100MB+ for large databases with high write throughput
- Monitor WAL size and rotate when needed

---

## CLI Tool

The CLI tool provides an interactive interface for managing your database.

### Starting the CLI

```bash
# Interactive mode
./bin/dbcli

# Or using make
make cli
```

### Connection Commands

```
connect <path> [wal-size-mb]  - Connect to disk database
connect-memory                - Connect to in-memory database
disconnect                    - Close current connection
```

**Examples:**
```
db> connect ./mydata 10
Connected to database at: ./mydata (WAL: 10 MB)

db> connect-memory
Connected to in-memory database

db> disconnect
Disconnected from database
```

### Database Operation Commands

```
set <key> <value>                    - Set a key-value pair
get <key>                            - Get value for a key
delete <key>                         - Delete a key
exists <key>                         - Check if key exists
keys                                 - List all keys
size                                 - Get number of entries
clear                                - Clear all data
set-ttl <key> <value> <duration>     - Set key with TTL
```

### Transaction Commands

```
begin      - Start a transaction
commit     - Commit current transaction
rollback   - Rollback current transaction
```

### Backup and Recovery Commands

```
backup [description]              - Create a backup
backup-list                      - List all backups
backup-restore <name>            - Restore from backup
backup-delete <name>             - Delete a backup
backup-info <name>               - Get backup information
recovery-point [desc]            - Create a recovery point
recovery-state                   - Get recovery state
validate                         - Validate data integrity
```

### WAL Commands

```
wal-status   - Get WAL status
wal-rotate   - Rotate WAL
wal-clear    - Clear WAL
```

### Maintenance Commands

```
compact      - Compact database (disk only)
disk-usage   - Get disk usage (disk only)
```

### Information Commands

```
status   - Show database status
config   - Show database configuration
version  - Show version
help     - Show help
```

### Command Examples

**Complete Workflow:**
```
db> connect ./mydata 10
Connected to database at: ./mydata (WAL: 10 MB)

db> set user:1 "Alice"
Set: user:1 = Alice

db> set user:2 "Bob"
Set: user:2 = Bob

db> begin
Transaction started

db> set user:3 "Charlie"
Set (in transaction): user:3 = Charlie

db> commit
Transaction committed

db> backup "Daily backup"
Backup created: 20240115_143022
  Description: Daily backup
  Timestamp: 2024-01-15 14:30:22

db> status
Database Status:
  Path: ./mydata
  Closed: false
  Size: 3 entries
  WAL Enabled: true
  WAL Size: 1024 bytes (0.00 MB)
  Backup Supported: true
  Recovery Supported: true
  Active Transaction: false

db> exit
Goodbye!
```

---

## Programmatic API

### Creating Databases

#### In-Memory Database

```go
import "database_engine/engine"

db := engine.NewInMemoryDB()
defer db.Close()
```

#### In-Memory Database with Custom Config

```go
import (
    "database_engine/engine"
    "database_engine/types"
)

config := types.DefaultConfig()
config.MaxKeySize = 2048      // 2KB max key size
config.MaxValueSize = 2*1024*1024  // 2MB max value size

db := engine.NewInMemoryDBWithConfig(config)
defer db.Close()
```

#### Disk-Based Database

```go
db, err := engine.NewDiskDB("./data")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

#### Disk-Based Database with WAL

```go
// 10MB WAL
db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### Basic Operations

```go
// Set
err := db.Set("key1", []byte("value1"))

// Get
value, err := db.Get("key1")
if err == types.ErrKeyNotFound {
    // Handle not found
}

// Delete
err := db.Delete("key1")

// Exists
exists, err := db.Exists("key1")

// Keys
keys, err := db.Keys()

// Size
size, err := db.Size()

// Clear
err := db.Clear()
```

### TTL Operations

```go
import "time"

// Set with TTL (1 hour)
err := db.SetWithTTL("session:123", []byte("active"), time.Hour)

// Cleanup expired keys
count := db.CleanupExpired()
fmt.Printf("Cleaned up %d expired keys\n", count)
```

### Batch Operations

```go
// Batch Get
keys := []types.Key{"key1", "key2", "key3"}
values, err := db.BatchGet(keys)

// Batch Set
entries := []types.Entry{
    {Key: "key1", Value: []byte("value1"), Timestamp: time.Now()},
    {Key: "key2", Value: []byte("value2"), Timestamp: time.Now()},
}
err := db.BatchSet(entries)

// Batch Delete
keys := []types.Key{"key1", "key2", "key3"}
err := db.BatchDelete(keys)
```

### Transactions

```go
// Begin transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Perform operations
err = tx.Set("key1", []byte("value1"))
if err != nil {
    tx.Rollback()
    return
}

// Commit or rollback
err = tx.Commit()
// or
err = tx.Rollback()
```

### Backup and Recovery

```go
// Create backup
metadata, err := db.CreateBackup("Description")
if err != nil {
    log.Fatal(err)
}

// List backups
backups, err := db.ListBackups()

// Restore from backup
err := db.RestoreFromBackup("20240115_143022")

// Get backup info
info, err := db.GetBackupInfo("20240115_143022")

// Delete backup
err := db.DeleteBackup("20240115_143022")

// Create recovery point
metadata, err := db.CreateRecoveryPoint("Description")

// Validate data integrity
isValid, issues, err := db.ValidateDataIntegrity()

// Get recovery state
state := db.GetRecoveryState()
```

### WAL Operations

```go
// Check if WAL is enabled
if db.IsWALEnabled() {
    // Get WAL size
    walSize, err := db.GetWALSize()
    
    // Rotate WAL
    err := db.RotateWAL()
    
    // Clear WAL
    err := db.ClearWAL()
}
```

### Disk Operations

```go
// Compact database (disk only)
err := db.Compact()

// Get disk usage (disk only)
usage, err := db.GetDiskUsage()
fmt.Printf("Disk usage: %d bytes\n", usage)
```

### Configuration

```go
// Get current configuration
config := db.GetConfig()

// Update configuration
config.MaxKeySize = 2048
err := db.SetConfig(config)
```

### Error Handling

```go
import "database_engine/types"

value, err := db.Get("key1")
if err != nil {
    switch err {
    case types.ErrKeyNotFound:
        fmt.Println("Key not found")
    case types.ErrKeyExpired:
        fmt.Println("Key has expired")
    case types.ErrDatabaseClosed:
        fmt.Println("Database is closed")
    case types.ErrInvalidKey:
        fmt.Println("Invalid key")
    case types.ErrInvalidValue:
        fmt.Println("Invalid value")
    default:
        log.Fatal(err)
    }
}
```

---

## Configuration

### Default Configuration

```go
config := types.DefaultConfig()
```

Default values:
- `MaxMemorySize`: 1GB
- `MaxKeySize`: 1KB
- `MaxValueSize`: 1MB
- `WriteBufferSize`: 64KB
- `ReadBufferSize`: 64KB
- `EnablePersistence`: false
- `DataDirectory`: "./data"
- `WALEnabled`: false
- `EnableTTL`: true
- `CleanupInterval`: 5 minutes
- `LogLevel`: "info"

### Custom Configuration

```go
config := types.Config{
    MaxMemorySize:     2 * 1024 * 1024 * 1024, // 2GB
    MaxKeySize:        2048,                   // 2KB
    MaxValueSize:      2 * 1024 * 1024,        // 2MB
    WriteBufferSize:   128 * 1024,             // 128KB
    ReadBufferSize:    128 * 1024,             // 128KB
    EnablePersistence: true,
    DataDirectory:     "./mydata",
    WALEnabled:        true,
    EnableTTL:         true,
    CleanupInterval:   time.Minute * 10,
    LogLevel:          "debug",
}

db := engine.NewInMemoryDBWithConfig(config)
```

### Configuration Options

#### MaxMemorySize
Maximum memory usage in bytes. Used for in-memory storage limits.

#### MaxKeySize
Maximum key size in bytes. Default: 1KB.

#### MaxValueSize
Maximum value size in bytes. Default: 1MB.

#### WriteBufferSize
Write buffer size for disk operations. Larger buffers improve write performance but use more memory.

#### ReadBufferSize
Read buffer size for disk operations. Larger buffers improve read performance but use more memory.

#### EnablePersistence
Enable disk-based persistence. Required for disk-based storage.

#### DataDirectory
Directory path for persistent data storage.

#### WALEnabled
Enable Write-Ahead Logging. Only available for disk-based storage.

#### EnableTTL
Enable Time-to-Live support for automatic key expiration.

#### CleanupInterval
Interval for automatic cleanup of expired keys.

#### LogLevel
Logging level: "debug", "info", "warn", "error".

---

## Performance Considerations

### Performance Characteristics

Based on benchmarks (Apple M2):

#### In-Memory Storage
- **Set Operations**: ~496ns/op
- **Get Operations**: ~68ns/op
- **Delete Operations**: ~240ns/op
- **Batch Set (10 items)**: ~5.3μs/op
- **Batch Get (10 items)**: ~1.1μs/op

#### Disk-Based Storage
- **Set Operations**: ~1.09ms/op
- **Get Operations**: ~2.24μs/op
- **Performance Ratio**: Disk is ~403x slower for writes, ~28x slower for reads

#### Write-Ahead Logging (WAL)
- **WAL Overhead**: ~2400% performance impact (significant but acceptable for durability)
- **WAL Recovery**: <200ms for 1,000 operations
- **Data Durability**: 100% crash recovery guarantee

### Optimization Tips

1. **Use In-Memory for Temporary Data**
   - Fastest performance
   - Use for caching and temporary storage

2. **Use Batch Operations**
   - More efficient than individual operations
   - Reduces overhead for multiple operations

3. **Configure Buffer Sizes**
   - Larger buffers improve disk I/O performance
   - Balance between memory usage and performance

4. **Monitor WAL Size**
   - Rotate WAL when it approaches maximum size
   - Consider larger WAL for high-write applications

5. **Use Compaction**
   - Periodically compact disk-based databases
   - Reclaims disk space from deleted keys

6. **TTL Cleanup**
   - Set appropriate cleanup intervals
   - Manually trigger cleanup if needed

7. **Transaction Batching**
   - Group related operations in transactions
   - Reduces overhead for multiple operations

---

## Troubleshooting

### Common Issues

#### "key not found" Error

**Problem:** Attempting to get a key that doesn't exist.

**Solution:**
```go
value, err := db.Get("key1")
if err == types.ErrKeyNotFound {
    // Handle not found case
    fmt.Println("Key does not exist")
}
```

#### "key has expired" Error

**Problem:** Key has expired due to TTL.

**Solution:**
```go
value, err := db.Get("key1")
if err == types.ErrKeyExpired {
    // Key expired, set a new value if needed
    db.Set("key1", []byte("new value"))
}
```

#### "database is closed" Error

**Problem:** Attempting to use a closed database.

**Solution:**
```go
if db.IsClosed() {
    // Reopen database or handle appropriately
    db, err = engine.NewDiskDB("./data")
}
```

#### "backup not supported" Error

**Problem:** Attempting to use backup features on in-memory database.

**Solution:** Use disk-based database with WAL:
```go
db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
```

#### "WAL not supported" Error

**Problem:** Attempting to use WAL operations on database without WAL.

**Solution:** Create database with WAL enabled:
```go
db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
```

#### Transaction Already Active

**Problem:** Attempting to begin a transaction while one is already active.

**Solution:** Commit or rollback the current transaction first:
```go
if currentTx != nil {
    currentTx.Commit() // or Rollback()
}
tx, err := db.Begin()
```

#### Data Loss After Crash

**Problem:** Data lost after application crash.

**Solution:** Use disk-based database with WAL:
```go
db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
```

#### High Memory Usage

**Problem:** Database using too much memory.

**Solutions:**
- Use disk-based storage instead of in-memory
- Reduce `MaxMemorySize` in configuration
- Use TTL to automatically expire old keys
- Periodically clean up expired keys

#### Slow Performance

**Problem:** Database operations are slow.

**Solutions:**
- Use in-memory database for temporary data
- Use batch operations instead of individual operations
- Increase buffer sizes in configuration
- Use compaction for disk-based storage
- Monitor and rotate WAL if needed

#### Disk Space Issues

**Problem:** Running out of disk space.

**Solutions:**
- Run compaction to reclaim space
- Delete old backups
- Clear WAL if safe to do so
- Use TTL to automatically expire old data

---

## Examples

### Example 1: Simple Key-Value Store

```go
package main

import (
    "database_engine/engine"
    "fmt"
)

func main() {
    db := engine.NewInMemoryDB()
    defer db.Close()
    
    // Set values
    db.Set("name", []byte("John Doe"))
    db.Set("age", []byte("30"))
    db.Set("city", []byte("New York"))
    
    // Get values
    name, _ := db.Get("name")
    age, _ := db.Get("age")
    city, _ := db.Get("city")
    
    fmt.Printf("Name: %s\n", string(name))
    fmt.Printf("Age: %s\n", string(age))
    fmt.Printf("City: %s\n", string(city))
}
```

### Example 2: Session Management with TTL

```go
package main

import (
    "database_engine/engine"
    "fmt"
    "time"
)

func main() {
    db := engine.NewInMemoryDB()
    defer db.Close()
    
    // Create session with 1 hour TTL
    sessionID := "session:abc123"
    db.SetWithTTL(sessionID, []byte("user:123"), time.Hour)
    
    // Check if session exists
    session, err := db.Get(sessionID)
    if err != nil {
        fmt.Println("Session expired or not found")
    } else {
        fmt.Printf("Session: %s\n", string(session))
    }
    
    // Cleanup expired sessions
    count := db.CleanupExpired()
    fmt.Printf("Cleaned up %d expired sessions\n", count)
}
```

### Example 3: Transaction Example

```go
package main

import (
    "database_engine/engine"
    "fmt"
    "log"
)

func main() {
    db := engine.NewInMemoryDB()
    defer db.Close()
    
    // Begin transaction
    tx, err := db.Begin()
    if err != nil {
        log.Fatal(err)
    }
    
    // Perform multiple operations
    operations := []struct {
        key   string
        value string
    }{
        {"user:1", "Alice"},
        {"user:2", "Bob"},
        {"user:3", "Charlie"},
    }
    
    for _, op := range operations {
        if err := tx.Set(op.key, []byte(op.value)); err != nil {
            tx.Rollback()
            log.Fatal(err)
        }
    }
    
    // Commit transaction
    if err := tx.Commit(); err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Transaction committed successfully")
}
```

### Example 4: Persistent Database with Backup

```go
package main

import (
    "database_engine/engine"
    "fmt"
    "log"
)

func main() {
    // Create disk-based database with WAL
    db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Store some data
    db.Set("important:data", []byte("critical information"))
    
    // Create backup
    metadata, err := db.CreateBackup("Before important operation")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Backup created: %s\n", metadata.Timestamp.Format("20060102_150405"))
    
    // List backups
    backups, err := db.ListBackups()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Total backups: %d\n", len(backups))
}
```

### Example 5: Batch Operations

```go
package main

import (
    "database_engine/engine"
    "database_engine/types"
    "fmt"
    "time"
)

func main() {
    db := engine.NewInMemoryDB()
    defer db.Close()
    
    // Batch set
    entries := []types.Entry{
        {Key: "user:1", Value: []byte("Alice"), Timestamp: time.Now()},
        {Key: "user:2", Value: []byte("Bob"), Timestamp: time.Now()},
        {Key: "user:3", Value: []byte("Charlie"), Timestamp: time.Now()},
    }
    db.BatchSet(entries)
    
    // Batch get
    keys := []types.Key{"user:1", "user:2", "user:3"}
    values, err := db.BatchGet(keys)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    
    for key, value := range values {
        fmt.Printf("%s = %s\n", key, string(value))
    }
}
```

### Example 6: Data Integrity Check

```go
package main

import (
    "database_engine/engine"
    "fmt"
    "log"
)

func main() {
    db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Validate data integrity
    isValid, issues, err := db.ValidateDataIntegrity()
    if err != nil {
        log.Fatal(err)
    }
    
    if isValid {
        fmt.Println("Data integrity: OK")
    } else {
        fmt.Println("Data integrity: FAILED")
        for _, issue := range issues {
            fmt.Printf("  - %s\n", issue)
        }
    }
}
```

---

## Additional Resources

### Demo Programs

The project includes several demo programs:

```bash
# Basic demo
make demo

# Disk-based demo
make disk-demo

# WAL demo
make wal-demo

# Persistence demo
make persistence-demo

# Transaction demo
make transaction-demo
```

### Testing

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Run benchmarks
make run-benchmarks
```

### Project Structure

```
database_engine/
├── types/           # Core type definitions
├── storage/         # Storage engine implementations
├── engine/          # Main database engine
├── wal/             # Write-Ahead Logging
├── persistence/     # Backup and recovery
├── cmd/
│   ├── cli/         # CLI tool
│   ├── demo/        # Demo applications
│   └── ...
└── ...
```

---

## Support

For issues, questions, or contributions, please refer to the project repository.

---

**Version:** 1.0.0  
**Last Updated:** 2024

