# Custom Database Engine

A high-performance in-memory and disk-based key-value store written in Go.

## Features

### Phase 1 (Completed)
- In-memory key-value storage
- Core database interface
- Basic CRUD operations
- Thread-safe operations
- TTL support with cleanup

### Phase 2 (Completed)
- Disk-based storage engine
- Data persistence and recovery
- Automatic compaction
- Performance optimization

### Phase 4 (Completed)
- Data persistence and recovery mechanisms
- Backup and restore functionality
- Data integrity validation
- Recovery point creation
- Comprehensive error handling

### Phase 5 (Completed)
- Transaction support with ACID properties
- Atomic operations guarantee
- Consistency validation
- Transaction isolation
- Rollback support

### Phase 6 (Completed)
- CLI tool for database management
- Interactive command-line interface
- Database operations (CRUD)
- Backup and recovery management
- Transaction support
- Status monitoring

## Usage

### In-Memory Database
```go
package main

import (
    "fmt"
    "database_engine/engine"
)

func main() {
    db := engine.NewInMemoryDB()
    defer db.Close()
    
    // Set a key-value pair
    err := db.Set("user:1", "John Doe")
    if err != nil {
        panic(err)
    }
    
    // Get a value
    value, err := db.Get("user:1")
    if err != nil {
        panic(err)
    }
    fmt.Println("Value:", value)
    
    // Delete a key
    err = db.Delete("user:1")
    if err != nil {
        panic(err)
    }
}
```

### Persistence and Recovery
```go
package main

import (
    "database_engine/engine"
    "fmt"
    "log"
)

func main() {
    // Create database with persistence and recovery
    db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Check persistence support
    fmt.Printf("Backup Supported: %t\n", db.IsBackupSupported())
    fmt.Printf("Recovery Supported: %t\n", db.IsRecoverySupported())

    // Create backup
    metadata, err := db.CreateBackup("Important data backup")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Backup created: %s\n", metadata.Timestamp.Format("2006-01-02 15:04:05"))

    // List backups
    backups, err := db.ListBackups()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Found %d backups\n", len(backups))

    // Validate data integrity
    isValid, issues, err := db.ValidateDataIntegrity()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Data Integrity: %t\n", isValid)
    if len(issues) > 0 {
        fmt.Println("Issues found:", issues)
    }

    // Create recovery point before risky operation
    recoveryPoint, err := db.CreateRecoveryPoint("Before risky operation")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Recovery point created: %s\n", recoveryPoint.Timestamp.Format("2006-01-02 15:04:05"))

    // Restore from backup if needed
    if len(backups) > 0 {
        backupName := backups[0].Timestamp.Format("20060102_150405")
        err = db.RestoreFromBackup(backupName)
        if err != nil {
            log.Printf("Restore failed: %v", err)
        } else {
            fmt.Println("Restore completed successfully")
        }
    }
}
```

### Transactions
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

    // Perform operations within transaction
    err = tx.Set("key1", []byte("value1"))
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }

    err = tx.Set("key2", []byte("value2"))
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }

    // Commit transaction
    err = tx.Commit()
    if err != nil {
        log.Fatal(err)
    }

    // Or rollback if needed
    // err = tx.Rollback()
}
```

### Disk-Based Database with WAL
```go
package main

import (
    "database_engine/engine"
    "fmt"
    "log"
    "time"
)

func main() {
    // Create database with WAL enabled
    db, err := engine.NewDiskDBWithWAL("./data", 10*1024*1024) // 10MB WAL
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Check WAL status
    fmt.Printf("WAL Enabled: %t\n", db.IsWALEnabled())
    
    walSize, err := db.GetWALSize()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("WAL Size: %d bytes\n", walSize)

    // Perform operations (automatically logged to WAL)
    err = db.Set("key1", []byte("value1"))
    if err != nil {
        log.Fatal(err)
    }

    err = db.SetWithTTL("session", []byte("active"), time.Hour)
    if err != nil {
        log.Fatal(err)
    }

    // WAL operations
    if walSize > 5*1024*1024 { // 5MB threshold
        err = db.RotateWAL()
        if err != nil {
            log.Printf("WAL rotation failed: %v", err)
        }
    }

    // Clear WAL if needed
    err = db.ClearWAL()
    if err != nil {
        log.Printf("WAL clear failed: %v", err)
    }
}
```

### Disk-Based Database
```go
package main

import (
    "fmt"
    "database_engine/engine"
)

func main() {
    db, err := engine.NewDiskDB("./data")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // Set a key-value pair
    err = db.Set("user:1", "John Doe")
    if err != nil {
        panic(err)
    }
    
    // Get a value
    value, err := db.Get("user:1")
    if err != nil {
        panic(err)
    }
    fmt.Println("Value:", value)
    
    // Compact database
    err = db.Compact()
    if err != nil {
        panic(err)
    }
    
    // Get disk usage
    usage, err := db.GetDiskUsage()
    if err != nil {
        panic(err)
    }
    fmt.Println("Disk usage:", usage, "bytes")
}
```

## Architecture

The database engine is designed with a modular architecture focused on core functionality:

- **Core Interface**: Defines the contract for all storage engines
- **In-Memory Engine**: Fast, volatile storage for temporary data
- **Disk Engine**: Persistent storage with automatic compaction
- **Transaction Manager**: ACID transaction support
- **Backup & Recovery**: Data persistence and recovery mechanisms

## Design Philosophy

This database engine prioritizes:
- **Simplicity**: Core functionality without complex features
- **Performance**: High-speed operations with minimal overhead
- **Reliability**: Thread-safe operations and data persistence
- **Extensibility**: Clean interfaces for future enhancements

### CLI Tool

The database engine includes a comprehensive CLI tool for interactive database management.

#### Building the CLI

```bash
# Build the CLI tool
make cli-build

# Or manually
go build -o bin/dbcli cmd/cli/main.go
```

#### Using the CLI

```bash
# Run in interactive mode
./bin/dbcli

# Or use make
make cli
```

#### Example CLI Session

```
db> connect ./mydata 10
Connected to database at: ./mydata (WAL: 10 MB)

db> set user:1 "Alice Johnson"
Set: user:1 = Alice Johnson

db> set user:2 "Bob Smith"
Set: user:2 = Bob Smith

db> get user:1
user:1 = Alice Johnson

db> keys
Keys (2):
  user:1
  user:2

db> begin
Transaction started

db> set user:3 "Charlie Brown"
Set (in transaction): user:3 = Charlie Brown

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

db> help
Available Commands:
...
```

## Development

```bash
# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./...

# Build
go build ./...

# Build CLI tool
make cli-build

# Run CLI tool
make cli
```
