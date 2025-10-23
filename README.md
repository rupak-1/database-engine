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

### Future Phases
- Transaction support
- Indexing and querying
- Replication and clustering

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
- **Transaction Manager**: ACID transaction support (planned)
- **Index Manager**: Efficient data indexing and querying (planned)

## Design Philosophy

This database engine prioritizes:
- **Simplicity**: Core functionality without complex features
- **Performance**: High-speed operations with minimal overhead
- **Reliability**: Thread-safe operations and data persistence
- **Extensibility**: Clean interfaces for future enhancements

## Development

```bash
# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./...

# Build
go build ./...
```
