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

### Phase 3 (Completed)
- Write-Ahead Logging (WAL)
- WAL recovery and rotation
- Crash recovery mechanisms
- Data durability guarantees

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
