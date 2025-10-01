# Custom Database Engine

A high-performance in-memory and disk-based key-value store written in Go.

## Features

### Phase 1 (Current)
- In-memory key-value storage
- Core database interface
- Basic CRUD operations
- Thread-safe operations

### Phase 2 (Current)
- Disk-based storage engine
- Data persistence and recovery
- Automatic compaction
- Performance optimization

### Future Phases
- Write-Ahead Logging (WAL)
- Transaction support
- Indexing and querying
- Replication and clustering

### Not Implemented
- TTL (Time-To-Live) support
- Compression and optimization features
- Monitoring and metrics collection
- Comprehensive documentation

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
