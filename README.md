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
- TTL support with cleanup

### Future Phases
- Write-Ahead Logging (WAL)
- Transaction support
- Indexing and querying
- Replication and clustering
- Performance optimizations

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

The database engine is designed with a modular architecture:

- **Core Interface**: Defines the contract for all storage engines
- **In-Memory Engine**: Fast, volatile storage for temporary data
- **Disk Engine**: Persistent storage with WAL and recovery
- **Transaction Manager**: ACID transaction support
- **Index Manager**: Efficient data indexing and querying

## Development

```bash
# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./...

# Build
go build ./...
```
