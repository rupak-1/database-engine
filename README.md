# Custom Database Engine

A high-performance in-memory and disk-based key-value store written in Go.

## Features

### Phase 1 (Current)
- ✅ In-memory key-value storage
- ✅ Core database interface
- ✅ Basic CRUD operations
- ✅ Thread-safe operations

### Future Phases
- 🔄 Disk-based storage engine
- 🔄 Write-Ahead Logging (WAL)
- 🔄 Data persistence and recovery
- 🔄 Transaction support
- 🔄 Indexing and querying
- 🔄 Replication and clustering
- 🔄 Performance optimizations

## Usage

```go
package main

import (
    "fmt"
    "database_engine/engine"
)

func main() {
    db := engine.NewInMemoryDB()
    
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
