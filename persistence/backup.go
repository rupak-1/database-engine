package persistence

import (
	"database_engine/types"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// BackupMetadata contains information about a backup
type BackupMetadata struct {
	Timestamp   time.Time `json:"timestamp"`
	Version     string    `json:"version"`
	EntryCount  int64     `json:"entry_count"`
	DataSize    int64     `json:"data_size"`
	IndexSize   int64     `json:"index_size"`
	WALSize     int64     `json:"wal_size"`
	Checksum    string    `json:"checksum"`
	BackupType  string    `json:"backup_type"` // "full", "incremental"
	Description string    `json:"description"`
}

// BackupManager handles backup and restore operations
type BackupManager struct {
	dataDir     string
	backupDir   string
	mu          sync.RWMutex
	lastBackup  *BackupMetadata
	backupCount int
}

// NewBackupManager creates a new backup manager
func NewBackupManager(dataDir string) (*BackupManager, error) {
	backupDir := filepath.Join(dataDir, "backups")

	// Create backup directory if it doesn't exist
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	bm := &BackupManager{
		dataDir:   dataDir,
		backupDir: backupDir,
	}

	// Load existing backup metadata
	if err := bm.loadBackupMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load backup metadata: %w", err)
	}

	return bm, nil
}

// CreateFullBackup creates a complete backup of the database
func (bm *BackupManager) CreateFullBackup(description string) (*BackupMetadata, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	timestamp := time.Now()
	backupName := fmt.Sprintf("backup_%s", timestamp.Format("20060102_150405"))
	backupPath := filepath.Join(bm.backupDir, backupName)

	// Create backup directory
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Copy data files
	dataFiles := []string{"data.db", "index.db", "wal.log"}
	var totalSize int64
	var entryCount int64

	for _, file := range dataFiles {
		srcPath := filepath.Join(bm.dataDir, file)
		dstPath := filepath.Join(backupPath, file)

		if err := bm.copyFile(srcPath, dstPath); err != nil {
			// File might not exist, that's okay
			continue
		}

		// Get file size
		if stat, err := os.Stat(dstPath); err == nil {
			totalSize += stat.Size()
		}
	}

	// Count entries from index file
	if indexPath := filepath.Join(backupPath, "index.db"); bm.fileExists(indexPath) {
		if count, err := bm.countEntriesFromIndex(indexPath); err == nil {
			entryCount = count
		}
	}

	// Create metadata
	metadata := &BackupMetadata{
		Timestamp:   timestamp,
		Version:     "1.0.0",
		EntryCount:  entryCount,
		DataSize:    totalSize,
		IndexSize:   0, // Will be calculated
		WALSize:     0, // Will be calculated
		Checksum:    "", // Will be calculated
		BackupType:  "full",
		Description: description,
	}

	// Calculate checksum (excluding metadata.json)
	metadata.Checksum = bm.calculateChecksum(backupPath)

	// Save metadata
	if err := bm.saveBackupMetadata(backupPath, metadata); err != nil {
		return nil, fmt.Errorf("failed to save backup metadata: %w", err)
	}

	bm.lastBackup = metadata
	bm.backupCount++

	return metadata, nil
}

// RestoreFromBackup restores the database from a backup
func (bm *BackupManager) RestoreFromBackup(backupName string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	backupPath := filepath.Join(bm.backupDir, backupName)

	// Check if backup exists
	if !bm.fileExists(backupPath) {
		return fmt.Errorf("backup %s not found", backupName)
	}

	// Load backup metadata
	metadata, err := bm.loadBackupMetadataFromPath(backupPath)
	if err != nil {
		return fmt.Errorf("failed to load backup metadata: %w", err)
	}

	// Verify backup integrity
	if err := bm.verifyBackupIntegrity(backupPath, metadata); err != nil {
		return fmt.Errorf("backup integrity check failed: %w", err)
	}

	// Create temporary directory for current data
	tempDir := filepath.Join(bm.dataDir, "temp_restore")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Backup current data
	if err := bm.backupCurrentData(tempDir); err != nil {
		return fmt.Errorf("failed to backup current data: %w", err)
	}

	// Restore from backup
	if err := bm.restoreBackupFiles(backupPath); err != nil {
		// Restore current data if restore fails
		bm.restoreCurrentData(tempDir)
		return fmt.Errorf("failed to restore backup: %w", err)
	}

	return nil
}

// ListBackups returns a list of available backups
func (bm *BackupManager) ListBackups() ([]BackupMetadata, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var backups []BackupMetadata

	entries, err := os.ReadDir(bm.backupDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() && entry.Name()[:7] == "backup_" {
			backupPath := filepath.Join(bm.backupDir, entry.Name())
			metadata, err := bm.loadBackupMetadataFromPath(backupPath)
			if err != nil {
				continue // Skip invalid backups
			}
			backups = append(backups, *metadata)
		}
	}

	return backups, nil
}

// DeleteBackup removes a backup
func (bm *BackupManager) DeleteBackup(backupName string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	backupPath := filepath.Join(bm.backupDir, backupName)

	if !bm.fileExists(backupPath) {
		return fmt.Errorf("backup %s not found", backupName)
	}

	return os.RemoveAll(backupPath)
}

// GetBackupInfo returns information about a specific backup
func (bm *BackupManager) GetBackupInfo(backupName string) (*BackupMetadata, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	backupPath := filepath.Join(bm.backupDir, backupName)
	return bm.loadBackupMetadataFromPath(backupPath)
}

// Helper methods

func (bm *BackupManager) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func (bm *BackupManager) fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func (bm *BackupManager) countEntriesFromIndex(indexPath string) (int64, error) {
	file, err := os.Open(indexPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var index map[types.Key]int64
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&index); err != nil {
		return 0, err
	}

	return int64(len(index)), nil
}

func (bm *BackupManager) calculateChecksum(backupPath string) string {
	// Simple checksum calculation - in production, use crypto/sha256
	var checksum int64
	
	filepath.Walk(backupPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Base(path) != "metadata.json" {
			checksum += info.Size()
		}
		return nil
	})

	return fmt.Sprintf("%x", checksum)
}

func (bm *BackupManager) saveBackupMetadata(backupPath string, metadata *BackupMetadata) error {
	metadataPath := filepath.Join(backupPath, "metadata.json")

	file, err := os.Create(metadataPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(metadata)
}

func (bm *BackupManager) loadBackupMetadata() error {
	// Load the most recent backup metadata
	backups, err := bm.ListBackups()
	if err != nil {
		return err
	}

	if len(backups) > 0 {
		// Find the most recent backup
		var latest *BackupMetadata
		for i := range backups {
			if latest == nil || backups[i].Timestamp.After(latest.Timestamp) {
				latest = &backups[i]
			}
		}
		bm.lastBackup = latest
		bm.backupCount = len(backups)
	}

	return nil
}

func (bm *BackupManager) loadBackupMetadataFromPath(backupPath string) (*BackupMetadata, error) {
	metadataPath := filepath.Join(backupPath, "metadata.json")

	file, err := os.Open(metadataPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var metadata BackupMetadata
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (bm *BackupManager) verifyBackupIntegrity(backupPath string, metadata *BackupMetadata) error {
	// Verify checksum
	calculatedChecksum := bm.calculateChecksum(backupPath)
	if calculatedChecksum != metadata.Checksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", metadata.Checksum, calculatedChecksum)
	}

	// Verify required files exist
	requiredFiles := []string{"metadata.json"}
	for _, file := range requiredFiles {
		if !bm.fileExists(filepath.Join(backupPath, file)) {
			return fmt.Errorf("required file %s not found in backup", file)
		}
	}

	return nil
}

func (bm *BackupManager) backupCurrentData(tempDir string) error {
	files := []string{"data.db", "index.db", "wal.log"}

	for _, file := range files {
		srcPath := filepath.Join(bm.dataDir, file)
		dstPath := filepath.Join(tempDir, file)

		if bm.fileExists(srcPath) {
			if err := bm.copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bm *BackupManager) restoreBackupFiles(backupPath string) error {
	files := []string{"data.db", "index.db", "wal.log"}

	for _, file := range files {
		srcPath := filepath.Join(backupPath, file)
		dstPath := filepath.Join(bm.dataDir, file)

		if bm.fileExists(srcPath) {
			if err := bm.copyFile(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			// Remove file if it doesn't exist in backup
			os.Remove(dstPath)
		}
	}

	return nil
}

func (bm *BackupManager) restoreCurrentData(tempDir string) error {
	files := []string{"data.db", "index.db", "wal.log"}

	for _, file := range files {
		srcPath := filepath.Join(tempDir, file)
		dstPath := filepath.Join(bm.dataDir, file)

		if bm.fileExists(srcPath) {
			if err := bm.copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetLastBackup returns the most recent backup metadata
func (bm *BackupManager) GetLastBackup() *BackupMetadata {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.lastBackup
}

// GetBackupCount returns the total number of backups
func (bm *BackupManager) GetBackupCount() int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return bm.backupCount
}
