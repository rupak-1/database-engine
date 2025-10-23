package persistence_test

import (
	"database_engine/persistence"
	"database_engine/storage"
	"database_engine/types"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBackupManager(t *testing.T) {
	tempDir := t.TempDir()

	bm, err := persistence.NewBackupManager(tempDir)
	assert.NoError(t, err)
	assert.NotNil(t, bm)
	assert.Equal(t, 0, bm.GetBackupCount())
}

func TestCreateFullBackup(t *testing.T) {
	tempDir := t.TempDir()

	// Create a disk storage with some data
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	// Add some test data
	err = diskStorage.Set("key1", []byte("value1"))
	require.NoError(t, err)

	err = diskStorage.Set("key2", []byte("value2"))
	require.NoError(t, err)

	err = diskStorage.SetWithTTL("key3", []byte("value3"), time.Hour)
	require.NoError(t, err)

	err = diskStorage.Close()
	require.NoError(t, err)

	// Create backup manager
	bm, err := persistence.NewBackupManager(tempDir)
	require.NoError(t, err)

	// Create backup
	metadata, err := bm.CreateFullBackup("Test backup")
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, "full", metadata.BackupType)
	assert.Equal(t, "Test backup", metadata.Description)
	assert.Greater(t, metadata.EntryCount, int64(0))
	assert.Greater(t, metadata.DataSize, int64(0))
	assert.NotEmpty(t, metadata.Checksum)

	// Verify backup directory exists
	backupDir := filepath.Join(tempDir, "backups")
	assert.DirExists(t, backupDir)

	// Verify metadata file exists
	backupName := fmt.Sprintf("backup_%s", metadata.Timestamp.Format("20060102_150405"))
	metadataFile := filepath.Join(backupDir, backupName, "metadata.json")
	assert.FileExists(t, metadataFile)
}

func TestListBackups(t *testing.T) {
	tempDir := t.TempDir()

	bm, err := persistence.NewBackupManager(tempDir)
	require.NoError(t, err)

	// Initially no backups
	backups, err := bm.ListBackups()
	assert.NoError(t, err)
	assert.Len(t, backups, 0)

	// Create some test data and backup
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Create backup
	_, err = bm.CreateFullBackup("Test backup")
	require.NoError(t, err)

	// List backups
	backups, err = bm.ListBackups()
	assert.NoError(t, err)
	assert.Len(t, backups, 1)

	// Verify backup details
	for _, backup := range backups {
		assert.Equal(t, "full", backup.BackupType)
		assert.NotEmpty(t, backup.Checksum)
		assert.Greater(t, backup.Timestamp, time.Time{})
	}
}

func TestRestoreFromBackup(t *testing.T) {
	tempDir := t.TempDir()

	// Create initial data
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("original", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Create backup
	bm, err := persistence.NewBackupManager(tempDir)
	require.NoError(t, err)

	metadata, err := bm.CreateFullBackup("Test restore backup")
	require.NoError(t, err)

	// Modify data after backup
	diskStorage, err = storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("modified", []byte("new data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Restore from backup
	backupName := fmt.Sprintf("backup_%s", metadata.Timestamp.Format("20060102_150405"))
	err = bm.RestoreFromBackup(backupName)
	assert.NoError(t, err)

	// Verify data was restored
	diskStorage, err = storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	// Original data should exist
	value, err := diskStorage.Get("original")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("data"), value)

	// Modified data should not exist
	_, err = diskStorage.Get("modified")
	assert.Error(t, err)
	assert.Equal(t, types.ErrKeyNotFound, err)

	err = diskStorage.Close()
	require.NoError(t, err)
}

func TestDeleteBackup(t *testing.T) {
	tempDir := t.TempDir()

	bm, err := persistence.NewBackupManager(tempDir)
	require.NoError(t, err)

	// Create test data and backup
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	metadata, err := bm.CreateFullBackup("Test delete backup")
	require.NoError(t, err)

	// Verify backup exists
	backups, err := bm.ListBackups()
	require.NoError(t, err)
	assert.Len(t, backups, 1)

	// Delete backup
	backupName := fmt.Sprintf("backup_%s", metadata.Timestamp.Format("20060102_150405"))
	err = bm.DeleteBackup(backupName)
	assert.NoError(t, err)

	// Verify backup was deleted
	backups, err = bm.ListBackups()
	assert.NoError(t, err)
	assert.Len(t, backups, 0)
}

func TestGetBackupInfo(t *testing.T) {
	tempDir := t.TempDir()

	bm, err := persistence.NewBackupManager(tempDir)
	require.NoError(t, err)

	// Create test data and backup
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	metadata, err := bm.CreateFullBackup("Test info backup")
	require.NoError(t, err)

	// Get backup info
	backupName := fmt.Sprintf("backup_%s", metadata.Timestamp.Format("20060102_150405"))
	info, err := bm.GetBackupInfo(backupName)
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, metadata.Timestamp.Unix(), info.Timestamp.Unix())
	assert.Equal(t, "Test info backup", info.Description)
	assert.Equal(t, "full", info.BackupType)
}

func TestNewRecoveryManager(t *testing.T) {
	tempDir := t.TempDir()

	rm, err := persistence.NewRecoveryManager(tempDir)
	assert.NoError(t, err)
	assert.NotNil(t, rm)

	// Check default state
	state := rm.GetRecoveryState()
	assert.Equal(t, "auto", state.RecoveryMode)
	assert.Equal(t, 0, state.RecoveryCount)
}

func TestPerformRecovery(t *testing.T) {
	tempDir := t.TempDir()

	rm, err := persistence.NewRecoveryManager(tempDir)
	require.NoError(t, err)

	// Create some test data first
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("recovery_test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Perform recovery on directory with data
	err = rm.PerformRecovery()
	assert.NoError(t, err)

	// Check recovery state
	state := rm.GetRecoveryState()
	assert.Equal(t, 1, state.RecoveryCount)
	assert.False(t, state.LastRecovery.IsZero())
}

func TestCreateRecoveryPoint(t *testing.T) {
	tempDir := t.TempDir()

	rm, err := persistence.NewRecoveryManager(tempDir)
	require.NoError(t, err)

	// Create test data
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("recovery_test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Create recovery point
	metadata, err := rm.CreateRecoveryPoint("Test recovery point")
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Contains(t, metadata.Description, "Test recovery point")
	assert.Equal(t, "full", metadata.BackupType)
}

func TestSetRecoveryMode(t *testing.T) {
	tempDir := t.TempDir()

	rm, err := persistence.NewRecoveryManager(tempDir)
	require.NoError(t, err)

	// Test valid modes
	modes := []string{"auto", "manual", "backup"}
	for _, mode := range modes {
		err = rm.SetRecoveryMode(mode)
		assert.NoError(t, err)

		state := rm.GetRecoveryState()
		assert.Equal(t, mode, state.RecoveryMode)
	}

	// Test invalid mode
	err = rm.SetRecoveryMode("invalid")
	assert.Error(t, err)
}

func TestValidateDataIntegrity(t *testing.T) {
	tempDir := t.TempDir()

	rm, err := persistence.NewRecoveryManager(tempDir)
	require.NoError(t, err)

	// Test empty directory
	isValid, issues, err := rm.ValidateDataIntegrity()
	assert.NoError(t, err)
	assert.False(t, isValid)
	assert.NotEmpty(t, issues)

	// Create valid data
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Test with valid data
	isValid, issues, err = rm.ValidateDataIntegrity()
	assert.NoError(t, err)
	assert.True(t, isValid)
	assert.Empty(t, issues)
}

func TestForceRecoveryFromBackup(t *testing.T) {
	tempDir := t.TempDir()

	rm, err := persistence.NewRecoveryManager(tempDir)
	require.NoError(t, err)

	// Create test data and backup
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("backup_test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Create recovery point
	metadata, err := rm.CreateRecoveryPoint("Test force recovery")
	require.NoError(t, err)

	// Modify data
	diskStorage, err = storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("modified", []byte("new data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Force recovery from backup
	backupName := fmt.Sprintf("backup_%s", metadata.Timestamp.Format("20060102_150405"))
	err = rm.ForceRecoveryFromBackup(backupName)
	assert.NoError(t, err)

	// Check recovery state
	state := rm.GetRecoveryState()
	assert.Equal(t, "backup", state.RecoveryMode)
	assert.Equal(t, backupName, state.LastBackup)
	assert.True(t, state.BackupRecovery)
	assert.True(t, state.DataIntegrity)
}

func TestRecoveryConcurrency(t *testing.T) {
	tempDir := t.TempDir()

	rm, err := persistence.NewRecoveryManager(tempDir)
	require.NoError(t, err)

	// Create some test data first
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("concurrent_test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Test concurrent recovery operations
	done := make(chan bool, 5)

	for i := 0; i < 5; i++ {
		go func(i int) {
			err := rm.PerformRecovery()
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 5; i++ {
		<-done
	}

	// Check final state
	state := rm.GetRecoveryState()
	assert.Equal(t, 5, state.RecoveryCount)
}

func TestBackupIntegrity(t *testing.T) {
	tempDir := t.TempDir()

	bm, err := persistence.NewBackupManager(tempDir)
	require.NoError(t, err)

	// Create test data
	diskStorage, err := storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	err = diskStorage.Set("integrity_test", []byte("data"))
	require.NoError(t, err)
	err = diskStorage.Close()
	require.NoError(t, err)

	// Create backup
	metadata, err := bm.CreateFullBackup("Integrity test backup")
	require.NoError(t, err)

	// Verify backup integrity by restoring
	backupName := fmt.Sprintf("backup_%s", metadata.Timestamp.Format("20060102_150405"))
	err = bm.RestoreFromBackup(backupName)
	assert.NoError(t, err)

	// Verify data integrity
	diskStorage, err = storage.NewDiskStorage(tempDir)
	require.NoError(t, err)

	value, err := diskStorage.Get("integrity_test")
	assert.NoError(t, err)
	assert.Equal(t, types.Value("data"), value)

	err = diskStorage.Close()
	require.NoError(t, err)
}
