package main

import (

	//"strings"
	"fmt"
	"testing"
)

// testBucket and testFile are defined in test_utils.go
// setupTestEnvironment and cleanupTestEnvironment are defined in test_utils.go

func TestObjectUpload(t *testing.T) {
	// 1. Setup: Create or clean bucket
	t.Log("Setting up test environment...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment and create bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Upload file through gateway
	t.Log("Uploading test file through gateway...")
	if err := uploadTestFile(t); err != nil {
		t.Fatalf("Failed to upload test file: %v", err)
	}

	// 3. Verify files in storage systems
	t.Log("Verifying files in storage systems...")
	firstErr, secondErr := verifyObjectsDirectly(t, *localMinioForTesting, testBucket, testFile)
	if firstErr != nil {
		t.Fatalf("Failed to verify files in first storage: %v", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("Failed to verify files in second storage: %v", secondErr)
	}
}

func TestObjectDownload(t *testing.T) {
	// 1. Setup: Create or clean bucket
	t.Log("Setting up test environment...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment and create bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Upload a test file first
	t.Log("Uploading test file through gateway...")
	if err := uploadTestFile(t); err != nil {
		t.Fatalf("Failed to upload test file: %v", err)
	}

	// Verify the file exists in both storage systems before downloading
	t.Log("Verifying file exists in storage systems...")
	firstErr, secondErr := verifyObjectsDirectly(t, *localMinioForTesting, testBucket, testFile)
	if firstErr != nil {
		t.Fatalf("Failed to verify files in first storage: %v", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("Failed to verify files in second storage: %v", secondErr)
	}

	// 3. Download the file through gateway
	t.Log("Downloading test file through gateway...")
	downloadedFile, err := downloadTestFile(t, testFile)
	if err != nil {
		t.Fatalf("Failed to download test file: %v", err)
	}
	defer cleanupTestFile(t, downloadedFile)

	// 4. Verify the downloaded file
	t.Log("Verifying downloaded file...")
	if err := verifyDownloadedFile(t, downloadedFile); err != nil {
		t.Fatalf("Failed to verify downloaded file: %v", err)
	}
}

func TestObjectsUploadInParallel(t *testing.T) {
	// 1. Setup: Create or clean bucket
	t.Log("Setting up test environment...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment and create bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Upload files through gateway in parallel
	t.Log("Uploading test files through gateway in parallel...")
	tempFiles, err := uploadTestFiles(t)
	if err != nil {
		t.Fatalf("Failed to upload test files: %v", err)
	}
	// Clean up temp files after test
	defer func() {
		for _, f := range tempFiles {
			cleanupTestFile(t, f)
		}
	}()

	// 3. Verify files in storage systems
	t.Log("Verifying files in storage systems...")

	// Verify all files
	const numFiles = 100
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("myfile%02d.txt", i)
		firstErr, secondErr := verifyObjectsDirectly(t, *localMinioForTesting, testBucket, filename)
		if firstErr != nil {
			t.Errorf("Failed to verify file %s in first storage: %v", filename, firstErr)
		}
		if secondErr != nil {
			t.Errorf("Failed to verify file %s in second storage: %v", filename, secondErr)
		}
	}
}

func TestObjectsDownloadInParallel(t *testing.T) {
	// 1. Setup: Create or clean bucket
	t.Log("Setting up test environment...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment and create bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Upload files through gateway in parallel first
	t.Log("Uploading test files through gateway in parallel...")
	tempFiles, err := uploadTestFiles(t)
	if err != nil {
		t.Fatalf("Failed to upload test files: %v", err)
	}
	// Clean up upload temp files
	defer func() {
		for _, f := range tempFiles {
			cleanupTestFile(t, f)
		}
	}()

	// 3. Verify files exist in storage systems before downloading
	t.Log("Verifying files exist in storage systems...")
	const numFiles = 100
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("myfile%02d.txt", i)
		firstErr, secondErr := verifyObjectsDirectly(t, *localMinioForTesting, testBucket, filename)
		if firstErr != nil {
			t.Fatalf("Failed to verify file %s in first storage: %v", filename, firstErr)
		}
		if secondErr != nil {
			t.Fatalf("Failed to verify file %s in second storage: %v", filename, secondErr)
		}
	}

	// 4. Download files through gateway in parallel
	t.Log("Downloading test files through gateway in parallel...")
	downloadedFiles, err := downloadTestFiles(t)
	if err != nil {
		t.Fatalf("Failed to download test files: %v", err)
	}
	// Clean up downloaded temp files
	defer func() {
		for _, f := range downloadedFiles {
			cleanupTestFile(t, f)
		}
	}()

	// 5. Verify downloaded files
	t.Log("Verifying downloaded files...")
	for _, filename := range downloadedFiles {
		if err := verifyDownloadedFile(t, filename); err != nil {
			t.Errorf("Failed to verify downloaded file %s: %v", filename, err)
		}
	}
}
