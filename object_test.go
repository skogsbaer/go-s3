package main

import (
	"os/exec"
	"strings"
	"testing"
	"time"
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
	downloadedFile := "downloaded_" + testFile
	defer cleanupTestFile(t, downloadedFile)

	// Wait a moment to ensure file replication is complete
	time.Sleep(2 * time.Second)

	// Try to download the file using mc get
	downloadCmd := exec.Command("mc", "get",
		"local-s3/"+testBucket+"/"+testFile,
		downloadedFile)
	if err := downloadCmd.Run(); err != nil {
		t.Fatalf("Failed to download file through gateway: %v", err)
	}

	// 4. Verify the downloaded file
	t.Log("Verifying downloaded file...")
	if err := verifyDownloadedFile(t, downloadedFile); err != nil {
		t.Fatalf("Failed to verify downloaded file: %v", err)
	}
}

func TestListBucket(t *testing.T) {
	// 1. Setup: Create or clean bucket
	t.Log("Setting up test environment...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment and create bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Upload a test file
	t.Log("Uploading test file through gateway...")
	if err := uploadTestFile(t); err != nil {
		t.Fatalf("Failed to upload test file: %v", err)
	}

	// Wait a moment to ensure file replication is complete
	time.Sleep(2 * time.Second)

	// 3. Verify files in first storage (MinIO)
	t.Log("Verifying files in first storage...")
	cmd := exec.Command("mc", "ls", "play/"+testBucket)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to list files in first storage: %v", err)
	}
	firstStorageFiles := string(output)
	if !strings.Contains(firstStorageFiles, testFile+".cypher.first") ||
		!strings.Contains(firstStorageFiles, testFile+".rand.second") {
		t.Errorf("Expected files not found in first storage. Got:\n%s", firstStorageFiles)
	}

	// 4. Verify files in second storage (Scaleway)
	t.Log("Verifying files in second storage...")
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+testBucket)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to list files in second storage: %v", err)
	}
	secondStorageFiles := string(output)
	if !strings.Contains(secondStorageFiles, testFile+".cypher.second") ||
		!strings.Contains(secondStorageFiles, testFile+".rand.first") {
		t.Errorf("Expected files not found in second storage. Got:\n%s", secondStorageFiles)
	}

	// 5. List files through gateway
	t.Log("Listing files through gateway...")
	cmd = exec.Command("mc", "ls", "local-s3/"+testBucket)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to list files through gateway: %v", err)
	}
	gatewayFiles := string(output)
	if !strings.Contains(gatewayFiles, testFile) {
		t.Errorf("Expected file not found in gateway listing. Got:\n%s", gatewayFiles)
	}
}
