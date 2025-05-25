package main

import (
	"fmt"
	"os"
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
	if err := setupTestEnvironment(t); err != nil {
		t.Fatalf("Failed to setup test environment: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// Create bucket through our gateway
	t.Log("Creating test bucket...")
	cmd := exec.Command("mc", "mb", "local-s3/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create bucket through gateway: %v", err)
	}

	// 2. Upload file through gateway
	t.Log("Uploading test file through gateway...")
	if err := uploadTestFile(t); err != nil {
		t.Fatalf("Failed to upload test file: %v", err)
	}

	// 3. Verify files in storage systems
	t.Log("Verifying files in storage systems...")
	if err := verifyFiles(t); err != nil {
		t.Fatalf("Failed to verify files: %v", err)
	}
}

func TestObjectDownload(t *testing.T) {
	// 1. Setup: Create or clean bucket
	t.Log("Setting up test environment...")
	if err := setupTestEnvironment(t); err != nil {
		t.Fatalf("Failed to setup test environment: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// Create bucket through our gateway
	t.Log("Creating test bucket...")
	cmd := exec.Command("mc", "mb", "local-s3/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create bucket through gateway: %v", err)
	}

	// 2. Upload a test file first
	t.Log("Uploading test file through gateway...")
	if err := uploadTestFile(t); err != nil {
		t.Fatalf("Failed to upload test file: %v", err)
	}

	// Verify the file exists in both storage systems before downloading
	t.Log("Verifying file exists in storage systems...")
	if err := verifyFiles(t); err != nil {
		t.Fatalf("Failed to verify uploaded file: %v", err)
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
	if err := setupTestEnvironment(t); err != nil {
		t.Fatalf("Failed to setup test environment: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// Create bucket through our gateway
	t.Log("Creating test bucket...")
	cmd := exec.Command("mc", "mb", "local-s3/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create bucket through gateway: %v", err)
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
	cmd = exec.Command("mc", "ls", "play/"+testBucket)
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

func uploadTestFile(t *testing.T) error {
	// Create a test file
	content := fmt.Sprintf("Test content created at %s", time.Now().Format(time.RFC3339))
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to create test file: %v", err)
	}
	defer cleanupTestFile(t, testFile)

	// Upload through gateway using mc put
	cmd := exec.Command("mc", "put",
		testFile,
		"local-s3/"+testBucket+"/")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to upload file through gateway: %v", err)
	}

	return nil
}

func verifyFiles(t *testing.T) error {
	// Check first storage (MinIO)
	cmd := exec.Command("mc", "ls", "play/"+testBucket+"/"+testFile)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("file not found in first storage: %v", err)
	}

	// Check second storage (Scaleway)
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+testBucket+"/"+testFile)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("file not found in second storage: %v", err)
	}

	return nil
}

func verifyDownloadedFile(t *testing.T, filename string) error {
	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return fmt.Errorf("downloaded file does not exist: %v", err)
	}

	// Read and verify file content
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read downloaded file: %v", err)
	}

	// Basic content verification (file should not be empty)
	if len(content) == 0 {
		return fmt.Errorf("downloaded file is empty")
	}

	return nil
}
