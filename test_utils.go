package main

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

const (
	testBucket = "freiburg-test-bucket"
	testFile   = "myfile.txt"
)

// cleanupTestEnvironment removes test buckets from both storage systems
func cleanupTestEnvironment(t *testing.T) {
	t.Log("Cleaning up test environment...")

	// Clean buckets
	cmd := exec.Command("mc", "rb", "--force", "play/"+testBucket)
	cmd.Run() // Ignore error as bucket might not exist

	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "rb", "--force", "s3://"+testBucket)
	cmd.Run() // Ignore error as bucket might not exist
}

// setupTestEnvironment ensures test buckets are clean before starting
func setupTestEnvironment(t *testing.T) error {
	// Check if bucket exists in first storage (MinIO)
	cmd := exec.Command("mc", "ls", "play/"+testBucket)
	if err := cmd.Run(); err == nil {
		// Bucket exists, remove it
		cmd = exec.Command("mc", "rb", "--force", "play/"+testBucket)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to remove bucket from first storage: %v", err)
		}
	}

	// Check if bucket exists in second storage (Scaleway)
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+testBucket)
	if err := cmd.Run(); err == nil {
		// Bucket exists, remove it
		cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "rb", "--force", "s3://"+testBucket)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to remove bucket from second storage: %v", err)
		}
	}

	return nil
}

// cleanupTestFile removes a test file from the filesystem
func cleanupTestFile(t *testing.T, filename string) {
	if err := os.Remove(filename); err != nil {
		t.Logf("Failed to remove test file: %v", err)
	}
}
