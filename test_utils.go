package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

// Test flags
var localMinioForTesting = flag.Bool("test-local-minio", false, "Use local MinIO for testing")
var noCleanup = flag.Bool("test-no-cleanup", false, "Skip cleanup after tests")

const (
	testBucket = "freiburg-test-bucket"
	testFile   = "myfile.txt"
)

// StorageType represents which storage system to verify
type StorageType int

const (
	FirstStorage StorageType = iota
	SecondStorage
)

// verifyBucketExistsDirectly verifies that a bucket exists in both storage systems
// local: true for local MinIO, false for cloud storage
// bucketName: name of the bucket to verify
// Returns both errors if any occur
func verifyBucketExistsDirectly(t *testing.T, local bool, bucketName string) (error, error) {
	var firstErr, secondErr error

	// Check first storage
	var firstCmd *exec.Cmd
	if local {
		firstCmd = exec.Command("mc", "--insecure", "ls", "firstminio/"+bucketName)
	} else {
		firstCmd = exec.Command("mc", "ls", "play/"+bucketName)
	}
	output, err := firstCmd.CombinedOutput()
	if err != nil {
		firstErr = fmt.Errorf("bucket not found in first storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	// Check second storage
	var secondCmd *exec.Cmd
	if local {
		secondCmd = exec.Command("mc", "--insecure", "ls", "secondminio/"+bucketName)
	} else {
		secondCmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
	}
	output, err = secondCmd.CombinedOutput()
	if err != nil {
		secondErr = fmt.Errorf("bucket not found in second storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	return firstErr, secondErr
}

// verifyBucketDoesNotExistDirectly verifies that a bucket does not exist in both storage systems
// local: true for local MinIO, false for cloud storage
// bucketName: name of the bucket to verify
// Returns both errors if any occur
func verifyBucketDoesNotExistDirectly(t *testing.T, local bool, bucketName string) (error, error) {
	var firstErr, secondErr error

	// Check first storage
	var firstCmd *exec.Cmd
	if local {
		firstCmd = exec.Command("mc", "--insecure", "ls", "firstminio/"+bucketName)
	} else {
		firstCmd = exec.Command("mc", "ls", "play/"+bucketName)
	}
	output, err := firstCmd.CombinedOutput()
	if err == nil {
		firstErr = fmt.Errorf("bucket still exists in first storage\nDebug output: %s",
			string(output))
	}

	// Check second storage
	var secondCmd *exec.Cmd
	if local {
		secondCmd = exec.Command("mc", "--insecure", "ls", "secondminio/"+bucketName)
	} else {
		secondCmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
	}
	output, err = secondCmd.CombinedOutput()
	if err == nil {
		secondErr = fmt.Errorf("bucket still exists in second storage\nDebug output: %s",
			string(output))
	}

	return firstErr, secondErr
}

// verifyBucketIsEmptyDirectly verifies that a bucket exists and is empty in both storage systems
// local: true for local MinIO, false for cloud storage
// bucketName: name of the bucket to verify
// Returns both errors if any occur
func verifyBucketIsEmptyDirectly(t *testing.T, local bool, bucketName string) (error, error) {
	var firstErr, secondErr error

	// Check first storage
	var firstCmd *exec.Cmd
	if local {
		firstCmd = exec.Command("mc", "--insecure", "ls", "firstminio/"+bucketName)
	} else {
		firstCmd = exec.Command("mc", "ls", "play/"+bucketName)
	}
	output, err := firstCmd.CombinedOutput()
	if err != nil {
		firstErr = fmt.Errorf("bucket not found in first storage: %v\nDebug output: %s",
			err,
			string(output))
	} else if string(output) != "" {
		firstErr = fmt.Errorf("bucket is not empty in first storage\nDebug output: %s",
			string(output))
	}

	// Check second storage
	var secondCmd *exec.Cmd
	if local {
		secondCmd = exec.Command("mc", "--insecure", "ls", "secondminio/"+bucketName)
	} else {
		secondCmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
	}
	output, err = secondCmd.CombinedOutput()
	if err != nil {
		secondErr = fmt.Errorf("bucket not found in second storage: %v\nDebug output: %s",
			err,
			string(output))
	} else if string(output) != "" {
		secondErr = fmt.Errorf("bucket is not empty in second storage\nDebug output: %s",
			string(output))
	}

	return firstErr, secondErr
}

// cleanupTestEnvironment removes test buckets from both storage systems
func cleanupTestEnvironment(t *testing.T) {
	t.Log("Cleaning up test environment...")

	if *localMinioForTesting {
		// Clean local MinIO buckets
		cmd := exec.Command("mc", "rb", "--insecure", "--force", "firstminio/"+testBucket)
		cmd.Run() // Ignore error as bucket might not exist

		cmd = exec.Command("mc", "rb", "--insecure", "--force", "secondminio/"+testBucket)
		cmd.Run() // Ignore error as bucket might not exist
	} else {
		// Clean cloud buckets
		cmd := exec.Command("mc", "rb", "--force", "play/"+testBucket)
		cmd.Run() // Ignore error as bucket might not exist

		cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "rb", "--force", "s3://"+testBucket)
		cmd.Run() // Ignore error as bucket might not exist
	}
}

// setupTestEnvironment ensures test buckets are clean before starting
func setupTestEnvironment(t *testing.T) error {

	if *localMinioForTesting {
		t.Log("Using local MinIO storage for testing")

		// Check if bucket exists in first storage (MinIO)
		cmd := exec.Command("mc", "--insecure", "ls", "firstminio/"+testBucket)
		if err := cmd.Run(); err == nil {
			// Bucket exists, remove it
			cmd = exec.Command("mc", "--insecure", "rb", "--force", "firstminio/"+testBucket)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to remove bucket from first storage: %v", err)
			}
		}

		// Check if bucket exists in second storage (MinIO)
		cmd = exec.Command("mc", "--insecure", "ls", "secondminio/"+testBucket)
		if err := cmd.Run(); err == nil {
			// Bucket exists, remove it
			cmd = exec.Command("mc", "--insecure", "rb", "--force", "secondminio/"+testBucket)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to remove bucket from second storage: %v", err)
			}
		}

	} else {
		t.Log("Using cloud storage for testing")

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
	}

	return nil
}

// createTempFile creates a file in the system's temp directory
func createTempFile(t *testing.T, content string) (string, error) {
	// Create a temporary file
	tempFile, err := os.CreateTemp("", "s3-test-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %v", err)
	}

	// Write content to the file
	if _, err := tempFile.Write([]byte(content)); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to write to temp file: %v", err)
	}

	// Close the file
	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to close temp file: %v", err)
	}

	return tempFile.Name(), nil
}

// cleanupTestFile removes a test file
func cleanupTestFile(t *testing.T, filename string) {
	if err := os.Remove(filename); err != nil {
		t.Logf("Failed to remove test file: %v", err)
	}
}

// createTestFile creates a test file with the given name and content
func createTestFile(t *testing.T, filename string, content string) error {
	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to create test file: %v", err)
	}
	return nil
}

// createBucketDirectly creates a bucket in both storage systems
// local: true for local MinIO, false for cloud storage
// bucketName: name of the bucket to create
// Returns both errors if any occur
func createBucketDirectly(t *testing.T, local bool, bucketName string) (error, error) {
	var firstErr, secondErr error

	// Create in first storage
	var firstCmd *exec.Cmd
	if local {
		firstCmd = exec.Command("mc", "--insecure", "mb", "firstminio/"+bucketName)
	} else {
		firstCmd = exec.Command("mc", "mb", "play/"+bucketName)
	}
	output, err := firstCmd.CombinedOutput()
	if err != nil {
		firstErr = fmt.Errorf("failed to create bucket in first storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	// Create in second storage
	var secondCmd *exec.Cmd
	if local {
		secondCmd = exec.Command("mc", "--insecure", "mb", "secondminio/"+bucketName)
	} else {
		secondCmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "mb", "s3://"+bucketName)
	}
	output, err = secondCmd.CombinedOutput()
	if err != nil {
		secondErr = fmt.Errorf("failed to create bucket in second storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	return firstErr, secondErr
}

// uploadObjectDirectly uploads an object to both storage systems
// local: true for local MinIO, false for cloud storage
// bucketName: name of the bucket to upload to
// objectName: name of the object to upload
// objectPath: path to the file to upload
// Returns both errors if any occur
func uploadObjectDirectly(t *testing.T, local bool, bucketName string, objectName string, objectPath string) (error, error) {
	var firstErr, secondErr error

	// Upload to first storage with cypher suffix
	var firstCmd *exec.Cmd
	if local {
		firstCmd = exec.Command("mc", "--insecure", "cp", objectPath, "firstminio/"+bucketName+"/"+objectName+".cypher.first")
	} else {
		firstCmd = exec.Command("mc", "cp", objectPath, "play/"+bucketName+"/"+objectName+".cypher.first")
	}
	output, err := firstCmd.CombinedOutput()
	if err != nil {
		firstErr = fmt.Errorf("failed to upload object to first storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	// Upload to first storage with rand suffix
	if local {
		firstCmd = exec.Command("mc", "--insecure", "cp", objectPath, "firstminio/"+bucketName+"/"+objectName+".rand.second")
	} else {
		firstCmd = exec.Command("mc", "cp", objectPath, "play/"+bucketName+"/"+objectName+".rand.second")
	}
	output, err = firstCmd.CombinedOutput()
	if err != nil {
		firstErr = fmt.Errorf("failed to upload object to first storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	// Upload to second storage with cypher suffix
	var secondCmd *exec.Cmd
	if local {
		secondCmd = exec.Command("mc", "--insecure", "cp", objectPath, "secondminio/"+bucketName+"/"+objectName+".cypher.second")
	} else {
		secondCmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "cp", objectPath, "s3://"+bucketName+"/"+objectName+".cypher.second")
	}
	output, err = secondCmd.CombinedOutput()
	if err != nil {
		secondErr = fmt.Errorf("failed to upload object to second storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	// Upload to second storage with rand suffix
	if local {
		secondCmd = exec.Command("mc", "--insecure", "cp", objectPath, "secondminio/"+bucketName+"/"+objectName+".rand.first")
	} else {
		secondCmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "cp", objectPath, "s3://"+bucketName+"/"+objectName+".rand.first")
	}
	output, err = secondCmd.CombinedOutput()
	if err != nil {
		secondErr = fmt.Errorf("failed to upload object to second storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	return firstErr, secondErr
}

// setupTestEnvironmentWithBucket ensures test buckets are clean and creates a new bucket
// bucketName: name of the bucket to create
// Returns error if setup or bucket creation fails
func setupTestEnvironmentWithBucket(t *testing.T, bucketName string) error {
	// First, clean up any existing test environment
	if err := setupTestEnvironment(t); err != nil {
		return fmt.Errorf("failed to setup test environment: %v", err)
	}

	// Create the bucket in both storage systems
	firstErr, secondErr := createBucketDirectly(t, *localMinioForTesting, bucketName)
	if firstErr != nil {
		return fmt.Errorf("failed to create bucket in first storage: %v", firstErr)
	}
	if secondErr != nil {
		return fmt.Errorf("failed to create bucket in second storage: %v", secondErr)
	}

	// Verify the bucket exists in both storage systems
	firstErr, secondErr = verifyBucketExistsDirectly(t, *localMinioForTesting, bucketName)
	if firstErr != nil {
		return fmt.Errorf("failed to verify bucket in first storage: %v", firstErr)
	}
	if secondErr != nil {
		return fmt.Errorf("failed to verify bucket in second storage: %v", secondErr)
	}

	return nil
}

func uploadTestFile(t *testing.T) error {
	// Create a test file in temp directory
	content := fmt.Sprintf("Test content created at %s", time.Now().Format(time.RFC3339))
	tempFile, err := createTempFile(t, content)
	if err != nil {
		return fmt.Errorf("failed to create test file: %v", err)
	}
	defer cleanupTestFile(t, tempFile)

	// Upload through gateway using mc put
	cmd := exec.Command("mc", "put",
		tempFile,
		"local-s3/"+testBucket+"/"+testFile)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to upload file through gateway: %v", err)
	}

	return nil
}

func uploadTestFiles(t *testing.T) ([]string, error) {
	// Create test files
	content := fmt.Sprintf("Test content created at %s", time.Now().Format(time.RFC3339))

	// Create and upload 100 files in parallel
	const numFiles = 100
	errChan := make(chan error, numFiles)
	tempFiles := make([]string, numFiles)

	// Create and upload files in parallel
	for i := 0; i < numFiles; i++ {
		// Create temp file
		tempFile, err := createTempFile(t, content)
		if err != nil {
			// Clean up any already created temp files
			for j := 0; j < i; j++ {
				cleanupTestFile(t, tempFiles[j])
			}
			return nil, fmt.Errorf("failed to create test file %d: %v", i, err)
		}
		tempFiles[i] = tempFile

		// Upload file in a goroutine
		go func(fname string, index int) {
			cmd := exec.Command("mc", "put",
				fname,
				"local-s3/"+testBucket+"/"+fmt.Sprintf("myfile%02d.txt", index))
			if err := cmd.Run(); err != nil {
				errChan <- fmt.Errorf("failed to upload file %d through gateway: %v", index, err)
				return
			}
			errChan <- nil
		}(tempFile, i)
	}

	// Wait for all uploads to complete and check for errors
	for i := 0; i < numFiles; i++ {
		if err := <-errChan; err != nil {
			// Clean up all temp files on error
			for _, f := range tempFiles {
				cleanupTestFile(t, f)
			}
			return nil, err
		}
	}

	return tempFiles, nil
}

func downloadTestFile(t *testing.T, sourceFile string) (string, error) {
	// Create a temporary file for download
	tempFile, err := os.CreateTemp("", "s3-download-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file for download: %v", err)
	}
	tempFile.Close() // Close it so mc can write to it

	// Download the file using mc get
	cmd := exec.Command("mc", "get",
		"local-s3/"+testBucket+"/"+sourceFile,
		tempFile.Name())
	if err := cmd.Run(); err != nil {
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to download file through gateway: %v", err)
	}

	return tempFile.Name(), nil
}

func downloadTestFiles(t *testing.T) ([]string, error) {
	// Download 100 files in parallel
	const numFiles = 100
	errChan := make(chan error, numFiles)
	downloadedFiles := make([]string, numFiles)

	// Download files in parallel
	for i := 0; i < numFiles; i++ {
		sourceFile := fmt.Sprintf("myfile%02d.txt", i)

		// Create a temporary file for download
		tempFile, err := os.CreateTemp("", "s3-download-*")
		if err != nil {
			// Clean up any already created temp files
			for j := 0; j < i; j++ {
				cleanupTestFile(t, downloadedFiles[j])
			}
			return nil, fmt.Errorf("failed to create temp file for download %d: %v", i, err)
		}
		tempFile.Close() // Close it so mc can write to it
		downloadedFiles[i] = tempFile.Name()

		// Download file in a goroutine
		go func(src, dst string, index int) {
			cmd := exec.Command("mc", "get",
				"local-s3/"+testBucket+"/"+src,
				dst)
			if err := cmd.Run(); err != nil {
				errChan <- fmt.Errorf("failed to download file %d through gateway: %v", index, err)
				return
			}
			errChan <- nil
		}(sourceFile, tempFile.Name(), i)
	}

	// Wait for all downloads to complete and check for errors
	for i := 0; i < numFiles; i++ {
		if err := <-errChan; err != nil {
			// Clean up all temp files on error
			for _, f := range downloadedFiles {
				cleanupTestFile(t, f)
			}
			return nil, err
		}
	}

	return downloadedFiles, nil
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

func verifyObjectsDirectly(t *testing.T, local bool, bucketName string, objectName string) (error, error) {
	var firstErr, secondErr error

	// Check first storage
	var firstCmd *exec.Cmd
	if local {
		firstCmd = exec.Command("mc", "--insecure", "ls", "firstminio/"+bucketName+"/"+objectName)
	} else {
		firstCmd = exec.Command("mc", "ls", "play/"+bucketName+"/"+objectName)
	}
	output, err := firstCmd.CombinedOutput()
	if err != nil {
		firstErr = fmt.Errorf("file not found in first storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	// Check second storage
	var secondCmd *exec.Cmd
	if local {
		secondCmd = exec.Command("mc", "--insecure", "ls", "secondminio/"+bucketName+"/"+objectName)
	} else {
		secondCmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName+"/"+objectName)
	}
	output, err = secondCmd.CombinedOutput()
	if err != nil {
		secondErr = fmt.Errorf("file not found in second storage: %v\nDebug output: %s",
			err,
			string(output))
	}

	return firstErr, secondErr
}
