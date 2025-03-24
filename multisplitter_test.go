package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// behaves like slicing in python
func safeSlice[T any](s []T, start, end int) []T {
	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start >= len(s) || end <= 0 || start >= end {
		return nil
	}
	return s[start:end]
}

func RunTest(t *testing.T, s string, chunkSize int, outputs int) {
	// Test data
	inputData := []byte(s)
	source := bytes.NewReader(inputData)

	// Define a splitter function that splits each chunk into outputs parts
	splitter := func(chunk []byte) [][]byte {
		partSize := chunkSize / outputs
		res := make([][]byte, outputs)
		for i := 0; i < outputs; i++ {
			res[i] = safeSlice(chunk, i*partSize, (i+1)*partSize)
		}
		return res
	}

	// Create a MultiSplitter
	ms, readers, err := NewMultiSplitter(source, chunkSize, outputs, splitter)
	if err != nil {
		t.Fatalf("Failed to create MultiSplitter: %v", err)
	}

	// Expected results for each output reader
	expected := make([]string, outputs)
	for {
		chunk := safeSlice(inputData, 0, chunkSize)
		if len(chunk) == 0 {
			break
		}
		inputData = safeSlice(inputData, chunkSize, len(inputData))
		splitted := splitter(chunk)
		for i := 0; i < outputs; i++ {
			expected[i] += string(splitted[i])
		}
	}
	logrus.Debugf("expected: %v", expected)

	// Read from all output readers concurrently
	var wg sync.WaitGroup
	results := make([]string, outputs)

	for i := 0; i < outputs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var buf bytes.Buffer
			_, err := io.Copy(&buf, readers[i])
			n := time.Duration(rand.IntN(20)) * time.Microsecond
			time.Sleep(n) // sleep for a random time to vary to order of reads
			if err != nil && err != io.EOF {
				t.Errorf("Reader %d error: %v", i, err)
			}

			results[i] = buf.String()
		}()
	}

	// Wait for all readers to finish
	wg.Wait()

	// Close the MultiSplitter
	ms.Close()

	// Check results
	for i := 0; i < outputs; i++ {
		if results[i] != expected[i] {
			t.Errorf("Reader %d got %q, want %q", i, results[i], expected[i])
		}
	}
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.IntN(len(charset))]
	}
	return string(result)
}

func TestMultiSplitter(t *testing.T) {
	input := randomString(97)
	//RunTest(t, input, 5, 4)
	for chunkSize := 1; chunkSize < 10; chunkSize++ {
		for outputs := 1; outputs < 10; outputs++ {
			name := fmt.Sprintf("chunkSize=%d, outputs=%d", chunkSize, outputs)
			res := t.Run(name, func(t *testing.T) {
				RunTest(t, input, chunkSize, outputs)
			})
			if !res {
				return
			}
		}
	}

}
