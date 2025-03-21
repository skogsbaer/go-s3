package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestPrependReader(t *testing.T) {
	tests := []struct {
		name           string
		prefix         []byte
		content        string
		bufferSize     int
		expectedOutput string
	}{
		{
			name:           "Single read - buffer larger than combined content",
			prefix:         []byte("prefix-"),
			content:        "content",
			bufferSize:     100,
			expectedOutput: "prefix-content",
		},
		{
			name:           "Multiple reads - buffer smaller than prefix",
			prefix:         []byte("long-prefix-"),
			content:        "content",
			bufferSize:     5,
			expectedOutput: "long-prefix-content",
		},
		{
			name:           "Multiple reads - buffer equals prefix length",
			prefix:         []byte("prefix"),
			content:        "content",
			bufferSize:     6,
			expectedOutput: "prefixcontent",
		},
		{
			name:           "Empty prefix",
			prefix:         []byte{},
			content:        "content",
			bufferSize:     100,
			expectedOutput: "content",
		},
		{
			name:           "Empty content",
			prefix:         []byte("prefix-"),
			content:        "",
			bufferSize:     100,
			expectedOutput: "prefix-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a reader with the test content
			contentReader := strings.NewReader(tt.content)

			// Create the PrependReader with the prefix and content reader
			n := 0
			reader := PrependReader{
				prefix: tt.prefix,
				n:      &n,
				r:      contentReader,
			}

			// Read the content in chunks using the specified buffer size
			var result bytes.Buffer
			buf := make([]byte, tt.bufferSize)
			nReads := 0

			for {
				n, err := reader.Read(buf)
				nReads++
				if n > 0 {
					result.Write(buf[:n])
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			expectedReads := Ceil(float64(len(tt.expectedOutput)) / float64(tt.bufferSize))
			if nReads != expectedReads && nReads != expectedReads+1 {
				t.Errorf("expected %d reads (or one more), got %d", expectedReads, nReads)
			}

			// Check if the result matches the expected output
			if result.String() != tt.expectedOutput {
				t.Errorf("expected %q, got %q", tt.expectedOutput, result.String())
			}
		})
	}
}

func TestPrependReaderEdgeCases(t *testing.T) {
	t.Run("read exactly at buffer boundary", func(t *testing.T) {
		prefix := []byte("abc")
		content := "defghijklm"
		n := 0
		reader := PrependReader{
			prefix: prefix,
			n:      &n,
			r:      strings.NewReader(content),
		}

		// First read should get the prefix exactly
		buf1 := make([]byte, 3)
		n1, err1 := reader.Read(buf1)
		if err1 != nil {
			t.Fatalf("unexpected error on first read: %v", err1)
		}
		if n1 != 3 || string(buf1) != "abc" {
			t.Errorf("first read: expected %q, got %q (n=%d)", "abc", string(buf1), n1)
		}

		// Second read should start the content
		buf2 := make([]byte, 4)
		n2, err2 := reader.Read(buf2)
		if err2 != nil {
			t.Fatalf("unexpected error on second read: %v", err2)
		}
		if n2 != 4 || string(buf2[:n2]) != "defg" {
			t.Errorf("second read: expected %q, got %q (n=%d)", "defg", string(buf2[:n2]), n2)
		}

		// Read the rest of the content
		buf3 := make([]byte, 10)
		n3, err3 := reader.Read(buf3)
		if n3 != 6 || string(buf3[:n3]) != "hijklm" {
			t.Errorf("third read: expected %q, got %q (n=%d)", "hijklm", string(buf3[:n3]), n3)
		}
		if err3 != io.EOF && err3 != nil {
			t.Errorf("expected EOF or nil on final read, got: %v", err3)
		}
	})

	t.Run("read with empty buffer", func(t *testing.T) {
		k := 0
		reader := PrependReader{
			prefix: []byte("prefix"),
			n:      &k,
			r:      strings.NewReader("content"),
		}

		buf := make([]byte, 0)
		n, err := reader.Read(buf)
		if n != 0 {
			t.Errorf("expected 0 bytes read, got %d", n)
		}
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}
