package main

import (
	"encoding/base64"
	"io"
	"math"
)

func Base64Encode(input []byte) string {
	return base64.StdEncoding.EncodeToString(input)
}

func Base64Decode(encoded string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

type UpperCaseReader struct {
	r io.ReadCloser
}

func (u UpperCaseReader) Read(p []byte) (int, error) {
	n, err := u.r.Read(p)
	// Transform the data to uppercase
	for i := 0; i < n; i++ {
		p[i] = p[i] + 1
	}
	return n, err
}

func (u UpperCaseReader) Close() error {
	return u.r.Close()
}

// Ceil returns the smallest integer value greater than or equal to x.
func Ceil(x float64) int {
	return int(math.Ceil(x))
}

// Better use io.MultiReader
type PrependReader struct {
	prefix []byte
	n      *int
	r      io.Reader
}

func (u PrependReader) Read(p []byte) (int, error) {
	// fmt.Printf("u: %v\n", u)
	start := *u.n
	if start < len(u.prefix) {
		n := copy(p, u.prefix[start:])
		*u.n += n
		if *u.n >= len(u.prefix) {
			n2, err := u.r.Read(p[n:])
			return n + n2, err
		} else {
			return n, nil
		}
	} else {
		// fmt.Printf("done with prefix, only reading from buffer")
		// Serve the underlying reader after the prefix
		return u.r.Read(p)
	}
}
