package main

import (
	"errors"
	"io"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	ErrSplitterClosed = errors.New("splitter has been closed")
	ErrInvalidOutput  = errors.New("splitter function returned invalid output")
)

// SplitterFunc defines a function that takes a fixed length byte array as input
// and outputs p byte arrays, where p is the number of output readers.
type SplitterFunc func(data []byte) [][]byte

// Define an enum using iota
type MultiSplitterState int

const (
	Started MultiSplitterState = iota
	Running
	Done
)

type splitterChanItem struct {
	// exactly one of these will be non-nil
	data []byte
	err  error
}

// MultiSplitter creates p readers from a single source reader.
// It reads n bytes at a time from the source, applies the splitter function,
// and distributes the results to the p output readers.
type MultiSplitter struct {
	source      io.Reader // The source reader
	chunkSize   int       // Size n of each chunk to read from source
	splitter    SplitterFunc
	outputChans []chan splitterChanItem // Channels to deliver data to each output reader
	state       MultiSplitterState
	stateMutex  sync.Mutex
}

// NewMultiSplitter creates a new MultiSplitter with the given parameters:
// - source: the reader to read from
// - chunkSize: the size n of each chunk to read from source
// - outputs: the number p of output readers to create
// - splitter: the function to split each chunk into p parts
func NewMultiSplitter(source io.Reader, chunkSize, outputs int, splitter SplitterFunc) (*MultiSplitter, []io.Reader, error) {
	if chunkSize <= 0 {
		return nil, nil, errors.New("chunk size must be positive")
	}
	if outputs <= 0 {
		return nil, nil, errors.New("number of outputs must be positive")
	}
	if source == nil {
		return nil, nil, errors.New("source reader cannot be nil")
	}
	if splitter == nil {
		return nil, nil, errors.New("splitter function cannot be nil")
	}

	ms := &MultiSplitter{
		source:      source,
		chunkSize:   chunkSize,
		splitter:    splitter,
		outputChans: make([]chan splitterChanItem, outputs),
		state:       Started,
	}

	// Create channels for each output
	for i := 0; i < outputs; i++ {
		ms.outputChans[i] = make(chan splitterChanItem, 1) // Buffer size 1 to avoid deadlock
	}

	// Create output readers
	readers := make([]io.Reader, outputs)
	for i := 0; i < outputs; i++ {
		readers[i] = &splitterReader{
			parent: ms,
			index:  i,
		}
	}

	return ms, readers, nil
}

func withMutex[T any](mutex *sync.Mutex, f func() T) T {
	mutex.Lock()
	defer mutex.Unlock()
	return f()
}

// read reads the next chunk from the source and distributes it to the output readers
func (ms *MultiSplitter) read() error {
	// Read a chunk from the source
	buf := make([]byte, ms.chunkSize) // FIXME: avoid allocation on every read
	n, err := io.ReadFull(ms.source, buf)
	logrus.Debugf("MultiSplitter.read: n=%v, err=%v", n, err)

	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}

	// Handle partial reads
	if n < ms.chunkSize {
		if n > 0 {
			buf = buf[:n]
		} else {
			if err == nil {
				err = io.EOF
			}
			ms.close(err)
			return err
		}
	}

	// Apply the splitter function to get p outputs
	outputs := ms.splitter(buf)

	// Validate the outputs
	if len(outputs) != len(ms.outputChans) {
		logrus.Fatalf("Splitter function returned %v outputs, expecting %v",
			len(outputs), len(ms.outputChans))
		err := ErrInvalidOutput // FIXME: better error message
		ms.close(err)
		return err
	}

	// Send data to each output reader
	for i, data := range outputs {
		ms.outputChans[i] <- splitterChanItem{data: data}
	}

	// Handle errors
	if err != nil {
		ms.close(io.EOF)
	}

	return err
}

func (ms *MultiSplitter) readLoop() {
	if withMutex(&ms.stateMutex, func() bool {
		if ms.state == Started {
			ms.state = Running
			return true
		} else {
			return false
		}
	}) {
		logrus.Debugf("starting readLoop of MultiSplitter")
		go func() {
			var err error
			for err == nil {
				err = ms.read()
			}
		}()
	}
}

// close closes all channels and marks the splitter as closed
func (ms *MultiSplitter) close(err error) {
	if withMutex(&ms.stateMutex, func() bool {
		needClose := (ms.state != Done)
		ms.state = Done
		return needClose
	}) {
		logrus.Debugf("closing MultiSplitter")
		// Notify all output readers of the error or EOF
		for i := 0; i < len(ms.outputChans); i++ {
			ms.outputChans[i] <- splitterChanItem{err: err}
			close(ms.outputChans[i])
		}
	}
}

// Close closes the MultiSplitter
func (ms *MultiSplitter) Close() error {
	ms.close(io.EOF)
	return nil
}

// splitterReader implements io.Reader and represents one of the p output readers
type splitterReader struct {
	parent    *MultiSplitter
	index     int
	buffer    []byte
	bufferPos int
}

// Expects that the buffer is not empty
func (sr *splitterReader) readFromBuffer(p []byte) (int, error) {
	n := copy(p, sr.buffer[sr.bufferPos:])
	sr.bufferPos += n
	// If we've fully consumed the buffer, clear it
	if sr.bufferPos >= len(sr.buffer) {
		sr.buffer = nil
		sr.bufferPos = 0
	}
	return n, nil
}

// Read implements the io.Reader interface for a splitter output
func (sr *splitterReader) Read(p []byte) (int, error) {
	logrus.Debugf("splitterReader[%v].Read()", sr.index)
	if sr.index == 0 {
		sr.parent.readLoop()
	}
	// Use any remaining buffered data first
	if sr.bufferPos < len(sr.buffer) {
		return sr.readFromBuffer(p)
	} else {
		// Get the data
		item, ok := <-sr.parent.outputChans[sr.index]
		if !ok {
			return 0, io.EOF
		}
		if item.err != nil {
			return 0, item.err
		} else if len(item.data) == 0 {
			return 0, nil
		} else {
			sr.buffer = item.data
			sr.bufferPos = 0
			return sr.readFromBuffer(p)
		}
	}
}
