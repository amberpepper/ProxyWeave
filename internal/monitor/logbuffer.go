package monitor

import (
	"io"
	"sync"
)

// LogBuffer is a thread-safe ring buffer that captures recent log output.
// It implements io.Writer so it can be plugged into log.SetOutput via io.MultiWriter.
type LogBuffer struct {
	mu       sync.Mutex
	buf      []byte
	size     int
	seq      uint64
	watchers map[chan uint64]struct{}
}

// NewLogBuffer creates a ring buffer that keeps the last `size` bytes of log output.
func NewLogBuffer(size int) *LogBuffer {
	return &LogBuffer{
		buf:      make([]byte, 0, size),
		size:     size,
		watchers: make(map[chan uint64]struct{}),
	}
}

// Write implements io.Writer. Appends data and trims the front if over capacity.
func (lb *LogBuffer) Write(p []byte) (n int, err error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.buf = append(lb.buf, p...)
	if len(lb.buf) > lb.size {
		lb.buf = lb.buf[len(lb.buf)-lb.size:]
	}
	lb.seq++
	for ch := range lb.watchers {
		select {
		case ch <- lb.seq:
		default:
		}
	}
	return len(p), nil
}

// Content returns a copy of the current buffer contents.
func (lb *LogBuffer) Content() string {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return string(lb.buf)
}

// Snapshot returns a copy of current content and sequence number.
func (lb *LogBuffer) Snapshot() (content string, seq uint64) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return string(lb.buf), lb.seq
}

// Subscribe returns a watcher channel that receives sequence updates when log content changes.
// The returned cancel function must be called to release resources.
func (lb *LogBuffer) Subscribe() (updates <-chan uint64, currentSeq uint64, cancel func()) {
	ch := make(chan uint64, 1)
	lb.mu.Lock()
	lb.watchers[ch] = struct{}{}
	currentSeq = lb.seq
	lb.mu.Unlock()

	cancel = func() {
		lb.mu.Lock()
		if _, ok := lb.watchers[ch]; ok {
			delete(lb.watchers, ch)
			close(ch)
		}
		lb.mu.Unlock()
	}
	return ch, currentSeq, cancel
}

// SharedLogBuffer is the global log buffer accessible by the server.
var SharedLogBuffer *LogBuffer

func init() {
	SharedLogBuffer = NewLogBuffer(64 * 1024) // 64KB ring buffer
}

// LogWriter returns an io.Writer that can be used with io.MultiWriter to capture logs.
func LogWriter() io.Writer {
	return SharedLogBuffer
}
