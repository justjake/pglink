package backend

import (
	"encoding/binary"
	"io"
	"testing"
)

// makeMessage creates a PostgreSQL wire protocol message for benchmarking.
func makeMessage(msgType byte, body []byte) []byte {
	msg := make([]byte, 5+len(body))
	msg[0] = msgType
	binary.BigEndian.PutUint32(msg[1:5], uint32(4+len(body)))
	copy(msg[5:], body)
	return msg
}

// BenchmarkChanReader_Actual benchmarks the real ChanReader implementation.
func BenchmarkChanReader_Actual(b *testing.B) {
	// Pre-create messages
	msg := makeMessage('D', make([]byte, 100)) // 105 byte messages

	pr, pw := io.Pipe()

	// Create a message reader that parses from the pipe
	msgBuf := make([]byte, 105)
	readMsg := func() (*byte, error) {
		_, err := io.ReadFull(pr, msgBuf)
		if err != nil {
			return nil, err
		}
		msgType := msgBuf[0]
		return &msgType, nil
	}

	// Use the actual ChanReader
	reader := NewChanReader(readMsg)
	outCh := reader.ReadingChan()

	// Writer goroutine
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < b.N; i++ {
			if _, err := pw.Write(msg); err != nil {
				return
			}
		}
		pw.Close()
	}()

	b.ResetTimer()

	// Reader loop - receive and continue for each message
	consumed := 0
	for consumed < b.N {
		result, ok := <-outCh
		if !ok || result.Error != nil {
			break
		}
		consumed++
		reader.Continue() // Signal to read next
	}

	b.StopTimer()

	// Clean shutdown - close pipe first to unblock reader
	pw.Close()
	pr.Close()
	<-writerDone

	b.ReportMetric(float64(b.N), "messages")
}
