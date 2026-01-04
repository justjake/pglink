package pgwire

import (
	"fmt"
	"sync/atomic"
)

// Stats tracks performance statistics for pgwire operations.
// All fields use atomic operations for thread-safe access.
// These stats are only updated when StatsEnabled is true.
var Stats struct {
	Enabled bool // Set to true to enable stats collection

	// Read side: TryNextBatch statistics
	BatchCalls    atomic.Int64    // Number of TryNextBatch calls that returned true
	BatchMsgs     atomic.Int64    // Total messages across all batches
	BatchBytes    atomic.Int64    // Total bytes across all batches
	BatchSizeHist [8]atomic.Int64 // Histogram: [0]=1msg, [1]=2msg, [2]=3-4msg, [3]=5-8msg, [4]=9-16msg, [5]=17-32msg, [6]=33-64msg, [7]=65+msg

	// Write side: writev statistics
	WritevCalls       atomic.Int64 // Number of writeToConn calls
	WritevBufs        atomic.Int64 // Total number of buffers passed to writev
	WritevBytes       atomic.Int64 // Total bytes written via writev
	WritevRingBufs    atomic.Int64 // Number of ring buffer slices
	WritevRingBytes   atomic.Int64 // Bytes from ring buffer
	WritevPrefixBytes atomic.Int64 // Bytes from prefix buffers
	WritevWrapArounds atomic.Int64 // Number of wrap-arounds (2 slices from ring)

	// Streaming (oversized message) statistics
	StreamingReads  atomic.Int64 // Number of streaming message reads
	StreamingBytes  atomic.Int64 // Total bytes read via streaming
	StreamingWrites atomic.Int64 // Number of streaming message writes (fallback path)
}

// batchSizeHistBucket returns the histogram bucket for a given message count.
func batchSizeHistBucket(msgCount int64) int {
	switch {
	case msgCount <= 1:
		return 0
	case msgCount <= 2:
		return 1
	case msgCount <= 4:
		return 2
	case msgCount <= 8:
		return 3
	case msgCount <= 16:
		return 4
	case msgCount <= 32:
		return 5
	case msgCount <= 64:
		return 6
	default:
		return 7
	}
}

// RecordBatch records statistics for a TryNextBatch that returned messages.
func RecordBatch(msgCount, byteCount int64) {
	if !Stats.Enabled {
		return
	}
	Stats.BatchCalls.Add(1)
	Stats.BatchMsgs.Add(msgCount)
	Stats.BatchBytes.Add(byteCount)
	Stats.BatchSizeHist[batchSizeHistBucket(msgCount)].Add(1)
}

// RecordWritev records statistics for a writev call.
func RecordWritev(bufCount int, prefixBytes, ringBytes int64, ringBufCount int, hasWrapAround bool) {
	if !Stats.Enabled {
		return
	}
	Stats.WritevCalls.Add(1)
	Stats.WritevBufs.Add(int64(bufCount))
	Stats.WritevBytes.Add(prefixBytes + ringBytes)
	Stats.WritevPrefixBytes.Add(prefixBytes)
	Stats.WritevRingBufs.Add(int64(ringBufCount))
	Stats.WritevRingBytes.Add(ringBytes)
	if hasWrapAround {
		Stats.WritevWrapArounds.Add(1)
	}
}

// RecordStreamingRead records a streaming (oversized) message read.
func RecordStreamingRead(bytes int64) {
	if !Stats.Enabled {
		return
	}
	Stats.StreamingReads.Add(1)
	Stats.StreamingBytes.Add(bytes)
}

// RecordStreamingWrite records when we fall back to streaming write path.
func RecordStreamingWrite() {
	if !Stats.Enabled {
		return
	}
	Stats.StreamingWrites.Add(1)
}

// StatsSnapshot returns a formatted summary of all statistics.
func StatsSnapshot() string {
	batchCalls := Stats.BatchCalls.Load()
	writevCalls := Stats.WritevCalls.Load()

	if batchCalls == 0 && writevCalls == 0 {
		return "no stats collected"
	}

	var result string

	if batchCalls > 0 {
		avgMsgs := float64(Stats.BatchMsgs.Load()) / float64(batchCalls)
		avgBytes := float64(Stats.BatchBytes.Load()) / float64(batchCalls)
		result += fmt.Sprintf("batches: calls=%d avg_msgs=%.2f avg_bytes=%.1f hist=[",
			batchCalls, avgMsgs, avgBytes)
		for i := 0; i < 8; i++ {
			if i > 0 {
				result += ","
			}
			result += fmt.Sprintf("%d", Stats.BatchSizeHist[i].Load())
		}
		result += "] "
	}

	if writevCalls > 0 {
		avgBufs := float64(Stats.WritevBufs.Load()) / float64(writevCalls)
		avgBytes := float64(Stats.WritevBytes.Load()) / float64(writevCalls)
		avgRingBufs := float64(Stats.WritevRingBufs.Load()) / float64(writevCalls)
		avgRingBytes := float64(Stats.WritevRingBytes.Load()) / float64(writevCalls)
		avgPrefixBytes := float64(Stats.WritevPrefixBytes.Load()) / float64(writevCalls)
		wrapArounds := Stats.WritevWrapArounds.Load()
		result += fmt.Sprintf("writev: calls=%d avg_bufs=%.2f avg_bytes=%.1f avg_ring_bufs=%.2f avg_ring_bytes=%.1f avg_prefix_bytes=%.1f wrap_arounds=%d ",
			writevCalls, avgBufs, avgBytes, avgRingBufs, avgRingBytes, avgPrefixBytes, wrapArounds)
	}

	streamingReads := Stats.StreamingReads.Load()
	streamingWrites := Stats.StreamingWrites.Load()
	if streamingReads > 0 || streamingWrites > 0 {
		result += fmt.Sprintf("streaming: reads=%d read_bytes=%d writes=%d",
			streamingReads, Stats.StreamingBytes.Load(), streamingWrites)
	}

	return result
}
