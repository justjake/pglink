# Ring Buffer Architecture for pglink

## Overview

This document describes a zero-copy ring buffer architecture for pglink that eliminates per-message synchronization overhead while maintaining correctness for connection pooling semantics.

## Problem Statement

### Current Architecture

pglink currently uses `ChanReader` which requires a `Continue()` signal after each message:

```
Reader goroutine:  Receive() → send to channel → wait Continue() → loop
Main goroutine:    recv from channel → process → Continue() → loop
```

This creates **two channel operations per message**, which dominates CPU time for high-throughput workloads (trace analysis showed 46% of scheduler latency in `Continue()`).

### pgproto3 Constraint

The underlying `pgproto3` library uses zero-copy parsing:
- Messages are decoded into flyweight structs (one instance per type)
- Message fields (like `DataRow.Values`) are slices pointing into an 8KB read buffer
- Data is only valid until the next `Receive()` call

This constraint prevents naive buffering - we cannot read ahead without corrupting in-flight messages.

## Proposed Architecture

### Ring Buffer with Atomic Coordination

Replace per-message synchronization with a lock-free ring buffer:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Ring Buffer (256KB)                             │
│                                                                          │
│  [msg1][msg2][msg3][msg4][    free space    ][old consumed data]        │
│                    ↑     ↑                                               │
│                readPos  writePos                                         │
│                (atomic) (atomic)                                         │
└─────────────────────────────────────────────────────────────────────────┘

Background Thread:                    Main Thread (Session):
  read from network                     snapshot writePos
  write to ring buffer                  process ALL messages in [readPos, writePos)
  advance writePos atomically           batch write to output
  signal if reader waiting              advance readPos atomically
                                        signal if writer waiting
```

### Key Design Principles

1. **Zero extra copies**: Main thread reads directly from ring buffer
2. **Batch processing**: Handle all available messages per loop iteration
3. **Batch I/O**: Single syscall to write multiple messages
4. **Lock-free fast path**: Atomics for coordination, locks only when blocking
5. **Flow control**: Backpressure through buffer exhaustion and explicit limits

## Detailed Design

### Data Structures

We use a **struct-of-arrays** layout for message metadata, which should improve cache
efficiency when scanning message types. This design targets scenarios like forwarding
thousands of DataRow messages, though actual performance gains are unverified.

```go
// RingBuffer provides a lock-free single-producer single-consumer ring buffer
// with pre-parsed message metadata for efficient batch operations.
//
// The buffer uses struct-of-arrays layout for metadata:
// - types[]   : Message type bytes, 64 per cache line
// - offsets[] : Message start offsets, 8 per cache line (int64)
//
// This allows scanning 1000 message types with ~16 cache line loads,
// vs ~125 loads with array-of-structs layout.
type RingBuffer struct {
    // Raw data buffer (power of 2 size)
    data     []byte
    dataMask int64  // len(data) - 1, for bitwise AND

    // Message metadata - struct of arrays for cache efficiency
    // Background thread parses headers and populates these
    types    []byte   // Message type byte for each message
    offsets  []int64  // Start offset in data buffer for each message
    metaMask int64    // len(types) - 1, for bitwise AND

    // Data buffer positions (cache-line padded)
    _            [64]byte
    dataWritePos int64    // Atomic: next write position in data buffer
    _            [64 - 8]byte
    dataReadPos  int64    // Atomic: consumed position in data buffer
    _            [64 - 8]byte

    // Message count positions (cache-line padded)
    msgWriteCount int64   // Atomic: number of complete messages parsed
    _             [64 - 8]byte
    msgReadCount  int64   // Atomic: number of messages consumed
    _             [64 - 8]byte

    // Signaling for blocking cases
    dataReady  chan struct{} // Buffered(1): writer signals reader
    spaceReady chan struct{} // Buffered(1): reader signals writer

    // Error propagation
    err atomic.Pointer[error]
}

// Cache efficiency reasoning (theoretical, not benchmarked):
//
// Array-of-structs: struct { Type byte; Offset int64 } = 16 bytes (with padding)
//   - 4 messages per 64-byte cache line
//   - Scanning 1000 types would load ~250 cache lines
//
// Struct-of-arrays: separate types[]byte and offsets[]int64
//   - types[]: 64 messages per cache line
//   - offsets[]: 8 offsets per cache line
//   - Scanning 1000 types would load ~16 cache lines
//   - For uniform runs, only need first/last offset
//
// Whether this matters in practice depends on actual access patterns
// and how much time is spent in type scanning vs other work.
```

### Deadlock Analysis

The ring buffer has potential deadlock conditions that must be prevented:

**Deadlock #1: Partial message fills buffer**

```
Scenario:
  - Data buffer nearly full
  - Writer receives partial message that won't fit
  - Writer blocks waiting for space (spaceReady)
  - Reader has no complete messages to consume
  - Reader blocks waiting for messages (dataReady)
  - Neither can make progress → DEADLOCK

Prevention:
  - Reserve headroom (minDataHeadroom) in data buffer
  - Never fill buffer beyond (size - headroom)
  - Headroom must be >= max expected message size
  - If message exceeds headroom, return error rather than deadlock
```

**Deadlock #2: Circular wait on signals**

```
Scenario:
  - Writer sends signal to dataReady (channel has 1 item)
  - Writer parses another message, tries to signal (dropped - channel full)
  - Reader wakes, processes both messages
  - Reader sees no more messages, waits on dataReady
  - dataReady is empty (second signal was dropped)
  - Writer is blocked on network I/O
  - Reader waits indefinitely → LATENCY BUG (not true deadlock)

Prevention:
  - Reader always checks for new messages BEFORE waiting
  - The check-then-wait pattern:
      msgLimit := atomic.Load(&msgWriteCount)
      if msgLimit > processed { process() }
      else { wait on dataReady }
  - Even if signal was lost, next network read will signal again
```

**Deadlock #3: Metadata full, data has space**

```
Scenario:
  - Metadata arrays (types[], offsets[]) are full
  - Data buffer has space
  - Writer can't record new messages
  - Writer blocks on spaceReady

Analysis:
  - If metadata is full, we have metaSize complete messages
  - Reader CAN process those messages
  - Reader will advance msgReadCount, signal spaceReady
  - NOT a deadlock - reader can always make progress
```

### Physical vs Logical Positions

Positions are **logical** (monotonically increasing) rather than physical:

```go
// Convert logical position to physical buffer index
func (r *RingBuffer) phys(logical int64) int64 {
    return logical & r.mask  // Equivalent to logical % size for power of 2
}

// Available data to read
func (r *RingBuffer) Available() int64 {
    return atomic.LoadInt64(&r.writePos) - atomic.LoadInt64(&r.readPos)
}

// Available space to write
func (r *RingBuffer) Space() int64 {
    return int64(len(r.buf)) - r.Available()
}
```

### Writer Goroutine (Background)

The writer goroutine reads from the network and parses message boundaries,
populating both the data buffer and the metadata arrays.

**Deadlock Prevention:**
- Reserve headroom in data buffer to prevent partial-message deadlock
- Track parse position separately from write position
- Always signal after advancing msgWriteCount

```go
// Headroom prevents deadlock when buffer fills with partial message.
const minDataHeadroom = 8192

func (r *RingBuffer) WriterLoop(conn io.Reader) error {
    var parsePos int64  // Where we've parsed up to (writer-private)

    for {
        // Try to parse complete messages from buffered data
        if r.parseMessages(&parsePos) {
            continue  // Parsed something, try for more
        }

        // No complete messages. Need more data from network.
        if err := r.readMore(conn); err != nil {
            return err
        }
    }
}

// parseMessages parses complete messages from [parsePos, dataWritePos).
// Returns true if any messages were parsed.
func (r *RingBuffer) parseMessages(parsePos *int64) bool {
    dataWrite := atomic.LoadInt64(&r.dataWritePos)
    msgWrite := atomic.LoadInt64(&r.msgWriteCount)
    msgRead := atomic.LoadInt64(&r.msgReadCount)
    metaSpace := int64(len(r.types)) - (msgWrite - msgRead)

    parsedAny := false

    for {
        unparsed := dataWrite - *parsePos
        if unparsed < 5 || metaSpace == 0 {
            break  // Need more data or metadata full
        }

        // Read message header: [type:1][length:4]
        physPos := *parsePos & r.dataMask
        msgType := r.data[physPos]
        msgLen := int64(r.readUint32(physPos+1)) + 1  // +1 for type byte

        // Reject oversized messages
        if msgLen > int64(len(r.data)) - minDataHeadroom {
            r.setError(ErrMessageTooLarge{msgLen})
            return false
        }

        if unparsed < msgLen {
            break  // Incomplete message
        }

        // Record in metadata
        idx := msgWrite & r.metaMask
        r.types[idx] = msgType
        r.offsets[idx] = *parsePos

        *parsePos += msgLen
        msgWrite++
        metaSpace--
        parsedAny = true

        // Publish to reader
        atomic.StoreInt64(&r.msgWriteCount, msgWrite)
        r.signal(r.dataReady)
    }

    return parsedAny
}

// readMore reads from conn into the data buffer, blocking if necessary.
func (r *RingBuffer) readMore(conn io.Reader) error {
    dataWrite := atomic.LoadInt64(&r.dataWritePos)
    dataRead := atomic.LoadInt64(&r.dataReadPos)
    dataSpace := int64(len(r.data)) - (dataWrite - dataRead) - minDataHeadroom

    msgWrite := atomic.LoadInt64(&r.msgWriteCount)
    msgRead := atomic.LoadInt64(&r.msgReadCount)
    metaSpace := int64(len(r.types)) - (msgWrite - msgRead)

    // Wait if no space available
    for dataSpace <= 0 || metaSpace == 0 {
        select {
        case <-r.spaceReady:
        case <-r.ctx.Done():
            return r.ctx.Err()
        }
        dataRead = atomic.LoadInt64(&r.dataReadPos)
        dataSpace = int64(len(r.data)) - (dataWrite - dataRead) - minDataHeadroom
        msgRead = atomic.LoadInt64(&r.msgReadCount)
        metaSpace = int64(len(r.types)) - (msgWrite - msgRead)
    }

    // Read into contiguous region (don't wrap in single read)
    physWrite := dataWrite & r.dataMask
    maxRead := min(dataSpace, int64(len(r.data)) - physWrite)

    n, err := conn.Read(r.data[physWrite : physWrite+maxRead])
    if n > 0 {
        atomic.AddInt64(&r.dataWritePos, int64(n))
    }
    if err != nil {
        r.setError(err)
        return err
    }
    return nil
}

// readUint32 reads big-endian uint32, handling buffer wraparound.
func (r *RingBuffer) readUint32(physPos int64) uint32 {
    if physPos + 4 <= int64(len(r.data)) {
        return binary.BigEndian.Uint32(r.data[physPos:])
    }
    var buf [4]byte
    for i := range 4 {
        buf[i] = r.data[(physPos+int64(i)) & r.dataMask]
    }
    return binary.BigEndian.Uint32(buf[:])
}

func (r *RingBuffer) signal(ch chan struct{}) {
    select {
    case ch <- struct{}{}:
    default:
    }
}

func (r *RingBuffer) setError(err error) {
    r.err.Store(&err)
    close(r.dataReady)
}

type ErrMessageTooLarge struct{ Size int64 }

func (e ErrMessageTooLarge) Error() string {
    return fmt.Sprintf("message size %d exceeds buffer capacity", e.Size)
}
```

### Message Parsing from Ring Buffer

```go
// TryReadMessageHeader attempts to read a message header at the given position.
// Returns (type, length, true) if complete header available, (0, 0, false) otherwise.
func (r *RingBuffer) TryReadMessageHeader(pos, limit int64) (byte, uint32, bool) {
    if limit - pos < 5 {
        return 0, 0, false
    }

    physPos := r.phys(pos)

    // Handle header spanning buffer boundary (rare)
    if physPos + 5 > int64(len(r.buf)) {
        return r.readWrappedHeader(pos)
    }

    msgType := r.buf[physPos]
    length := binary.BigEndian.Uint32(r.buf[physPos+1 : physPos+5])
    return msgType, length, true
}

// MessageBody returns a slice of the message body.
// For wrapped messages, copies to a temporary buffer.
func (r *RingBuffer) MessageBody(start, end int64) []byte {
    physStart := r.phys(start + 5)  // Skip header
    physEnd := r.phys(end)
    bodyLen := end - start - 5

    if physEnd > physStart {
        // Contiguous
        return r.buf[physStart:physEnd]
    }

    // Wrapped - need to copy (rare case at buffer boundary)
    body := make([]byte, bodyLen)
    n := copy(body, r.buf[physStart:])
    copy(body[n:], r.buf[:physEnd])
    return body
}
```

## Session Event Loop

### Main Loop Structure

The main loop uses message counts rather than byte positions for coordination.
This works with the struct-of-arrays metadata design.

**Important: Check-then-wait pattern prevents signal loss bugs.**

```go
func (s *Session) eventLoop() error {
    //
    // Track how far we've processed in each ring buffer.
    // These are message indices (not byte offsets).
    //
    var (
        frontendMsgPos int64  // Next message to process from frontend
        backendMsgPos  int64  // Next message to process from backend
    )

    for {
        //
        // Step 1: Snapshot current message counts (lock-free)
        //
        // Load msgWriteCount from each ring buffer.
        // This tells us how many complete messages are available.
        // The snapshot is consistent within this loop iteration.
        //
        frontendMsgLimit := atomic.LoadInt64(&s.frontendRing.msgWriteCount)
        backendMsgLimit := atomic.LoadInt64(&s.backendRing.msgWriteCount)

        //
        // Step 2: Process all available frontend messages
        //
        // Process messages [frontendMsgPos, frontendMsgLimit)
        // This may batch-forward many messages in a single syscall.
        //
        if frontendMsgLimit > frontendMsgPos {
            var err error
            frontendMsgPos, err = s.processFrontendMessages(frontendMsgPos, frontendMsgLimit)
            if err != nil {
                return err
            }
        }

        //
        // Step 3: Process all available backend messages
        //
        if backendMsgLimit > backendMsgPos {
            var err error
            backendMsgPos, err = s.processBackendMessages(backendMsgPos, backendMsgLimit)
            if err != nil {
                return err
            }
        }

        //
        // Step 4: Wait for new data if nothing to process
        //
        // IMPORTANT: We check the condition BEFORE waiting.
        // This prevents a race where:
        //   1. We check msgWriteCount (no new messages)
        //   2. Writer adds message, signals dataReady
        //   3. We start waiting (signal already consumed/dropped)
        //   4. We wait forever
        //
        // By checking the snapshot from step 1, we ensure that
        // if we enter the wait, there truly were no messages.
        // The next signal from writer will wake us.
        //
        if frontendMsgLimit == frontendMsgPos && backendMsgLimit == backendMsgPos {
            select {
            case <-s.frontendRing.dataReady:
                // Frontend has new data, loop to process
            case <-s.backendRing.dataReady:
                // Backend has new data, loop to process
            case <-s.ctx.Done():
                return s.ctx.Err()
            }
        }
        // If we processed messages, loop immediately without waiting
    }
}
```

### Message Processing with Batch Forwarding

Using the pre-parsed metadata arrays, the main thread can efficiently scan
message types and batch contiguous runs of fast-forwardable messages.

```go
func (s *Session) processFrontendMessages(fromMsg, toMsg int64) (int64, error) {
    if fromMsg >= toMsg {
        return fromMsg, nil  // Nothing to process
    }

    ring := s.frontendRing
    batchStart := fromMsg

    //
    // Process messages [fromMsg, toMsg), batching fast-forwardable ones.
    // Goal: minimize syscalls by writing multiple messages in one Write().
    //
    for msgIdx := fromMsg; msgIdx < toMsg; msgIdx++ {
        msgType := ring.types[msgIdx & ring.metaMask]

        if IsFastForwardableClientType(msgType) {
            continue  // Add to batch
        }

        // Flush batch before this non-forwardable message
        if err := s.flushMessageRange(ring, batchStart, msgIdx, s.backendConn); err != nil {
            return msgIdx, err
        }

        // Process message that needs inspection
        if err := s.handleClientMessage(ring, msgIdx, msgType); err != nil {
            return msgIdx, err
        }

        batchStart = msgIdx + 1  // Next batch starts after this message
    }

    // Flush remaining batch
    if err := s.flushMessageRange(ring, batchStart, toMsg, s.backendConn); err != nil {
        return batchStart, err
    }

    // Free buffer space: advance read positions to end of last message
    ring.advanceReader(toMsg)

    return toMsg, nil
}

// advanceReader updates read positions after consuming messages up to msgIdx.
func (r *RingBuffer) advanceReader(msgIdx int64) {
    if msgIdx == 0 {
        return
    }

    // Compute byte offset of end of last consumed message
    lastMsgIdx := msgIdx - 1
    start := r.offsets[lastMsgIdx & r.metaMask]
    physStart := start & r.dataMask
    length := int64(binary.BigEndian.Uint32(r.data[physStart+1:])) + 1
    endOffset := start + length

    atomic.StoreInt64(&r.dataReadPos, endOffset)
    atomic.StoreInt64(&r.msgReadCount, msgIdx)

    // Signal writer (non-blocking)
    select {
    case r.spaceReady <- struct{}{}:
    default:
    }
}

// flushMessageRange writes messages [fromMsg, toMsg) to output in one syscall.
func (s *Session) flushMessageRange(ring *RingBuffer, fromMsg, toMsg int64, w io.Writer) error {
    if fromMsg >= toMsg {
        return nil
    }

    // Byte range: start of first message to end of last message
    startOffset := ring.offsets[fromMsg & ring.metaMask]
    endOffset := ring.messageEnd(toMsg - 1)

    return ring.writeRange(startOffset, endOffset, w)
}

// messageEnd returns the byte offset where message msgIdx ends.
func (r *RingBuffer) messageEnd(msgIdx int64) int64 {
    start := r.offsets[msgIdx & r.metaMask]
    physStart := start & r.dataMask
    // Wire format: [type:1][length:4][body:length-4], total = 1 + length
    length := int64(binary.BigEndian.Uint32(r.data[physStart+1:])) + 1
    return start + length
}

// writeRange writes bytes [startOffset, endOffset) to w, handling wraparound.
func (r *RingBuffer) writeRange(startOffset, endOffset int64, w io.Writer) error {
    physStart := startOffset & r.dataMask
    physEnd := endOffset & r.dataMask

    if physEnd > physStart {
        // Contiguous: [.....|===data===|.....]
        _, err := w.Write(r.data[physStart:physEnd])
        return err
    }

    // Wrapped: [==end==|........|==start==]
    if _, err := w.Write(r.data[physStart:]); err != nil {
        return err
    }
    _, err := w.Write(r.data[:physEnd])
    return err
}
```

### Batch Write Implementation

```go
func (s *Session) flushBatch(ring *RingBuffer, from, to int64, w io.Writer) error {
    if from == to {
        return nil
    }

    physFrom := ring.phys(from)
    physTo := ring.phys(to)

    if physTo > physFrom {
        // Contiguous region: single write syscall
        _, err := w.Write(ring.buf[physFrom:physTo])
        return err
    }

    // Wrapped region: use writev for single syscall (or two writes)
    // Option 1: Two writes
    if _, err := w.Write(ring.buf[physFrom:]); err != nil {
        return err
    }
    _, err := w.Write(ring.buf[:physTo])
    return err

    // Option 2: Use net.Buffers for writev (better)
    // bufs := net.Buffers{ring.buf[physFrom:], ring.buf[:physTo]}
    // _, err := bufs.WriteTo(w)
    // return err
}
```

## Flow Control and Fairness

### Problem: Pipeline Starvation

A client sending 100s of pipelined queries could:
1. Fill the backend with work
2. Hold the backend connection for extended periods
3. Starve other clients waiting for backend connections

### Solution: Transaction-Aware Flow Control

```go
type SessionFlowControl struct {
    // Pipeline depth tracking
    pendingQueries    int32    // Queries sent to backend, not yet complete
    maxPipelineDepth  int32    // Configurable limit (e.g., 16)

    // Transaction state
    inTransaction     bool     // Are we in a transaction block?

    // Backpressure signaling
    pipelineReady     chan struct{}  // Signaled when pipeline has room
}

func (s *Session) handleClientMessage(ring *RingBuffer, start, end int64, msgType byte) error {
    switch msgType {
    case 'Q': // Simple Query
        return s.handleSimpleQuery(ring, start, end)
    case 'P': // Parse (extended query)
        return s.handleParse(ring, start, end)
    case 'E': // Execute
        return s.handleExecute(ring, start, end)
    case 'S': // Sync
        return s.handleSync(ring, start, end)
    // ... etc
    }
}

func (s *Session) handleSync(ring *RingBuffer, start, end int64) error {
    // Sync marks end of a pipeline batch
    atomic.AddInt32(&s.flow.pendingQueries, 1)

    // Check pipeline depth limit
    if atomic.LoadInt32(&s.flow.pendingQueries) >= s.flow.maxPipelineDepth {
        // Must wait for backend to drain before accepting more
        s.pauseFrontendReading()
    }

    // Forward the Sync message
    return s.forwardMessage(ring, start, end, s.backendConn)
}
```

### Transaction Boundary Detection

```go
func (s *Session) handleReadyForQuery(ring *RingBuffer, start, end int64) error {
    // Parse to get transaction status
    body := ring.MessageBody(start, end)
    txStatus := body[0]  // 'I' = idle, 'T' = in transaction, 'E' = error

    // Decrement pipeline counter
    atomic.AddInt32(&s.flow.pendingQueries, -1)

    // Update transaction state
    wasInTransaction := s.flow.inTransaction
    s.flow.inTransaction = (txStatus == 'T')

    // If transaction just ended, consider releasing backend
    if wasInTransaction && !s.flow.inTransaction {
        s.maybeReleaseBackend()
    }

    // Resume frontend reading if pipeline has room
    if atomic.LoadInt32(&s.flow.pendingQueries) < s.flow.maxPipelineDepth {
        s.resumeFrontendReading()
    }

    // Forward to client
    return s.forwardMessage(ring, start, end, s.frontendConn)
}

func (s *Session) maybeReleaseBackend() {
    // Don't release if client has pending data
    if s.frontendRing.Available() > 0 {
        return
    }

    // Don't release if pipeline is active
    if atomic.LoadInt32(&s.flow.pendingQueries) > 0 {
        return
    }

    // Release backend to pool for other clients
    s.pool.Release(s.backend)
    s.backend = nil
}
```

### Pause/Resume Frontend Reading

```go
func (s *Session) pauseFrontendReading() {
    // Stop the frontend ring buffer from reading more data
    // This creates TCP backpressure to the client
    s.frontendRing.Pause()
}

func (s *Session) resumeFrontendReading() {
    s.frontendRing.Resume()
}

// In RingBuffer:
func (r *RingBuffer) Pause() {
    atomic.StoreInt32(&r.paused, 1)
}

func (r *RingBuffer) Resume() {
    if atomic.CompareAndSwapInt32(&r.paused, 1, 0) {
        // Wake writer if it was waiting
        select {
        case r.spaceReady <- struct{}{}:
        default:
        }
    }
}

// Writer checks pause state:
func (r *RingBuffer) WriterLoop(conn io.Reader) error {
    for {
        // Check if paused
        if atomic.LoadInt32(&r.paused) == 1 {
            select {
            case <-r.spaceReady:  // Wait for resume
                continue
            case <-r.ctx.Done():
                return r.ctx.Err()
            }
        }
        // ... normal read logic
    }
}
```

## Configuration

### Buffer Sizing

```go
type RingBufferConfig struct {
    // Data buffer size (must be power of 2)
    // Default: 256KB
    DataSize int

    // Metadata array size - max messages that can be buffered (must be power of 2)
    // Default: 4096 messages
    // Memory: MetaSize * (1 byte for type + 8 bytes for offset) = MetaSize * 9 bytes
    MetaSize int

    // Maximum pipeline depth before backpressure
    // Default: 16 (balance between throughput and fairness)
    MaxPipelineDepth int
}

// Memory calculation per ring buffer:
//   Data buffer: DataSize bytes
//   Types array: MetaSize bytes
//   Offsets array: MetaSize * 8 bytes
//   Total: DataSize + (MetaSize * 9)
//
// Per session: 2 ring buffers (frontend + backend)
//
// Example with defaults (256KB data, 4096 messages):
//   Per ring: 256KB + 36KB = 292KB
//   Per session: 584KB
//   1000 sessions: ~570MB
```

### Tuning Recommendations

| Workload | Buffer Size | Pipeline Depth | Notes |
|----------|-------------|----------------|-------|
| OLTP (many short queries) | 64KB | 8 | Lower latency, more fairness |
| Analytics (large results) | 512KB | 32 | Batch large DataRow messages |
| Balanced | 256KB | 16 | Good default |
| Memory constrained | 32KB | 4 | Minimum viable |

## Message Type Classification

### Fast-Forwardable Messages (No Inspection Needed)

These messages can be batch-forwarded without parsing:

```go
// Client → Backend
var fastForwardClientTypes = map[byte]bool{
    'd': true,  // CopyData
}

// Backend → Client
var fastForwardServerTypes = map[byte]bool{
    'D': true,  // DataRow - bulk result data
    'd': true,  // CopyData - bulk COPY data
    'C': true,  // CommandComplete
    '1': true,  // ParseComplete
    '2': true,  // BindComplete
    '3': true,  // CloseComplete
    'n': true,  // NoData
    'N': true,  // NoticeResponse
    't': true,  // ParameterDescription
    's': true,  // PortalSuspended
    'T': true,  // RowDescription
}
```

### Messages Requiring Inspection

```go
// Must inspect for state tracking or rewriting
var inspectServerTypes = map[byte]bool{
    'Z': true,  // ReadyForQuery - transaction state, pipeline tracking
    'S': true,  // ParameterStatus - session state
    'K': true,  // BackendKeyData - cancel key
    'E': true,  // ErrorResponse - may need rewriting
    'R': true,  // Authentication* - startup flow
}

var inspectClientTypes = map[byte]bool{
    'Q': true,  // Query - may need rewriting, starts flow
    'P': true,  // Parse - named statement tracking
    'B': true,  // Bind - portal tracking
    'E': true,  // Execute - flow tracking
    'S': true,  // Sync - pipeline boundary
    'X': true,  // Terminate - cleanup
    'C': true,  // Close - statement/portal cleanup
}
```

## Error Handling

### Network Errors

```go
func (r *RingBuffer) WriterLoop(conn io.Reader) error {
    for {
        n, err := conn.Read(r.buf[physWrite:limit])
        if n > 0 {
            // Always advance position for data read, even on error
            atomic.AddInt64(&r.writePos, int64(n))
            r.signalDataReady()
        }
        if err != nil {
            // Store error for reader to see
            r.err.Store(&err)
            r.signalDataReady()  // Wake reader to handle error
            return err
        }
    }
}

func (s *Session) eventLoop() error {
    for {
        // Check for errors after processing
        if err := s.frontendRing.Error(); err != nil {
            // Process any remaining buffered data first
            s.drainAndClose()
            return fmt.Errorf("frontend: %w", err)
        }
        // ... similar for backend
    }
}
```

### Graceful Shutdown

```go
func (s *Session) Shutdown() error {
    // 1. Stop accepting new frontend data
    s.frontendRing.Close()

    // 2. Drain any buffered frontend data to backend
    s.drainFrontendToBackend()

    // 3. Wait for backend to complete pending work
    s.drainBackendToFrontend()

    // 4. Close backend connection
    s.backendRing.Close()

    return nil
}
```

## Performance Expectations

These are hypotheses based on the design, not verified results. Actual benchmarking
is required to validate these claims.

### Compared to ChanReader (Theoretical)

| Metric | ChanReader | RingBuffer | Expected Change |
|--------|------------|------------|-----------------|
| Channel ops/msg | 2 | 0 (amortized) | Should decrease |
| Syscalls/msg | 1-2 | ~0.1 (batched) | Should decrease |
| Memory copies | 2 | 1 (encode only) | Should decrease |
| Cache locality | Poor (channel) | Good (sequential) | Should improve |

### Hypothesized Benchmark Results

Based on the synchronization overhead analysis, we hypothesize:
- Simple queries (SELECT 1): Modest improvement (channel overhead is small portion)
- Result-heavy queries: Larger improvement from batch DataRow forwarding
- COPY operations: Potentially significant improvement from batch CopyData forwarding

These need to be validated with actual benchmarks before and after implementation.

## Implementation Plan

### Phase 1: RingBuffer Core
1. Implement `RingBuffer` with atomic coordination
2. Unit tests for wraparound, blocking, signaling
3. Benchmark raw throughput vs channels

### Phase 2: Message Segmentation
1. Implement message boundary detection
2. Handle wrapped messages at buffer boundary
3. Implement batch write with `writev`

### Phase 3: Session Integration
1. Create `RingSession` using ring buffers
2. Implement flow control and pipeline limiting
3. Transaction boundary detection

### Phase 4: Testing & Tuning
1. Integration tests with real PostgreSQL
2. Stress testing for fairness
3. Memory pressure testing
4. Benchmark comparison with pgbouncer

## Open Questions

1. **Buffer sizing**: Should we dynamically resize based on message patterns?
2. **Memory pressure**: Should we use mmap for large buffers to allow OS paging?
3. **Multi-core**: Should writer and reader be pinned to different cores?
4. **Metrics**: What observability do we need for buffer utilization?

## References

- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) - High-performance ring buffer design
- [io_uring](https://kernel.dk/io_uring.pdf) - Linux async I/O with ring buffers
- [pgbouncer](https://www.pgbouncer.org/) - Single-threaded event loop architecture
