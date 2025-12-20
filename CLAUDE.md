# Claude Instructions

## Testing

Run tests using `bin/test` instead of `go test` directly.

```sh
bin/test ./pkg/config
```

## Building

Requires the jsonv2 experiment:

```sh
GOEXPERIMENT=jsonv2 go build ./...
```

## Running

Use `bin/run` instead of `go run` directly.

```sh
bin/run ./cmd/pglink -config pglink.json
```

## Code Style

### Mutex locking

Always defer releasing locks on the line immediately after acquiring them, unless doing so would be incorrect. Extract helper functions if needed to maintain this pattern.

```go
// Good
func (c *Cache) Get(key string) (string, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    val, ok := c.data[key]
    return val, ok
}

// Bad - don't manually unlock in multiple places
func (c *Cache) Get(key string) (string, bool) {
    c.mu.RLock()
    if val, ok := c.data[key]; ok {
        c.mu.RUnlock()
        return val, true
    }
    c.mu.RUnlock()
    return "", false
}
```
