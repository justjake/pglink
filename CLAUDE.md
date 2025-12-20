# Claude Instructions

## Scripts

**Always check `bin/` for available scripts and prefer using them over direct commands.** The scripts handle environment setup (mise, GOEXPERIMENT, etc.) automatically.

When adding or modifying scripts in `bin/`, update this list.

| Script | Description |
|--------|-------------|
| `bin/build` | Build the pglink binary to `out/pglink` |
| `bin/format` | Format Go code with `go fmt` |
| `bin/lint` | Run golangci-lint |
| `bin/run` | Run pglink (e.g., `bin/run -config pglink.json`) |
| `bin/setup` | Install mise tools and configure git hooks |
| `bin/test` | Run tests (e.g., `bin/test ./pkg/config`) |
| `bin/tidy` | Run `go mod tidy` |

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
