# Claude Instructions

We are implementing a serious, high-performance PostgreSQL wire protocol proxy intended for use in production called "pglink".

## High standards

You must take your time and think deeply when working on pglink. Do not be lazy. Do not rush.

When you encounter a bug, take a step back and look at the structure of the project. You should fix the underlying issues to make the code correct by design, rather than adding a bandaid to fix the symptom. If you need to add a goroutine to solve a bug, you are probably making a bandaid fix rather than addressing the underlying issue.


## Test timeouts

This project is highly asynchronous, so it's easy to write tests that hang
forever.  Always run test commands with a 30s timeout or less, and consider test
runs that take longer than 30s to be a bug.

When you write a new test case, ensure the test case fails after 30s no matter what.
Add such timeouts to any tests you encounter.

## Debug logging

To run tests with debug logging enabled, set the `PGLINK_LOG_LEVEL` environment variable:

```bash
PGLINK_LOG_LEVEL=debug bin/test ./e2e -run TestBasicQuery
```

Supported log levels: `debug`, `info`, `warn`, `error`.

## Structure

- `pkg/frontend`: Interactions between clients and the proxy. Accepts incoming connections, authenticates clients, proxies client requests to the backend.
- `pkg/backend`: Interactions between the proxy and backend PostgreSQL / Materialize servers. Pools connections to the backend.
- `pkg/config`: Config loading and validation.
- `cmd/pglink`: Main entry point.

## Scripts

**Always check `bin/` for available scripts and prefer using them over direct commands.** The scripts handle environment setup (mise, GOEXPERIMENT, etc.) automatically.

When adding or modifying scripts in `bin/`, update this list.

| Script | Description |
|--------|-------------|
| `bin/build` | Build the pglink binary to `out/pglink` |
| `bin/doc` | Generate README.md from README.in.md and config types |
| `bin/format` | Format Go code with `go fmt` |
| `bin/go` | Proxy to `go` command with mise environment (prefer `bin/build` for building) |
| `bin/lint` | Run golangci-lint |
| `bin/run` | Run pglink (e.g., `bin/run -config pglink.json`) |
| `bin/setup` | Install mise tools and configure git hooks |
| `bin/test` | Run tests (e.g., `bin/test ./pkg/config`) |
| `bin/tidy` | Run `go mod tidy` |

## Code Style

### Error handling

Never ignore errors. Errors should be handled directly, or annotated with context (eg w/ fmt.Errorf) and returned to the caller. In rare cases it may be acceptable to panic, but you should ask before doing so.

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

### Iterators

When implementing iterable data structures, use modern Go iterators (`iter.Seq`, `iter.Seq2`) instead of ad-hoc interfaces like `ForEach` methods. This enables use with `range` and the `iter` package utilities.

```go
// Good - use iter.Seq for iteration
func (c *Cache) All() iter.Seq2[string, string] {
    return func(yield func(string, string) bool) {
        c.mu.RLock()
        defer c.mu.RUnlock()
        for k, v := range c.data {
            if !yield(k, v) {
                return
            }
        }
    }
}

// Usage:
for k, v := range cache.All() {
    fmt.Println(k, v)
}

// Bad - ad-hoc callback interface
func (c *Cache) ForEach(fn func(key, value string)) {
    // ...
}
```
