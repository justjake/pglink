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

## Debugging Benchmarks

### Quick iteration

Use shorter duration for faster feedback when debugging benchmark issues:

```bash
bin/bench -cases copy_in -duration 10s -rounds 1
```

### Benchmark targets

By default, `bin/bench` runs pglink, pgbouncer, and direct benchmarks. Control targets with flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-pglink` | true | Run pglink proxy benchmarks |
| `-pgbouncer` | true | Run pgbouncer proxy benchmarks |
| `-direct` | true | Run direct PostgreSQL connection benchmarks |
| `-mitm-proxy` | false | Run mitm-proxy benchmarks (simple passthrough proxy for baseline comparison) |

Examples:

```bash
# Compare pglink vs direct only
bin/bench -pgbouncer=false

# Run mitm-proxy vs direct vs pgbouncer (without pglink)
bin/bench -mitm-proxy -pglink=false

# Run all targets including mitm-proxy
bin/bench -mitm-proxy
```

### mitm-proxy options

The mitm-proxy is a minimal passthrough proxy useful for measuring proxy overhead:

| Flag | Description |
|------|-------------|
| `-mitm-single-thread` | Run mitm-proxy with GOMAXPROCS=1 (single-threaded) |
| `-mitm-split` | Use split I/O mode for mitm-proxy (2-goroutine model) |

### Enable debug logging

Use the `-debug` flag to enable debug-level logging for spawned pglink processes:

```bash
bin/bench -cases copy_in -debug
```

Or manually pass arguments:

```bash
bin/bench -a-args "-log-level debug"
```

### View pglink logs

Benchmark logs are written to the output directory:

```bash
cat out/benchmarks/*/pglink.*.log
```

### Compare variants (A/B testing)

Use A/B flags to compare different worktrees or configurations:

```bash
bin/bench -a-label "main" -a-worktree ../pglink \
          -b-label "fix" -b-worktree ./worktrees/my-fix
```

### Dump ring buffer state

Send SIGUSR1 to a running pglink process to dump ring buffer state for all sessions:

```bash
kill -USR1 $(pgrep pglink)
```

This also triggers a flight recorder snapshot if enabled.

**Note:** `bin/bench` automatically sends SIGUSR1 when a benchmark run fails, so ring buffer stats will be in the pglink logs.

## Structure

- `pkg/frontend`: Interactions between clients and the proxy. Accepts incoming connections, authenticates clients, proxies client requests to the backend.
- `pkg/backend`: Interactions between the proxy and backend PostgreSQL / Materialize servers. Pools connections to the backend.
- `pkg/config`: Config loading and validation.
- `pkg/config/pgbouncer`: Generates PgBouncer configuration files from pglink config (for benchmarking).
- `cmd/pglink`: Main entry point.

### PgBouncer Config Generator

The `pkg/config/pgbouncer` package generates equivalent PgBouncer configuration from pglink config. This is used for benchmarking pglink against PgBouncer.

**Important:** When adding new config options to `DatabaseConfig` that have PgBouncer equivalents (e.g., timeout settings, pool settings), you must also update `pkg/config/pgbouncer/pgbouncer.go` to pass through those settings. This ensures benchmark comparisons are fair.

## Reference: pgx, pgconn, pgproto3

Libraries for PostgreSQL. You can read their source in ./vendor.

## Reference: PgBouncer

The PgBouncer source code is available as a git submodule at `third_party/pgbouncer/` for reference purposes. PgBouncer is a mature, production-grade PostgreSQL connection pooler that implements similar functionality to pglink.

**Important:** The PgBouncer source is read-only reference material. Do not modify it. Use it to:
- Understand how a production pooler handles edge cases
- Reference protocol handling details
- Compare implementation approaches

To initialize the submodule (if not already done):
```bash
git submodule update --init third_party/pgbouncer
```

## Scripts

**Always check `bin/` for available scripts and prefer using them over direct commands.** The scripts handle environment setup (mise, GOEXPERIMENT, etc.) automatically.

When adding or modifying scripts in `bin/`, update this list.

| Script | Description |
|--------|-------------|
| `bin/bench` | Run benchmarks. Presets: `smoke` (2s, 1 round), `full` (10s, 3 rounds). Defaults: pgbouncer=true, observable=true |
| `bin/bench-simple` | Run simple query protocol benchmarks (SELECT 1, COPY IN/OUT) |
| `bin/build` | Build the pglink binary to `out/pglink` |
| `bin/doc` | Generate README.md from README.in.md and config types |
| `bin/format` | Format Go code with `go fmt` |
| `bin/go` | Proxy to `go` command with mise environment (prefer `bin/build` for building) |
| `bin/lint` | Run golangci-lint |
| `bin/run` | Run pglink (e.g., `bin/run -config pglink.json`) |
| `bin/setup` | Install mise tools and configure git hooks |
| `bin/test` | Run tests (e.g., `bin/test ./pkg/config`) |
| `bin/tidy` | Run `go mod tidy` |
| `bin/tool` | Run Go tools with mise environment (e.g., `bin/tool benchstat file1.txt file2.txt`) |
| `bin/update-docker-compose` | Update docker-compose.yaml images to latest versions |
| `bin/worktree-new` | Create a new git worktree for independent development |
| `bin/worktree-list` | List all worktrees with their branches and Claude plans |
| `bin/worktree-rm` | Remove a worktree |
| `bin/worktree-claude` | Start Claude in a worktree, with session resumption |

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

## Working in Worktrees

When told to "work independently in a worktree", use git worktrees to work on a separate branch without affecting the main checkout.

### Main worktree

Claude shouldn't make commits or change git state in the main worktree.
It's okay to make commits in worktrees created by Claude.

### Creating a worktree

```bash
bin/worktree-new <task-name> [path-to-claude-plan]
```

This creates:

- A worktree at `worktrees/<task-name>/`
- A branch named `worktree/<task-name>`
- A symlink to the Claude plan file (if provided)

### Working in a worktree

1. Change to the worktree directory: `cd worktrees/<task-name>`
2. Use `bin/build`, `bin/test`, etc. normally - they work in worktrees
3. **Commit work regularly** to the worktree branch

### Rebasing a worktree

When rebasing a worktree branch on main, use the **local** main branch (not `origin/main`):

```bash
git rebase main        # Correct - uses local main from the root repo
git rebase origin/main # Wrong - may be stale if you haven't fetched
```

Worktrees share the same `.git` directory with the main checkout, so the local `main` branch is always up-to-date with the root repo.

### Listing and removing worktrees

```bash
bin/worktree-list              # List all worktrees
bin/worktree-rm <task-name>    # Remove a worktree
```