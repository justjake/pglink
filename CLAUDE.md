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
