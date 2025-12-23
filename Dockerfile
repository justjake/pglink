# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /pglink ./cmd/pglink

# Runtime stage
FROM alpine:3.19

# Install ca-certificates for TLS
RUN apk add --no-cache ca-certificates

# Copy the binary from builder
COPY --from=builder /pglink /usr/local/bin/pglink

# Create a non-root user
RUN adduser -D -u 1000 pglink
USER pglink

# Default port
EXPOSE 5432

ENTRYPOINT ["pglink"]
