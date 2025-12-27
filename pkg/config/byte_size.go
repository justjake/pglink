package config

import (
	"encoding/json/v2"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ByteSize is an int64 byte count that can be unmarshaled from human-readable strings
// like "256kb", "1MB", "16KiB", or plain numbers.
type ByteSize int64

// Common byte size constants
const (
	Byte ByteSize = 1
	KB   ByteSize = 1000
	KiB  ByteSize = 1024
	MB   ByteSize = 1000 * 1000
	MiB  ByteSize = 1024 * 1024
	GB   ByteSize = 1000 * 1000 * 1000
	GiB  ByteSize = 1024 * 1024 * 1024
)

// Int64 returns the byte size as an int64.
func (b ByteSize) Int64() int64 {
	return int64(b)
}

// String returns a human-readable representation.
func (b ByteSize) String() string {
	switch {
	case b >= GiB && b%GiB == 0:
		return fmt.Sprintf("%dGiB", b/GiB)
	case b >= MiB && b%MiB == 0:
		return fmt.Sprintf("%dMiB", b/MiB)
	case b >= KiB && b%KiB == 0:
		return fmt.Sprintf("%dKiB", b/KiB)
	case b >= GB && b%GB == 0:
		return fmt.Sprintf("%dGB", b/GB)
	case b >= MB && b%MB == 0:
		return fmt.Sprintf("%dMB", b/MB)
	case b >= KB && b%KB == 0:
		return fmt.Sprintf("%dKB", b/KB)
	default:
		return fmt.Sprintf("%d", b)
	}
}

func (b ByteSize) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.String())
}

var byteSizeRegex = regexp.MustCompile(`(?i)^(\d+(?:\.\d+)?)\s*(b|kb|kib|mb|mib|gb|gib|k|m|g)?$`)

func (b *ByteSize) UnmarshalJSON(data []byte) error {
	// Try parsing as string first
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		// Try parsing as number (raw bytes)
		var n int64
		if err := json.Unmarshal(data, &n); err != nil {
			return fmt.Errorf("expected byte size string or number, got %s", string(data))
		}
		*b = ByteSize(n)
		return nil
	}

	parsed, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = parsed
	return nil
}

// ParseByteSize parses a human-readable byte size string.
// Supported formats: "256", "256b", "256kb", "256kib", "256k", "1mb", "1mib", "1m", etc.
// Case insensitive. IEC units (KiB, MiB, GiB) use 1024, SI units (KB, MB, GB) use 1000.
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty byte size string")
	}

	matches := byteSizeRegex.FindStringSubmatch(s)
	if matches == nil {
		return 0, fmt.Errorf("invalid byte size %q: expected format like '256kb', '1MiB', or '1024'", s)
	}

	numStr := matches[1]
	unit := strings.ToLower(matches[2])

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}

	var multiplier int64
	switch unit {
	case "", "b":
		multiplier = int64(Byte)
	case "k", "kb":
		multiplier = int64(KB)
	case "kib":
		multiplier = int64(KiB)
	case "m", "mb":
		multiplier = int64(MB)
	case "mib":
		multiplier = int64(MiB)
	case "g", "gb":
		multiplier = int64(GB)
	case "gib":
		multiplier = int64(GiB)
	default:
		return 0, fmt.Errorf("invalid byte size unit %q", unit)
	}

	return ByteSize(int64(num * float64(multiplier))), nil
}
