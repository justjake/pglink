package pgproxy

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	gnetLog "github.com/panjf2000/gnet/v2/pkg/logging"
)

type gnetLogger struct {
	logger *slog.Logger
}

func (g *gnetLogger) levelf(level slog.Level, format string, args ...any) {
	if !g.logger.Enabled(context.Background(), level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip [Callers, Levelf, Infof]
	r := slog.NewRecord(time.Now(), level, fmt.Sprintf(format, args...), pcs[0])
	_ = g.logger.Handler().Handle(context.Background(), r)
}

// Debugf implements [logging.Logger].
func (g *gnetLogger) Debugf(format string, args ...any) {
	g.levelf(slog.LevelDebug, format, args...)
}

// Infof implements [logging.Logger].
func (g *gnetLogger) Infof(format string, args ...any) {
	g.levelf(slog.LevelInfo, format, args...)
}

// Errorf implements [logging.Logger].
func (g *gnetLogger) Errorf(format string, args ...any) {
	g.levelf(slog.LevelError, format, args...)
}

// Fatalf implements [logging.Logger].
func (g *gnetLogger) Fatalf(format string, args ...any) {
	g.levelf(slog.LevelError, format, args...)
	os.Exit(1)
}

// Warnf implements [logging.Logger].
func (g *gnetLogger) Warnf(format string, args ...any) {
	g.levelf(slog.LevelWarn, format, args...)
}

var _ gnetLog.Logger = (*gnetLogger)(nil)
