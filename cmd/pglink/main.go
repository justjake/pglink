package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/config/pgbouncer"
	"github.com/justjake/pglink/pkg/frontend"
	"github.com/justjake/pglink/pkg/observability"
	"github.com/lucasb-eyer/go-colorful"
	"golang.org/x/term"
)

//go:embed README.md
var readmeMarkdown string

var bannerLines = []string{
	`                  __ _         __   `,
	`    ____   ____ _/ /(_)____   / /__ `,
	`   / __ \ / __ '/ // // __ \ / //_/ `,
	`  / /_/ // /_/ / // // / / // ,<    `,
	` / .___/ \__, /_//_//_/ /_//_/|_|   `,
	`/_/     /____/                      `,
}

func printBanner() {
	// Gradient from teal to purple
	teal, _ := colorful.Hex("#00CED1")
	purple, _ := colorful.Hex("#9B30FF")
	bgColor := lipgloss.Color("#1a1a2e")

	maxWidth := len(bannerLines[0])

	var lines []string
	for _, line := range bannerLines {
		var result strings.Builder
		for i, r := range line {
			t := float64(i) / float64(maxWidth-1)
			c := teal.BlendLuv(purple, t)
			style := lipgloss.NewStyle().
				Foreground(lipgloss.Color(c.Hex())).
				Background(bgColor).
				Bold(true)
			result.WriteString(style.Render(string(r)))
		}
		lines = append(lines, result.String())
	}

	box := lipgloss.NewStyle().
		Background(bgColor).
		Padding(0, 2).
		Render(strings.Join(lines, "\n"))

	fmt.Println(box)
	fmt.Println()
}

var (
	// Styles for usage output
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#00CED1"))

	descStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#888888"))

	flagStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#9B30FF")).
			Bold(true)

	exampleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#888888")).
			Italic(true)
)

func printUsage() {
	fmt.Println(titleStyle.Render("Usage:"))
	fmt.Print("  pglink ")
	flag.VisitAll(func(f *flag.Flag) {
		if f.Name == "help" {
			return
		}
		fmt.Printf("%s ", flagStyle.Render("-"+f.Name+" <"+f.Name+">"))
	})
	fmt.Println()
	fmt.Println()

	fmt.Println(titleStyle.Render("Options:"))
	flag.VisitAll(func(f *flag.Flag) {
		typeName := fmt.Sprintf("%T", f.Value)
		// Extract type name from *flag.stringValue -> string
		typeName = strings.TrimPrefix(typeName, "*flag.")
		typeName = strings.TrimSuffix(typeName, "Value")

		fmt.Printf("  %s %s\n",
			flagStyle.Render("-"+f.Name),
			descStyle.Render(typeName))
		fmt.Printf("      %s\n", f.Usage)
	})
	fmt.Println()

	fmt.Println(titleStyle.Render("Example:"))
	fmt.Println(exampleStyle.Render("  pglink -config /etc/pglink/pglink.json"))
	fmt.Println()

	fmt.Println(descStyle.Render("Run 'pglink -help' for full configuration documentation."))
	fmt.Println()
}

func printFullDocs() {
	// Get terminal width, default to 80 if not a terminal
	width := 80
	if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 0 {
		width = w
	}

	renderer, err := glamour.NewTermRenderer(
		glamour.WithAutoStyle(),
		glamour.WithWordWrap(width),
	)
	if err != nil {
		// Fallback to raw markdown
		fmt.Println(readmeMarkdown)
		return
	}

	out, err := renderer.Render(readmeMarkdown)
	if err != nil {
		// Fallback to raw markdown
		fmt.Println(readmeMarkdown)
		return
	}

	fmt.Print(out)
}

func main() {
	configPath := flag.String("config", "", "path to pglink.json config file")
	listenAddr := flag.String("listen-addr", "", "listen address (overrides config file, e.g. :16432, 0.0.0.0:5432)")
	algo := flag.String("algo", "", "session algorithm: default, ring (experimental)")
	prometheusListen := flag.String("prometheus-listen", "", "enable Prometheus metrics (format: :9090 or :9090/metrics)")
	flightRecorderDir := flag.String("flight-recorder", "", "enable flight recorder, save snapshots to this directory")
	flightRecorderMinAge := flag.String("flight-recorder-min-age", "", "flight recorder min age (e.g. 10s, 1m)")
	flightRecorderMaxBytes := flag.Int64("flight-recorder-max-bytes", 0, "flight recorder max bytes (e.g. 10485760 for 10MB)")
	jsonLogs := flag.Bool("json", false, "output logs in JSON format")
	showHelp := flag.Bool("help", false, "show full documentation")
	writePgbouncerDir := flag.String("write-pgbouncer-config-dir", "", "write equivalent pgbouncer config to this directory and exit")
	pgbouncerPort := flag.Int("pgbouncer-port", 6432, "port for pgbouncer to listen on (used with -write-pgbouncer-config-dir)")
	flag.Usage = printUsage
	flag.Parse()

	// Show full docs with -help
	if *showHelp {
		printFullDocs()
		os.Exit(0)
	}

	// Show compact usage when no config provided
	if *configPath == "" {
		printBanner()
		printUsage()
		os.Exit(1)
	}

	// Set up logger
	var handler slog.Handler
	if *jsonLogs {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, nil)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	cfg, err := config.ReadConfigFile(*configPath)
	if err != nil {
		logger.Error("failed to read config", "error", err)
		os.Exit(1)
	}

	// Apply CLI overrides
	if *listenAddr != "" {
		cfg.SetListenAddr(*listenAddr)
	}

	// Apply algo CLI override
	if *algo != "" {
		cfg.SetAlgo(*algo)
		logger.Info("using session algorithm", "algo", *algo)
	}

	// Apply Prometheus CLI override
	if *prometheusListen != "" {
		cfg.Prometheus = config.ParsePrometheusListen(*prometheusListen)
	}

	// Apply flight recorder CLI overrides
	if *flightRecorderDir != "" {
		cfg.FlightRecorder = config.ParseFlightRecorderDir(*flightRecorderDir)
	}
	if cfg.FlightRecorder != nil {
		if *flightRecorderMinAge != "" {
			parsed, err := time.ParseDuration(*flightRecorderMinAge)
			if err != nil {
				logger.Error("invalid flight-recorder-min-age", "error", err)
				os.Exit(1)
			}
			cfg.FlightRecorder.MinAge = config.Duration(parsed)
		}
		if *flightRecorderMaxBytes > 0 {
			cfg.FlightRecorder.MaxBytes = *flightRecorderMaxBytes
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	secrets, err := config.NewSecretCacheFromEnv(ctx)
	if err != nil {
		logger.Error("failed to create secrets cache", "error", err)
		os.Exit(1)
	}

	fsys := os.DirFS(cfg.Dir())

	if err := cfg.Validate(ctx, fsys, secrets, logger); err != nil {
		logger.Error("config validation failed", "error", err)
		os.Exit(1)
	}
	logger.Info("config validated", "path", cfg.FilePath())

	// Handle -write-pgbouncer-config-dir flag
	if *writePgbouncerDir != "" {
		pgbCfg, err := pgbouncer.GenerateConfig(ctx, cfg, secrets, *pgbouncerPort)
		if err != nil {
			logger.Error("failed to generate pgbouncer config", "error", err)
			os.Exit(1)
		}

		if err := pgbCfg.WriteToDir(*writePgbouncerDir); err != nil {
			logger.Error("failed to write pgbouncer config", "error", err)
			os.Exit(1)
		}

		logger.Info("wrote pgbouncer config",
			"dir", *writePgbouncerDir,
			"port", *pgbouncerPort)
		os.Exit(0)
	}

	// Initialize OpenTelemetry tracing if configured
	tracingEnabled := false
	var tracerProvider *observability.TracerProvider
	if cfg.OpenTelemetry != nil && cfg.OpenTelemetry.Enabled {
		var err error
		tracerProvider, err = observability.NewTracerProvider(ctx, cfg.OpenTelemetry)
		if err != nil {
			logger.Error("failed to create tracer provider", "error", err)
			os.Exit(1)
		}
		if tracerProvider != nil {
			tracingEnabled = true
			logger.Info("OpenTelemetry tracing enabled",
				"service_name", cfg.OpenTelemetry.GetServiceName(),
				"endpoint", cfg.OpenTelemetry.OTLPEndpoint,
				"protocol", cfg.OpenTelemetry.GetOTLPProtocol())
			defer func() {
				if err := tracerProvider.Shutdown(context.Background()); err != nil {
					logger.Error("failed to shutdown tracer provider", "error", err)
				}
			}()
		}
	}

	// Initialize Prometheus metrics if configured
	var metrics *observability.Metrics
	var metricsServer *observability.MetricsServer
	if cfg.Prometheus != nil {
		metrics = observability.DefaultMetrics()
		metricsServer = observability.NewMetricsServer(cfg.Prometheus, logger)
		if err := metricsServer.Start(); err != nil {
			logger.Error("failed to start metrics server", "error", err)
			os.Exit(1)
		}
		logger.Info("Prometheus metrics enabled",
			"listen", cfg.Prometheus.GetListen(),
			"path", cfg.Prometheus.GetPath())
		defer func() {
			if err := metricsServer.Shutdown(context.Background()); err != nil {
				logger.Error("failed to shutdown metrics server", "error", err)
			}
		}()
	}

	// Initialize flight recorder if configured
	var flightRecorder *observability.FlightRecorderService
	var flightRecorderMetrics *observability.FlightRecorderMetrics
	if cfg.FlightRecorder != nil {
		// Create metrics if Prometheus is enabled
		if metrics != nil {
			flightRecorderMetrics = observability.NewFlightRecorderMetrics()
		}

		var err error
		flightRecorder, err = observability.NewFlightRecorderService(cfg.FlightRecorder, logger, flightRecorderMetrics)
		if err != nil {
			logger.Error("failed to create flight recorder", "error", err)
			os.Exit(1)
		}
		if flightRecorder != nil {
			if err := flightRecorder.Start(ctx); err != nil {
				logger.Error("failed to start flight recorder", "error", err)
				os.Exit(1)
			}
			flightRecorder.SetupSignalHandler(ctx)
			// Register HTTP endpoints on metrics server if available
			if metricsServer != nil {
				flightRecorder.RegisterHTTPHandlers(metricsServer.Mux())
			}
			defer flightRecorder.Stop()
		}
	}

	svc, err := frontend.NewService(ctx, cfg, fsys, secrets, logger, tracingEnabled, metrics)
	if err != nil {
		logger.Error("failed to create service", "error", err)
		os.Exit(1)
	}

	// Handle shutdown signal in goroutine
	go func() {
		sig := <-sigChan
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
		svc.Shutdown()
	}()

	if err := svc.Listen(); err != nil {
		if err == context.Canceled {
			logger.Info("service shut down gracefully")
		} else {
			logger.Error("service error", "error", err)
			os.Exit(1)
		}
	}
}
