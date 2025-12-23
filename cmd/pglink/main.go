package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/config/pgbouncer"
	"github.com/justjake/pglink/pkg/frontend"
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

	ctx := context.Background()

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

	svc, err := frontend.NewService(ctx, cfg, fsys, secrets, logger)
	if err != nil {
		logger.Error("failed to create service", "error", err)
		os.Exit(1)
	}

	if err := svc.Listen(); err != nil {
		logger.Error("service error", "error", err)
		os.Exit(1)
	}
}
