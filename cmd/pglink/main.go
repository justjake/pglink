package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/frontend"
	"github.com/lucasb-eyer/go-colorful"
)

var bannerLines = []string{
	`                  __ _         __   `,
	`    ____   ____ _/ /(_)____   / /__ `,
	`   / __ \ / __ '/ // // __ \ / //_/ `,
	`  / /_/ // /_/ / // // / / // ,<    `,
	` / .___/ \__, /_//_//_/ /_//_/|_|   `,
	`/_/     /____/                      `,
}

func printBanner() {
	// Check if colors are disabled
	if os.Getenv("NO_COLOR") != "" {
		// No color support - print plain banner
		for _, line := range bannerLines {
			fmt.Println(line)
		}
		fmt.Println()
		return
	}

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

func main() {
	printBanner()

	configPath := flag.String("config", "", "path to pglink.json config file (required)")
	flag.Parse()

	if *configPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := config.ReadConfigFile(*configPath)
	if err != nil {
		log.Fatalf("failed to read config: %v", err)
	}

	svc := frontend.NewService(cfg)
	if err := svc.Listen(); err != nil {
		log.Fatalf("service error: %v", err)
	}
}
