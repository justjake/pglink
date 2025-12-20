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
}

func main() {
	printBanner()

	configPath := flag.String("config", "", "path to pglink.json config file")
	flag.Usage = printUsage
	flag.Parse()

	if *configPath == "" {
		printUsage()
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
