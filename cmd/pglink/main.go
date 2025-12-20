package main

import (
	"flag"
	"log"
	"os"

	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/frontend"
)

func main() {
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
