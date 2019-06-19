package main

import (
	"context"
	"os"

	"github.com/highstead/bttest"

	log "github.com/sirupsen/logrus"
)

func main() {
	cfg, err := bttest.GetConfig("config/config.json")

	// Pure dev, dont expect a config
	if err != nil {
		log.WithError(err).Fatal("Unable to parse config")
	}

	if cfg.IsDebug {
		log.SetLevel(log.DebugLevel)
		log.SetOutput(os.Stdout)
		log.WithField("Config", cfg).Println("Starting bttest")
	} else {
		log.SetOutput(os.Stderr)
		log.SetFormatter(&log.JSONFormatter{})
		log.Println("Starting bttest")
	}
	stop := make(chan os.Signal, 1)
	ctx := context.Background()
	tester, err := bttest.NewTester(ctx, cfg)
	if err != nil {
		log.WithError(err).Fatal("Unable to create new tester")
	}

	tester.Run(ctx)

	<-stop
}
