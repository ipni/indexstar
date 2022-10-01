package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	cli "github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "indexstar",
		Usage: "indexstar is a point in the content routing galaxy - routes requests in a star topology",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:      "config",
				Usage:     "config file",
				TakesFile: true,
			},
			&cli.StringFlag{
				Name:  "listen",
				Usage: "listen address",
				Value: ":8080",
			},
			&cli.StringFlag{
				Name:  "metrics",
				Usage: "metrics address",
				Value: ":8081",
			},
			&cli.StringSliceFlag{
				Name:  "backends",
				Usage: "backends to use",
				Value: cli.NewStringSlice("https://cid.contact/"),
			},
			&cli.BoolFlag{
				Name:  "translateReframe",
				Usage: "translate reframe requests into find requests to backends",
			},
		},
		Action: func(c *cli.Context) error {
			exit := make(chan os.Signal, 1)
			signal.Notify(exit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

			s, err := NewServer(c)
			if err != nil {
				return err
			}

			reload := make(chan os.Signal, 1)
			signal.Notify(reload, syscall.SIGHUP)

			done := s.Serve()

			for {
				select {
				case <-exit:
					return nil
				case err := <-done:
					return err
				case <-reload:
					err := s.Reload()
					if err != nil {
						log.Warnf("couldn't reload servers: %s", err)
					}
				}
			}
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}
