package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	cli "github.com/urfave/cli/v2"
)

// configCheckInterval determines how frequently the config file is checked for
// changes, to see if it needs to be reloaded. Set this to 0 to disable
// checking the config file.
const configCheckInterval = 5 * time.Second

func main() {
	app := &cli.App{
		Name:  "indexstar",
		Usage: "indexstar is a point in the content routing galaxy - routes requests in a star topology",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:      "config",
				Usage:     "Path to config file",
				TakesFile: true,
			},
			&cli.StringFlag{
				Name:  "listen",
				Usage: "HTTP server Listen address",
				Value: ":8080",
			},
			&cli.StringFlag{
				Name:  "metrics",
				Usage: "Metrics server listen address",
				Value: ":8081",
			},
			&cli.StringSliceFlag{
				Name:  "backends",
				Usage: "Backends to propagate requests to.",
				Value: cli.NewStringSlice("https://cid.contact/"),
			},
			&cli.StringSliceFlag{
				Name:  "cascadeBackends",
				Usage: "Backends to propagate lookup with SERVER_CASCADE_LABELS env var as query parameter",
			},
			&cli.StringFlag{
				Name:  "fallbackBackend",
				Usage: "Fallback Backend is used for requests to the index page",
			},
			&cli.BoolFlag{
				Name:  "translateReframe",
				Usage: "translate reframe requests into find requests to backends",
			},
			&cli.BoolFlag{
				Name:  "translateNonStreaming",
				Usage: "Whether to translate non-streaming JSON requests to streaming NDJSON requests before scattering to backends.",
			},
		},
		Action: func(c *cli.Context) error {
			exit := make(chan os.Signal, 1)
			signal.Notify(exit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

			s, err := NewServer(c)
			if err != nil {
				return err
			}

			sighup := make(chan os.Signal, 1)
			signal.Notify(sighup, syscall.SIGHUP)

			done := s.Serve()

			var (
				cfgPath  string
				modTime  time.Time
				ticker   *time.Ticker
				timeChan <-chan time.Time
			)
			if configCheckInterval != 0 {
				cfgPath = s.cfgBase
				if cfgPath == "" {
					cfgPath, err = Filename("")
					if err != nil {
						return err
					}
				}

				modTime, _, err = fileChanged(cfgPath, modTime)
				if err != nil {
					// No config file is not an error.
					if !errors.Is(err, os.ErrNotExist) {
						log.Error(err)
					}
				} else {
					ticker = time.NewTicker(configCheckInterval)
					timeChan = ticker.C
				}
			}

			reloadSig := make(chan struct{}, 1)
			for {
				select {
				case <-sighup:
					select {
					case reloadSig <- struct{}{}:
					default:
					}
				case <-exit:
					return nil
				case err := <-done:
					return err
				case <-reloadSig:
					err := s.Reload(c)
					if err != nil {
						log.Warnf("couldn't reload servers: %s", err)
					}
				case <-timeChan:
					var changed bool
					modTime, changed, err = fileChanged(s.cfgBase, modTime)
					if err != nil {
						log.Errorw("Cannot stat config file", "err", err, "path", cfgPath)
						ticker.Stop()
						ticker = nil
						timeChan = nil // reading from nil channel blocks forever
						continue
					}
					if changed {
						reloadSig <- struct{}{}
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

func fileChanged(filePath string, modTime time.Time) (time.Time, bool, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return modTime, false, err
	}
	if fi.ModTime() != modTime {
		return fi.ModTime(), true, nil
	}
	return modTime, false, nil
}
