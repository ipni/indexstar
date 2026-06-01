package main

import (
	"context"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reloadSig := make(chan os.Signal, 1)
	signal.Notify(reloadSig, syscall.SIGHUP)
	defer signal.Stop(reloadSig)

	err := runApp(ctx, os.Args, reloadSig, configCheckInterval, NewServer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func runApp(
	ctx context.Context,
	args []string,
	reloadSig chan os.Signal,
	configCheckInterval time.Duration,
	newServer func(c *cli.Context) (serverInterface, error),
) error {
	app := &cli.App{
		Name:  "indexstar",
		Usage: "indexstar is a point in the content routing galaxy - routes requests in a star topology",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:      configArg,
				Usage:     "Path to config file",
				TakesFile: true,
			},
			&cli.StringFlag{
				Name:  listenArg,
				Usage: "HTTP server Listen address",
				Value: ":8080",
			},
			&cli.StringFlag{
				Name:  metricsArg,
				Usage: "Metrics server listen address",
				Value: ":8081",
			},
			&cli.StringSliceFlag{
				Name:  backendsArg,
				Usage: "Backends to propagate regular requests to.",
				Value: cli.NewStringSlice("https://cid.contact/"),
			},
			&cli.StringSliceFlag{
				Name:  cascadeBackendsArg,
				Usage: "Backends to propagate lookup with SERVER_CASCADE_LABELS env var as query parameter",
			},
			&cli.StringSliceFlag{
				Name:  dhBackendsArg,
				Usage: "Backends to propagate Double Hashed requests to.",
			},
			&cli.StringSliceFlag{
				Name:  providersBackendsArg,
				Usage: "Backends to propagate providers requests to.",
			},
			&cli.BoolFlag{
				Name:  translateNonStreamingArg,
				Usage: "Whether to translate non-streaming JSON requests to streaming NDJSON requests before scattering to backends.",
			},
			&cli.BoolFlag{
				Name:   detailedProvidersMetricsArg,
				Usage:  "Whether to report detailed stats for providers through metrics.",
				Hidden: true,
			},
			&cli.StringFlag{
				Name:  homepageURLArg,
				Usage: "The actual webUI backend to be rendered via iframe.",
				Value: "https://web-ipni.cid.contact/",
			},
		},
		Action: func(c *cli.Context) error {
			ctx, ctxCancel := context.WithCancel(c.Context)
			defer ctxCancel()
			c.Context = ctx

			s, err := newServer(c)
			if err != nil {
				return err
			}

			done := s.Serve()

			var (
				cfgPath  string
				modTime  time.Time
				ticker   *time.Ticker
				timeChan <-chan time.Time
			)
			if configCheckInterval != 0 {
				cfgPath = s.GetCfgBase()
				if cfgPath == "" {
					cfgPath, err = Path("", "")
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

			for {
				select {
				case err := <-done:
					// Ensure we've started the shutdown sequence
					ctxCancel()

					// All errors must be collected to ensure the shutdown sequence is complete
					allErrs := []error{err}
					for err = range done {
						allErrs = append(allErrs, err)
					}

					return errors.Join(allErrs...)

				case <-timeChan:
					// Detect config file changes and reload config if needed.
					var changed bool
					modTime, changed, err = fileChanged(s.GetCfgBase(), modTime)
					if err != nil {
						log.Errorw("Cannot stat config file", "err", err, "path", cfgPath)
						ticker.Stop()
						ticker = nil
						timeChan = nil // disable timeChan from the select statement
						continue
					}
					if changed {
						select {
						case reloadSig <- syscall.SIGHUP:
						default:
						}
					}

				case <-reloadSig:
					err := s.Reload(c)
					if err != nil {
						log.Warnf("couldn't reload servers: %s", err)
					}
				}
			}
		},
	}
	err := app.RunContext(ctx, args)
	return err
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
