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
				Name:  "listen",
				Usage: "listen address",
				Value: ":8080",
			},
			&cli.StringSliceFlag{
				Name:  "backends",
				Usage: "backends to use",
				Value: cli.NewStringSlice("https://cid.contact/"),
			},
		},
		Action: func(c *cli.Context) error {
			exit := make(chan os.Signal, 1)
			signal.Notify(exit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

			s, err := NewServer(c)
			if err != nil {
				return err
			}
			select {
			case _ = <-exit:
				return nil
			case err := <-s.Serve():
				return err
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
