package main

import (
	"context"
	"sync"
	"time"
)

type scatterGather[T, R any] struct {
	targets []T
	start   time.Time
	wg      sync.WaitGroup
	out     chan R
	maxWait time.Duration
}

func (sg *scatterGather[T, R]) scatter(ctx context.Context, forEach func(T) (<-chan R, error)) error {
	sg.start = time.Now()
	sg.out = make(chan R, 1)
	for _, t := range sg.targets {
		sg.wg.Add(1)
		go func(target T) {
			defer sg.wg.Done()

			select {
			case <-ctx.Done():
				log.Errorw("context is done before completing scatter", "err", ctx.Err())
				return
			default:
			}

			sout, err := forEach(target)
			if err != nil {
				log.Errorw("failed to scatter on target", "target", target, "err", err)
				return
			}

			for {
				select {
				case <-ctx.Done():
					return
				case r, ok := <-sout:
					if !ok {
						return
					}
					sg.out <- r
				}
			}
		}(t)
	}
	go func() {
		defer close(sg.out)
		sg.wg.Wait()
	}()
	return nil
}

func (sg *scatterGather[_, R]) gather(ctx context.Context) <-chan R {
	gout := make(chan R, 1)
	go func() {
		defer func() {
			close(gout)
			elapsed := time.Since(sg.start)
			log.Debugw("Completed scatter gather", "elapsed", elapsed.String())
		}()

		var rerr error
		ticker := time.NewTicker(sg.maxWait)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Debugw("Maximum scatter gather wait time reached; closing off channels", "rerr", rerr)
				return
			case r, ok := <-sg.out:
				if !ok {
					log.Debugw("Gathered all scatters in time", "rerr", rerr)
					return
				}
				gout <- r
			}
		}
	}()
	return gout
}
