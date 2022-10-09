package main

import (
	"context"
	"sync"
	"time"

	"github.com/mercari/go-circuitbreaker"
)

type scatterGather[T, R any] struct {
	targets []T
	tcb     []*circuitbreaker.CircuitBreaker
	start   time.Time
	wg      sync.WaitGroup
	out     chan R
	maxWait time.Duration
}

func (sg *scatterGather[T, R]) scatter(ctx context.Context, forEach func(context.Context, T) (*R, error)) error {
	sg.start = time.Now()
	sg.out = make(chan R, 1)
	for i, t := range sg.targets {
		if (len(sg.tcb) > 0) && !sg.tcb[i].Ready() {
			continue
		}

		sg.wg.Add(1)
		go func(target T, i int) {
			defer sg.wg.Done()

			select {
			case <-ctx.Done():
				log.Errorw("context is done before completing scatter", "err", ctx.Err())
				return
			default:
			}

			cctx, cncl := context.WithTimeout(ctx, sg.maxWait)
			sout, err := forEach(cctx, target)
			cncl()
			if len(sg.tcb) > 0 {
				err = sg.tcb[i].Done(ctx, err)
			}
			if err != nil {
				log.Errorw("failed to scatter on target", "target", target, "err", err)
				return
			}

			if sout != nil {
				if ctx.Err() == nil {
					select {
					case <-ctx.Done():
						return
					case sg.out <- *sout:
						return
					}
				}
			}
		}(t, i)
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

		for r := range sg.out {
			select {
			case <-ctx.Done():
				continue
			case gout <- r:
				continue
			}
		}
	}()
	return gout
}
