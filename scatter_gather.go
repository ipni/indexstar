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

		var cb *circuitbreaker.CircuitBreaker
		if len(sg.tcb) > i {
			cb = sg.tcb[i]
		}
		if cb != nil && !cb.Ready() {
			continue
		}

		sg.wg.Add(1)
		go func(target T, tcb *circuitbreaker.CircuitBreaker) {
			defer sg.wg.Done()

			select {
			case <-ctx.Done():
				log.Errorw("context is done before completing scatter", "err", ctx.Err())
				return
			default:
			}

			cctx, cancel := context.WithTimeout(ctx, sg.maxWait)
			sout, err := forEach(cctx, target)
			cancel()
			if tcb != nil {
				err = tcb.Done(cctx, err)
			}
			if err != nil {
				log.Errorw("failed to scatter on target", "target", target, "err", err, "maxWait", sg.maxWait)
				return
			}
			if sout != nil {
				select {
				case <-ctx.Done():
				case sg.out <- *sout:
				}
			}
		}(t, cb)
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
			log.Debugw("Completed scatter gather", "elapsed", time.Since(sg.start))
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-sg.out:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case gout <- r:
					continue
				}
			}
		}
	}()
	return gout
}
