package main

import (
	"context"
	"sync"
	"time"
)

type scatterGather[B Backend, R any] struct {
	backends []B
	start    time.Time
	wg       sync.WaitGroup
	out      chan R
	maxWait  time.Duration
}

func (sg *scatterGather[B, R]) scatter(ctx context.Context, forEach func(context.Context, B) (*R, error)) error {
	sg.start = time.Now()
	sg.out = make(chan R, 1)
	for _, backend := range sg.backends {

		if backend.CB() != nil && !backend.CB().Ready() {
			continue
		}

		sg.wg.Add(1)
		go func(target B) {
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
			if target.CB() != nil {
				err = target.CB().Done(cctx, err)
			}
			if err != nil {
				log.Errorw("failed to scatter on target", "target", target.URL().Host, "err", err, "maxWait", sg.maxWait)
				return
			}
			if sout != nil {
				select {
				case <-ctx.Done():
				case sg.out <- *sout:
				}
			}
		}(backend)
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
