package main

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	drclient "github.com/ipfs/go-delegated-routing/client"
	drproto "github.com/ipfs/go-delegated-routing/gen/proto"
	drserver "github.com/ipfs/go-delegated-routing/server"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
)

func NewReframeHTTPHandler(backends []*url.URL) (http.HandlerFunc, error) {
	svc, err := NewReframeService(backends)
	if err != nil {
		return nil, err
	}
	return drserver.DelegatedRoutingAsyncHandler(svc), nil
}

func NewReframeService(backends []*url.URL) (*ReframeService, error) {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = config.Reframe.MaxIdleConns
	t.MaxConnsPerHost = config.Reframe.MaxConnsPerHost
	t.MaxIdleConnsPerHost = config.Reframe.MaxIdleConnsPerHost
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout:   config.Reframe.DialerTimeout,
			KeepAlive: config.Reframe.DialerKeepAlive,
		}
		return dialer.DialContext(ctx, network, addr)
	}

	httpClient := http.Client{
		Transport: t,
		Timeout:   config.Reframe.HttpClientTimeout,
	}

	clients := make([]*backendDelegatedRoutingClient, 0, len(backends))
	for _, b := range backends {
		endpoint := b.JoinPath("reframe").String()
		q, err := drproto.New_DelegatedRouting_Client(endpoint, drproto.DelegatedRouting_Client_WithHTTPClient(&httpClient))
		if err != nil {
			return nil, err
		}
		clients = append(clients, &backendDelegatedRoutingClient{
			DelegatedRoutingClient: drclient.NewClient(q),
			url:                    b,
		})
	}
	return &ReframeService{clients}, nil
}

type ReframeService struct {
	backends []*backendDelegatedRoutingClient
}

type backendDelegatedRoutingClient struct {
	drclient.DelegatedRoutingClient
	url *url.URL
}

func (x *ReframeService) FindProviders(ctx context.Context, key cid.Cid) (<-chan drclient.FindProvidersAsyncResult, error) {
	startTime := time.Now()
	cout := make(chan drclient.FindProvidersAsyncResult, 1)
	var wg sync.WaitGroup
	for _, b := range x.backends {
		wg.Add(1)
		go func(backend *backendDelegatedRoutingClient) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				log.Errorw("context is done before finishing call instantiation", "url", backend.url, "err", ctx.Err())
				return
			default:
			}

			bout, err := backend.FindProvidersAsync(ctx, key)
			if err != nil {
				log.Errorw("failed to instantiate async find on backend", "url", backend.url, "err", err)
				return
			}

			for {
				select {
				case <-ctx.Done():
					return
				case r, ok := <-bout:
					if !ok {
						return
					}
					cout <- r
				}
			}
		}(b)
	}

	go func() {
		defer close(cout)
		wg.Wait()
	}()

	out := make(chan drclient.FindProvidersAsyncResult, 1)
	go func() {
		defer close(out)
		defer func() {
			elapsed := time.Now().Sub(startTime)
			log.Infow("took", "elapsed", elapsed.String())
		}()

		pids := make(map[peer.ID]struct{})
		var rerr error
		ticker := time.NewTicker(config.Reframe.ResultMaxWait)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Infow("time is up; closing off channels", "pid_count", len(pids), "rerr", rerr)
				if len(pids) == 0 {
					var result drclient.FindProvidersAsyncResult
					if rerr != nil {
						log.Warnw("no providers found in time, with error", "err", rerr, "key", key)
						result.Err = rerr
					}
					out <- result
				}
				return
			case r, ok := <-cout:
				if !ok {
					log.Debugw("all done in time", "pid_count", len(pids), "rerr", rerr)
					if len(pids) == 0 {
						var result drclient.FindProvidersAsyncResult
						if rerr != nil {
							log.Warnw("no providers found but error occurred", "err", rerr, "key", key)
							result.Err = rerr
						}
						out <- result
					}
					return
				}
				if r.Err != nil {
					log.Errorw("got error back from backend", "err", r.Err)
					rerr = r.Err
				} else {
					var result drclient.FindProvidersAsyncResult
					for _, ai := range r.AddrInfo {
						// TODO: Improve heuristic of picking which addrinfo to return by
						//       picking the most recently seen provider instead of first
						//       found.
						if _, seen := pids[ai.ID]; seen {
							continue
						}
						pids[ai.ID] = struct{}{}
						result.AddrInfo = append(result.AddrInfo, ai)
					}
					if len(result.AddrInfo) > 0 {
						out <- result
					}
				}
			}
		}
	}()

	return out, nil
}

func (x *ReframeService) GetIPNS(_ context.Context, _ []byte) (<-chan drclient.GetIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeService) PutIPNS(_ context.Context, _ []byte, _ []byte) (<-chan drclient.PutIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}
