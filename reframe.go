package main

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
	"net/http"
	"net/url"
	"time"

	"github.com/ipfs/go-cid"
	drclient "github.com/ipfs/go-delegated-routing/client"
	drproto "github.com/ipfs/go-delegated-routing/gen/proto"
	drserver "github.com/ipfs/go-delegated-routing/server"
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
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100

	httpClient := http.Client{
		Timeout:   5 * time.Second,
		Transport: t,
	}

	clients := make([]backendDelegatedRoutingClient, 0, len(backends))
	for _, b := range backends {
		endpoint := b.JoinPath("reframe").String()
		q, err := drproto.New_DelegatedRouting_Client(endpoint, drproto.DelegatedRouting_Client_WithHTTPClient(&httpClient))
		if err != nil {
			return nil, err
		}
		clients = append(clients, backendDelegatedRoutingClient{
			DelegatedRoutingClient: drclient.NewClient(q),
			url:                    b,
		})
	}
	return &ReframeService{clients}, nil
}

type ReframeService struct {
	backends []backendDelegatedRoutingClient
}

type backendDelegatedRoutingClient struct {
	drclient.DelegatedRoutingClient
	url *url.URL
}

func (x *ReframeService) FindProviders(ctx context.Context, key cid.Cid) (<-chan drclient.FindProvidersAsyncResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	out := make(chan drclient.FindProvidersAsyncResult, 1)
	cout := make(chan drclient.FindProvidersAsyncResult, 1)

	go func() {
		for _, b := range x.backends {
			bout, err := b.FindProvidersAsync(ctx, key)
			if err != nil {
				log.Errorw("failed to instantiate asyc find on backend", "url", b.url, "err", err)
				continue
			}
			go func() {
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
			}()
		}
	}()

	go func() {
		defer close(out)
		defer close(cout)
		defer cancel()

		pids := make(map[peer.ID]struct{})
		var rerr error
		for {
			select {
			case <-ctx.Done():
				if len(pids) == 0 {
					var result drclient.FindProvidersAsyncResult
					if rerr != nil {
						log.Warnw("no providers found in time, with error", "err", rerr, "key", key)
						result.Err = rerr
					}
					out <- result
				}
				return
			case r := <-cout:
				if r.Err != nil {
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
