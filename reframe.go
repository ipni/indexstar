package main

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"path"

	"github.com/ipfs/go-cid"
	drclient "github.com/ipfs/go-delegated-routing/client"
	drproto "github.com/ipfs/go-delegated-routing/gen/proto"
	drserver "github.com/ipfs/go-delegated-routing/server"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

func NewReframeHTTPHandler(backends []*url.URL) (http.HandlerFunc, error) {
	svc, err := NewReframeService(backends)
	if err != nil {
		return nil, err
	}
	return drserver.DelegatedRoutingAsyncHandler(svc), nil
}

func NewReframeService(backends []*url.URL) (*ReframeService, error) {
	httpClient := http.Client{
		Timeout:   config.Reframe.HttpClientTimeout,
		Transport: reframeRoundTripper(),
	}

	clients := make([]*backendDelegatedRoutingClient, 0, len(backends))
	for _, b := range backends {
		// TODO: replace with URL.JoinPath once upgraded to go 1.19
		endpoint := path.Join(b.String(), "reframe")
		q, err := drproto.New_DelegatedRouting_Client(endpoint, drproto.DelegatedRouting_Client_WithHTTPClient(&httpClient))
		if err != nil {
			return nil, err
		}
		drc, err := drclient.NewClient(q, nil, nil)
		if err != nil {
			return nil, err
		}
		clients = append(clients, &backendDelegatedRoutingClient{
			DelegatedRoutingClient: drc,
			url:                    b,
		})
	}
	return &ReframeService{clients}, nil
}

func reframeRoundTripper() *http.Transport {
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
	return t
}

type ReframeService struct {
	backends []*backendDelegatedRoutingClient
}

type backendDelegatedRoutingClient struct {
	drclient.DelegatedRoutingClient
	url *url.URL
}

func (x *ReframeService) FindProviders(ctx context.Context, key cid.Cid) (<-chan drclient.FindProvidersAsyncResult, error) {
	sg := &scatterGather[*backendDelegatedRoutingClient, drclient.FindProvidersAsyncResult]{
		targets: x.backends,
		maxWait: config.Reframe.ResultMaxWait,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := sg.scatter(ctx, func(cctx context.Context, b *backendDelegatedRoutingClient) (*drclient.FindProvidersAsyncResult, error) {
		ch, err := b.FindProvidersAsync(cctx, key)
		if err != nil {
			return nil, err
		}

		res := drclient.FindProvidersAsyncResult{}
		for r := range ch {
			res.AddrInfo = append(res.AddrInfo, r.AddrInfo...)
			if r.Err != nil {
				res.Err = r.Err
			}
		}
		// don't return both results and a partial-error.
		if len(res.AddrInfo) != 0 {
			res.Err = nil
		}
		return &res, nil
	}); err != nil {
		return nil, err
	}

	out := make(chan drclient.FindProvidersAsyncResult, 1)
	var lastErr error
	go func() {
		defer close(out)

		// Aggregate any results using the following logic:
		//
		//  * Wait at most config.Reframe.ResultMaxWait for a result from backend.
		//  * Only return a single result per peer ID picking the first one found.
		//  * Do not return error if at least one provider is found.
		//  * Return the last observed error if no providers are found.
		//  * If no providers are found and no error has occurred return an empty
		//    reframe result.
		pids := make(map[peer.ID]struct{})
		for r := range sg.gather(ctx) {
			if r.Err != nil {
				lastErr = r.Err
				continue
			}

			var result drclient.FindProvidersAsyncResult
			for _, ai := range r.AddrInfo {
				// TODO: Improve heuristic of picking which addrinfo to return by
				//       picking the most recently seen provider instead of first
				//       found. /provider tells us the last seen timestamp.
				if _, seen := pids[ai.ID]; seen {
					continue
				}
				pids[ai.ID] = struct{}{}
				result.AddrInfo = append(result.AddrInfo, ai)
			}
			if len(result.AddrInfo) > 0 {
				select {
				case <-ctx.Done():
					return
				case out <- result:
				}
			}
		}

		// If nothing is found then return the last returned error, if any.
		if len(pids) == 0 && lastErr != nil {
			select {
			case <-ctx.Done():
				return
			case out <- drclient.FindProvidersAsyncResult{Err: lastErr}:
			}
		}
	}()

	return out, nil
}

func (x *ReframeService) GetIPNS(context.Context, []byte) (<-chan drclient.GetIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeService) PutIPNS(context.Context, []byte, []byte) (<-chan drclient.PutIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeService) Provide(context.Context, *drclient.ProvideRequest) (<-chan drclient.ProvideAsyncResult, error) {
	return nil, routing.ErrNotSupported
}
