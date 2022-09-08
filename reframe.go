package main

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sync"

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
	httpClient := http.Client{
		Timeout:   config.Reframe.HttpClientTimeout,
		Transport: reframeRoundTripper(),
	}

	clients := make([]drclient.DelegatedRoutingClient, 0, len(backends))
	for _, b := range backends {
		endpoint := b.JoinPath("reframe").String()
		q, err := drproto.New_DelegatedRouting_Client(endpoint, drproto.DelegatedRouting_Client_WithHTTPClient(&httpClient))
		if err != nil {
			return nil, err
		}
		clients = append(clients, drclient.NewClient(q))
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
	backends []drclient.DelegatedRoutingClient
}

func (x *ReframeService) FindProviders(ctx context.Context, key cid.Cid) (<-chan drclient.FindProvidersAsyncResult, error) {
	out := make(chan drclient.FindProvidersAsyncResult)
	wg := sync.WaitGroup{}
	var lastErr error
	n := 0
	for _, c := range x.backends {
		childout, err := c.FindProvidersAsync(ctx, key)
		if err != nil {
			lastErr = err
			continue
		}
		n++
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case r, ok := <-childout:
					if !ok {
						return
					}
					out <- r
				}
			}
		}()
	}
	if n == 0 {
		close(out)
		return nil, lastErr
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}

func (x *ReframeService) GetIPNS(context.Context, []byte) (<-chan drclient.GetIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeService) PutIPNS(context.Context, []byte, []byte) (<-chan drclient.PutIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}
