package main

import (
	"context"
	"net/http"
	"net/url"
	"sync"
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
		Timeout:   10 * time.Second,
		Transport: t,
	}

	clients := make([]drclient.DelegatedRoutingClient, 0, len(backends))
	for _, b := range backends {
		b.Path += "/reframe"
		endpoint := b.String()
		q, err := drproto.New_DelegatedRouting_Client(endpoint, drproto.DelegatedRouting_Client_WithHTTPClient(&httpClient))
		if err != nil {
			return nil, err
		}
		clients = append(clients, drclient.NewClient(q))
	}
	return &ReframeService{clients}, nil
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
			for r := range childout {
				out <- r
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

func (x *ReframeService) GetIPNS(ctx context.Context, id []byte) (<-chan drclient.GetIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeService) PutIPNS(ctx context.Context, id []byte, record []byte) (<-chan drclient.PutIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}
