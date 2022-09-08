package main

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"sync"

	finderhttpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	"github.com/filecoin-project/storetheindex/api/v0/httpclient"
	"github.com/ipfs/go-cid"
	drclient "github.com/ipfs/go-delegated-routing/client"
	drserver "github.com/ipfs/go-delegated-routing/server"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

func NewReframeTranslatorHTTPHandler(backends []*url.URL) (http.HandlerFunc, error) {
	svc, err := NewReframeTranslatorService(backends)
	if err != nil {
		return nil, err
	}
	return drserver.DelegatedRoutingAsyncHandler(svc), nil
}

func NewReframeTranslatorService(backends []*url.URL) (*ReframeTranslatorService, error) {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = config.Reframe.MaxIdleConns
	t.MaxConnsPerHost = config.Reframe.MaxConnsPerHost
	t.MaxIdleConnsPerHost = config.Reframe.MaxIdleConnsPerHost

	httpClient := http.Client{
		Timeout:   config.Reframe.HttpClientTimeout,
		Transport: t,
	}

	clients := make([]*finderhttpclient.Client, 0, len(backends))
	for _, b := range backends {
		bc, err := finderhttpclient.New(b.String(), httpclient.WithClient(&httpClient))
		if err != nil {
			return nil, err
		}
		clients = append(clients, bc)
	}

	return &ReframeTranslatorService{clients}, nil
}

type ReframeTranslatorService struct {
	backends []*finderhttpclient.Client
}

func (x *ReframeTranslatorService) FindProviders(ctx context.Context, key cid.Cid) (<-chan drclient.FindProvidersAsyncResult, error) {
	out := make(chan drclient.FindProvidersAsyncResult)
	wg := sync.WaitGroup{}
	mh := key.Hash()
	for _, c := range x.backends {
		wg.Add(1)
		go func(c *finderhttpclient.Client, mh multihash.Multihash) {
			defer wg.Done()

			reframeResult := drclient.FindProvidersAsyncResult{}

			results, err := c.Find(ctx, mh)
			if err != nil {
				reframeResult.Err = err
			} else if results != nil {
				for _, mhr := range results.MultihashResults {
					for _, pr := range mhr.ProviderResults {
						if isBitswapMetadata(pr.Metadata) {
							reframeResult.AddrInfo = append(reframeResult.AddrInfo, pr.Provider)
						}
					}
				}
			}

			select {
			case out <- reframeResult:
				return
			case <-ctx.Done():
				return
			}
		}(c, mh)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}

var BitswapMetadataBytes = varint.ToUvarint(uint64(multicodec.TransportBitswap))

func isBitswapMetadata(meta []byte) bool {
	return bytes.Equal(meta, BitswapMetadataBytes)
}

func (x *ReframeTranslatorService) GetIPNS(context.Context, []byte) (<-chan drclient.GetIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeTranslatorService) PutIPNS(context.Context, []byte, []byte) (<-chan drclient.PutIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}
