package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/ipfs/go-cid"
	drclient "github.com/ipfs/go-delegated-routing/client"
	drserver "github.com/ipfs/go-delegated-routing/server"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

type findFunc func(ctx context.Context, method, source string, req *url.URL, body []byte) (int, []byte)

func NewReframeTranslatorHTTPHandler(backend findFunc) (http.HandlerFunc, error) {
	svc, err := NewReframeTranslatorService(backend)
	if err != nil {
		return nil, err
	}
	return drserver.DelegatedRoutingAsyncHandler(svc), nil
}

func NewReframeTranslatorService(backend findFunc) (*ReframeTranslatorService, error) {
	return &ReframeTranslatorService{backend}, nil
}

type ReframeTranslatorService struct {
	findBackend findFunc
}

func (x *ReframeTranslatorService) FindProviders(ctx context.Context, key cid.Cid) (<-chan drclient.FindProvidersAsyncResult, error) {
	out := make(chan drclient.FindProvidersAsyncResult)

	req, err := url.Parse("http://localhost-reframetranslator/multihash/" + key.Hash().B58String())
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)

		respStatus, respJSON := x.findBackend(ctx, "GET", findMethodReframe, req, []byte{})
		if respStatus == http.StatusNotFound {
			// no responses.
			return
		}

		outMsg := drclient.FindProvidersAsyncResult{}

		if respStatus != http.StatusOK {
			// error
			outMsg.Err = fmt.Errorf("status %d", respStatus)
			goto output
		}

		{
			var parsed model.FindResponse
			if err := json.Unmarshal(respJSON, &parsed); err != nil {
				outMsg.Err = err
				goto output
			}

			if len(parsed.MultihashResults) != 1 {
				outMsg.Err = fmt.Errorf("unexpected number of multihashes: %d", len(parsed.MultihashResults))
				goto output
			}
			seen := make(map[peer.ID]struct{})
			for _, prov := range parsed.MultihashResults[0].ProviderResults {
				if _, ok := seen[prov.Provider.ID]; !ok && isBitswapMetadata(prov.Metadata) {
					outMsg.AddrInfo = append(outMsg.AddrInfo, *prov.Provider)
					seen[prov.Provider.ID] = struct{}{}
				}
			}
			goto output
		}

	output:
		select {
		case out <- outMsg:
			return
		case <-ctx.Done():
			return
		}
	}()

	return out, nil
}

var BitswapMetadataBytes = varint.ToUvarint(uint64(multicodec.TransportBitswap))

func isBitswapMetadata(meta []byte) bool {
	// Metadata must be sorted according to the specification; see:
	// - https://github.com/filecoin-project/index-provider/blob/main/metadata/metadata.go#L143
	// This implies that if it includes Bitswap, its codec must appear at the beginning
	// of the metadata value. Hence, bytes.HasPrefix.
	return bytes.HasPrefix(meta, BitswapMetadataBytes)
}

func (x *ReframeTranslatorService) GetIPNS(context.Context, []byte) (<-chan drclient.GetIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeTranslatorService) PutIPNS(context.Context, []byte, []byte) (<-chan drclient.PutIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeTranslatorService) Provide(context.Context, *drclient.ProvideRequest) (<-chan drclient.ProvideAsyncResult, error) {
	return nil, routing.ErrNotSupported
}
