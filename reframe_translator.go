package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/ipfs/go-cid"
	drclient "github.com/ipfs/go-delegated-routing/client"
	drserver "github.com/ipfs/go-delegated-routing/server"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

type findFunc func(ctx context.Context, method string, req *url.URL, body []byte) (int, []byte)

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

	req, err := url.Parse("http://localhost-reframetranslator/multihash/" + key.Hash().String())
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)

		respStatus, respJSON := x.findBackend(ctx, "GET", req, []byte{})
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
			for _, prov := range parsed.MultihashResults[0].ProviderResults {
				if isBitswapMetadata(prov.Metadata) {
					outMsg.AddrInfo = append(outMsg.AddrInfo, prov.Provider)
				}
			}
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
	return bytes.Equal(meta, BitswapMetadataBytes)
}

func (x *ReframeTranslatorService) GetIPNS(context.Context, []byte) (<-chan drclient.GetIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeTranslatorService) PutIPNS(context.Context, []byte, []byte) (<-chan drclient.PutIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}
