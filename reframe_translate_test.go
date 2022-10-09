package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"testing"

	finderhttpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	"github.com/ipfs/go-cid"
	drp "github.com/ipfs/go-delegated-routing/gen/proto"
	"github.com/mercari/go-circuitbreaker"
	"github.com/stretchr/testify/require"
)

func doServe(ctx context.Context, bound net.Listener) {
	surls := make([]*url.URL, 0, 1)
	surl, err := url.Parse("https://cid.contact/")
	if err != nil {
		return
	}
	surls = append(surls, surl)

	b2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}

	s := server{
		Context:          ctx,
		Client:           *http.DefaultClient,
		Listener:         bound,
		metricsListener:  b2,
		servers:          surls,
		serverCallers:    []*circuitbreaker.CircuitBreaker{circuitbreaker.New()},
		base:             httputil.NewSingleHostReverseProxy(surls[0]),
		translateReframe: true,
	}
	s.Serve()
}
func TestReframe_IsReachable(t *testing.T) {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)
	bound, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go doServe(ctx, bound)
	defer cancel()
	base := "http://" + bound.Addr().String()
	reframeUrl := base + "/reframe"
	indexerClient, err := drp.New_DelegatedRouting_Client(reframeUrl)
	require.NoError(t, err)

	c, err := cid.Decode("bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy")
	require.NoError(t, err)

	providers, err := indexerClient.FindProviders(ctx, &drp.FindProvidersRequest{
		Key: drp.LinkToAny(c),
	})
	if err != nil {
		t.Log(err.Error())
	} else {
		t.Logf("OK, found %d", len(providers))
		for _, provider := range providers {
			for _, p := range provider.Providers {
				t.Log(p.ProviderNode.Peer.ID)
			}
		}
		client, err := finderhttpclient.New(base)
		require.NoError(t, err)
		find, err := client.Find(ctx, c.Hash())
		require.NoError(t, err)
		t.Log("found via mh finder", len(find.MultihashResults))
		require.Equal(t, len(find.MultihashResults), len(providers))
	}
}
