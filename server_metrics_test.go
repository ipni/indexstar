package main

import (
	"testing"

	"github.com/ipni/indexstar/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestUpdateTopProvidersReportsTopOnlyAndPrunesStale(t *testing.T) {
	srv := &server{
		pcounts: NewProviderMap(2),
	}

	invalidUTF8PeerID := peer.ID(string([]byte{0xff, 0xfe, 0xfd}))
	providerA := randomPeerID(t)
	providerB := randomPeerID(t)
	providerC := randomPeerID(t)

	addProviderCount := func(provider peer.ID, count int) {
		for range count {
			srv.pcounts.Add(provider)
		}
	}

	// First snapshot: only the top 2 providers should be emitted.
	addProviderCount(providerA, 10)
	addProviderCount(invalidUTF8PeerID, 9)
	addProviderCount(providerB, 8)
	addProviderCount(providerC, 1)

	srv.updateTopProviders()

	got := readTopProviderMetrics(t)
	require.Len(t, got, 2)
	require.Equal(t, 10.0, got[providerA.String()])
	require.Equal(t, 9.0, got[invalidUTF8PeerID.String()])
	require.NotContains(t, got, providerB.String())
	require.NotContains(t, got, providerC.String())

	// Second snapshot: old top providers should be removed when top list changes.
	addProviderCount(providerB, 20)
	addProviderCount(providerC, 30)

	srv.updateTopProviders()

	got = readTopProviderMetrics(t)
	require.Len(t, got, 2)
	require.Equal(t, 31.0, got[providerC.String()])
	require.Equal(t, 28.0, got[providerB.String()])
	require.NotContains(t, got, providerA.String())
	require.NotContains(t, got, invalidUTF8PeerID.String())
}

func readTopProviderMetrics(t *testing.T) map[string]float64 {
	t.Helper()

	ch := make(chan prometheus.Metric, 256)
	metrics.TopProvider.Collect(ch)
	close(ch)

	values := make(map[string]float64)
	for metric := range ch {
		pb := &dto.Metric{}
		require.NoError(t, metric.Write(pb))
		require.NotNil(t, pb.Gauge)

		var providerLabel string
		for _, label := range pb.Label {
			if label.GetName() == metrics.LabelProvider {
				providerLabel = label.GetValue()
				break
			}
		}
		require.NotEmpty(t, providerLabel)
		values[providerLabel] = pb.Gauge.GetValue()
	}

	return values
}
