package main

import (
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestProviderMapConcurrentAddSameProvider(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			goroutines       = 64
			addsPerGoroutine = 250
		)

		pm := NewProviderMap(1)
		provider := randomPeerID(t)

		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				for range addsPerGoroutine {
					pm.Add(provider)
				}
			})
		}

		wg.Wait()
		synctest.Wait()

		top := pm.Top()
		require.Len(t, top, 1)
		require.Equal(t, provider, top[0].Provider)
		require.Equal(t, int64(goroutines*addsPerGoroutine), top[0].Count)
	})
}

func TestProviderMapConcurrentAddDistinctProviders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			providerCount    = 64
			goroutines       = 64
			addsPerGoroutine = 100
		)

		pm := NewProviderMap(providerCount)
		providers := make([]peer.ID, providerCount)
		for i := range providers {
			providers[i] = randomPeerID(t)
		}

		var wg sync.WaitGroup
		for g := range goroutines {
			provider := providers[g]
			wg.Go(func() {
				for range addsPerGoroutine {
					pm.Add(provider)
				}
			})
		}

		wg.Wait()
		synctest.Wait()

		counts := providerCountMap(pm, providerCount)
		require.Len(t, counts, providerCount)

		for i, provider := range providers {
			require.Equal(t, int64(addsPerGoroutine), counts[provider], "provider %d", i)
		}
	})
}

func TestProviderMapConcurrentAddOverlappingProviders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			providerCount    = 32
			goroutines       = 96
			addsPerGoroutine = 200
		)

		pm := NewProviderMap(providerCount)
		providers := make([]peer.ID, providerCount)
		for i := range providers {
			providers[i] = randomPeerID(t)
		}

		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Go(func() {
				for j := range addsPerGoroutine {
					pm.Add(providers[(g+j)%providerCount])
				}
			})
		}

		wg.Wait()
		synctest.Wait()

		counts := providerCountMap(pm, providerCount)
		require.Len(t, counts, providerCount)

		var total int64
		for _, count := range counts {
			total += count
		}
		require.Equal(t, int64(goroutines*addsPerGoroutine), total)
	})
}

func providerCountMap(pm *ProviderMap, cardinality int) map[peer.ID]int64 {
	orig := pm.cardinality
	pm.cardinality = cardinality
	defer func() { pm.cardinality = orig }()

	counts := make(map[peer.ID]int64, cardinality)
	for _, rcrd := range pm.Top() {
		counts[rcrd.Provider] = rcrd.Count
	}
	return counts
}

func TestProviderMapConcurrentStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	synctest.Test(t, func(t *testing.T) {
		const (
			providerCount = 64
			goroutines    = 128
			operations    = 2000
		)

		pm := NewProviderMap(10)
		providers := make([]peer.ID, providerCount)
		for i := range providers {
			providers[i] = peer.ID(fmt.Appendf(nil, "provider-%d-%s", i, randomPeerID(t)))
		}

		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Go(func() {
				for op := range operations {
					switch op % 3 {
					case 0, 1:
						pm.Add(providers[(g+op)%providerCount])
					case 2:
						_ = pm.Top()
					}
				}
			})
		}

		wg.Wait()
		synctest.Wait()

		counts := providerCountMap(pm, providerCount)
		require.NotEmpty(t, counts)

		top := pm.Top()
		require.Len(t, top, 10)
		for i := 1; i < len(top); i++ {
			require.GreaterOrEqual(t, top[i-1].Count, top[i].Count)
		}
	})
}
