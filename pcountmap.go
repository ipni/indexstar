package main

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/peer"
)

type ProviderMap struct {
	cardinality int
	providers   map[peer.ID]*atomic.Int64
	lock        sync.RWMutex
}

func NewProviderMap(cardinality int) *ProviderMap {
	return &ProviderMap{
		cardinality: cardinality,
		providers:   make(map[peer.ID]*atomic.Int64),
	}
}

func (pm *ProviderMap) Add(provider peer.ID) {
	pm.lock.RLock()
	c, exists := pm.providers[provider]
	pm.lock.RUnlock()

	if !exists {
		// slow case..
		pm.lock.Lock()
		if c, exists = pm.providers[provider]; !exists { // Double-check
			newP := atomic.Int64{}
			pm.providers[provider] = &newP
			c = &newP
		}
		pm.lock.Unlock()
	}

	c.Add(1)
}

type ProviderCount struct {
	Provider peer.ID
	Count    int64
}

func (pm *ProviderMap) Top() []ProviderCount {
	pairs := pm.gatherProviderCounts()

	// Sort pairs by count in descending order
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Count > pairs[j].Count
	})

	n := min(pm.cardinality, len(pairs))

	return pairs[0:n]
}

func (pm *ProviderMap) gatherProviderCounts() []ProviderCount {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	pairs := make([]ProviderCount, 0, len(pm.providers))
	for provider, count := range pm.providers {
		pairs = append(pairs, ProviderCount{Provider: provider, Count: count.Load()})
	}

	return pairs
}
