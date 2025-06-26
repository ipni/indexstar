package main

import (
	"sort"
	"sync"
	"sync/atomic"
)

type ProviderMap struct {
	cardinality int
	providers   map[string]*atomic.Int64
	lock        sync.RWMutex
}

func NewProviderMap(cardinality int) *ProviderMap {
	return &ProviderMap{
		cardinality: cardinality,
		providers:   make(map[string]*atomic.Int64),
	}
}

func (pm *ProviderMap) Add(provider string) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	var c *atomic.Int64
	var exists bool

	if c, exists = pm.providers[provider]; !exists {
		// slow case..
		pm.lock.RUnlock()
		pm.lock.Lock()
		if c, exists = pm.providers[provider]; !exists { // Double-check
			newP := atomic.Int64{}
			pm.providers[provider] = &newP
			c = &newP
		}
		pm.lock.Unlock()
		pm.lock.RLock()
	}

	c.Add(1)
}

type ProviderCount struct {
	Provider string
	Count    int64
}

func (pm *ProviderMap) Top() []ProviderCount {
	n := pm.cardinality
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	pairs := make([]ProviderCount, 0, len(pm.providers))
	for provider, count := range pm.providers {
		pairs = append(pairs, ProviderCount{Provider: provider, Count: count.Load()})
	}

	// Sort pairs by count in descending order
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Count > pairs[j].Count
	})

	if n > len(pairs) {
		n = len(pairs)
	}

	return pairs[0:n]
}
