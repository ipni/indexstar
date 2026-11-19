package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/pcache"
	"github.com/libp2p/go-libp2p/core/peer"
)

// minStartupRetry is the shortest wait between attempts to fetch providers at
// startup
const minStartupRetry = time.Second

// prefetchedSource is a provider source whose first FetchAll returns a provider
// list fetched earlier. Later calls go to the wrapped source.
type prefetchedSource struct {
	pcache.ProviderSource
	first atomic.Pointer[[]*model.ProviderInfo]
}

func (s *prefetchedSource) FetchAll(ctx context.Context) ([]*model.ProviderInfo, error) {
	if infos := s.first.Swap(nil); infos != nil {
		return *infos, nil
	}
	return s.ProviderSource.FetchAll(ctx)
}

// checkProviderBackends fetches the provider list from every providers backend
// before the provider cache is created. The backends are fetched concurrently.
// A backend that fails is retried after the retry interval, at least
// minStartupRetry, up to maxAttempts attempts in total. It returns an error
// naming each backend that still fails, so that the server does not start with
// providers missing from its cache.
//
// On success it returns the sources wrapped so that the first FetchAll of each
// returns the list fetched here. The provider cache preload makes that first
// call, so the cache starts with these lists without fetching them again.
func checkProviderBackends(ctx context.Context, sources []pcache.ProviderSource, maxAttempts int, retry time.Duration) ([]pcache.ProviderSource, error) {
	maxAttempts = max(maxAttempts, 1)
	retry = max(retry, minStartupRetry)
	prefetched := make([]pcache.ProviderSource, len(sources))
	pending := make([]*prefetchedSource, len(sources))
	for i, src := range sources {
		pending[i] = &prefetchedSource{ProviderSource: src}
		prefetched[i] = pending[i]
	}
	for attempt := 1; ; attempt++ {
		// Fetch from the pending backends concurrently, so that an attempt
		// takes as long as the slowest backend
		fetchErrs := make([]error, len(pending))
		var wg sync.WaitGroup
		for i, src := range pending {
			wg.Go(func() {
				var infos []*model.ProviderInfo
				infos, fetchErrs[i] = src.ProviderSource.FetchAll(ctx)
				if fetchErrs[i] == nil {
					src.first.Store(&infos)
				}
			})
		}
		wg.Wait()

		var failed []*prefetchedSource
		var errs []error
		for i, err := range fetchErrs {
			if err != nil {
				failed = append(failed, pending[i])
				errs = append(errs, fmt.Errorf("%s: %w", pending[i], err))
			}
		}
		if len(failed) == 0 {
			// Fetched from all the providers.
			break
		}

		if attempt == maxAttempts {
			return nil, fmt.Errorf(
				"cannot fetch providers (attempts: %d): %w",
				maxAttempts, errors.Join(errs...),
			)
		}
		log.Warnw(
			"Cannot fetch providers from providers backends, retrying",
			"attempt", attempt,
			"maxAttempts", maxAttempts,
			"retryIn", retry,
			"err", errors.Join(errs...),
		)

		// Fetch from remaining providers.
		pending = failed

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retry):
		}
	}
	return prefetched, nil
}

func (s *server) providers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	pinfos := s.pcache.List()

	// Write out combined.
	//
	// Note that /providers never returns 404. Instead, when there are no
	// providers, an empty JSON array is returned.
	outData, err := json.Marshal(pinfos)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	writeJsonResponse(w, http.StatusOK, outData)
}

// provider returns most recent state of a single provider.
func (s *server) provider(w http.ResponseWriter, r *http.Request) {
	pid, err := peer.Decode(path.Base(r.URL.Path))
	if err != nil {
		log.Warnw("bad provider ID", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	pinfo, err := s.pcache.Get(r.Context(), pid)
	if err != nil {
		log.Warnw("count not get provider information", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	if pinfo == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	outData, err := json.Marshal(pinfo)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	writeJsonResponse(w, http.StatusOK, outData)
}
