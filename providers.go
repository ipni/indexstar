package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/ipni/go-libipni/pcache"
	"github.com/libp2p/go-libp2p/core/peer"
)

// checkProviderBackends fetches the provider list from every providers backend
// before the provider cache is created. The backends are fetched concurrently.
// A backend that fails is retried after the retry interval, up to maxAttempts
// attempts in total. It returns an error naming each backend that still fails,
// so that the server does not start with providers missing from its cache.
func checkProviderBackends(ctx context.Context, sources []pcache.ProviderSource, maxAttempts int, retry time.Duration) error {
	maxAttempts = max(maxAttempts, 1)
	pending := sources
	for attempt := 1; ; attempt++ {
		// Fetch from the pending backends concurrently, so that an attempt
		// takes as long as the slowest backend
		fetchErrs := make([]error, len(pending))
		var wg sync.WaitGroup
		for i, src := range pending {
			wg.Go(func() {
				_, fetchErrs[i] = src.FetchAll(ctx)
			})
		}
		wg.Wait()

		var failed []pcache.ProviderSource
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
			return fmt.Errorf(
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
			return ctx.Err()
		case <-time.After(retry):
		}
	}
	return nil
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
