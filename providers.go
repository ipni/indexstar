package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-shipyard/indexstar/httpserver"
	"github.com/libp2p/go-libp2p-core/peer"
)

func (s *server) providers(w http.ResponseWriter, r *http.Request) {
	combined := make(chan []model.ProviderInfo)
	wg := sync.WaitGroup{}
	var err error
	_, err = io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		log.Warnw("failed to read original request body", "err", err)
		return
	}

	for _, server := range s.servers {
		wg.Add(1)
		go func(server *url.URL) {
			defer wg.Done()

			// Copy the URL from original request and override host/schema to point
			// to the server.
			endpoint := *r.URL
			endpoint.Host = server.Host
			endpoint.Scheme = server.Scheme
			log := log.With("backend", endpoint)

			// If body in original request existed, make a reader for it.
			req, err := http.NewRequest(r.Method, endpoint.String(), nil)
			if err != nil {
				log.Warnw("failed to construct query", "err", err)
				return
			}
			req.Header.Set("X-Forwarded-Host", r.Host)
			resp, err := s.Client.Do(req)
			if err != nil {
				log.Warnw("failed query", "err", err)
				return
			}
			defer resp.Body.Close()
			dec := json.NewDecoder(resp.Body)
			var providers []model.ProviderInfo
			err = dec.Decode(&providers)
			if err != nil {
				log.Warnw("failed backend read", "err", err)
				return
			}
			if resp.StatusCode == http.StatusOK {
				combined <- providers
			} else {
				log.Warnw("failed backend unmarshal", "err", err)
			}
		}(server)
	}
	go func() {
		wg.Wait()
		close(combined)
	}()

	resp := make(map[peer.ID]model.ProviderInfo)
	for prov := range combined {
		for _, p := range prov {
			if curr, ok := resp[p.AddrInfo.ID]; ok {
				clt, e1 := time.Parse(time.RFC3339, curr.LastAdvertisementTime)
				plt, e2 := time.Parse(time.RFC3339, p.LastAdvertisementTime)
				if e1 == nil && e2 == nil && clt.Before(plt) {
					resp[p.AddrInfo.ID] = p
				}
				continue
			}
			resp[p.AddrInfo.ID] = p
		}
	}

	// Write out combined.
	// Note that /providers never returns 404. Instead, when there are no providers,
	// an empty JSON array is returned.
	out := make([]model.ProviderInfo, 0, len(resp))
	for _, a := range resp {
		out = append(out, a)
	}
	outData, err := json.Marshal(out)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, outData)
}

// provider returns most recent state of a single provider.
func (s *server) provider(w http.ResponseWriter, r *http.Request) {
	combined := make(chan model.ProviderInfo)
	wg := sync.WaitGroup{}
	var err error
	_, err = io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		log.Warnw("failed to read original request body", "err", err)
		return
	}

	for _, server := range s.servers {
		wg.Add(1)
		go func(server *url.URL) {
			defer wg.Done()

			// Copy the URL from original request and override host/schema to point
			// to the server.
			endpoint := *r.URL
			endpoint.Host = server.Host
			endpoint.Scheme = server.Scheme
			log := log.With("backend", endpoint)

			req, err := http.NewRequest(r.Method, endpoint.String(), nil)
			if err != nil {
				log.Warnw("failed to construct query", "err", err)
				return
			}
			req.Header.Set("X-Forwarded-Host", r.Host)
			resp, err := s.Client.Do(req)
			if err != nil {
				log.Warnw("failed query", "err", err)
				return
			}
			defer resp.Body.Close()
			dec := json.NewDecoder(resp.Body)
			var provider model.ProviderInfo
			err = dec.Decode(&provider)
			if err != nil {
				log.Warnw("failed backend read", "err", err)
				return
			}
			if resp.StatusCode == http.StatusOK {
				combined <- provider
			} else {
				log.Warnw("failed backend unmarshal", "err", err)
			}
		}(server)
	}
	go func() {
		wg.Wait()
		close(combined)
	}()

	resp := model.ProviderInfo{}
	for p := range combined {
		if resp.LastAdvertisementTime != "" {
			clt, e1 := time.Parse(time.RFC3339, resp.LastAdvertisementTime)
			plt, e2 := time.Parse(time.RFC3339, p.LastAdvertisementTime)
			if e1 == nil && e2 == nil && clt.Before(plt) {
				resp = p
			}
			continue
		}
		resp = p
	}

	// Write out combined.
	outData, err := json.Marshal(resp)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, outData)
}
