package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ipni/indexstar/httpserver"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (s *server) providers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
		return
	}

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
			log := log.With("backend", endpoint.Host)

			// If body in original request existed, make a reader for it.
			req, err := http.NewRequest(r.Method, endpoint.String(), nil)
			if err != nil {
				log.Warnw("failed to construct query", "err", err)
				return
			}
			req.Header.Set("X-Forwarded-Host", r.Host)
			req.Header.Set("Accept", mediaTypeJson)
			resp, err := s.Client.Do(req)
			if err != nil {
				log.Warnw("failed query", "err", err)
				return
			}

			defer resp.Body.Close()
			log = log.With("status", resp.StatusCode)
			switch resp.StatusCode {
			case http.StatusOK:
				dec := json.NewDecoder(resp.Body)
				var providers []model.ProviderInfo
				if err := dec.Decode(&providers); err != nil {
					log.Warnw("failed backend read", "err", err)
					return
				}
				combined <- providers
			case http.StatusNotFound, http.StatusNotImplemented:
				log.Debug("no providers")
			default:
				log.Warn("unexpected response while getting providers")
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
			log := log.With("backend", endpoint.Host)

			req, err := http.NewRequest(r.Method, endpoint.String(), nil)
			if err != nil {
				log.Warnw("failed to construct query", "err", err)
				return
			}
			req.Header.Set("X-Forwarded-Host", r.Host)
			req.Header.Set("Accept", mediaTypeJson)
			resp, err := s.Client.Do(req)
			if err != nil {
				log.Warnw("failed query", "err", err)
				return
			}
			defer resp.Body.Close()
			log = log.With("status", resp.StatusCode)
			switch resp.StatusCode {
			case http.StatusOK:
				dec := json.NewDecoder(resp.Body)
				var provider model.ProviderInfo
				if err = dec.Decode(&provider); err != nil {
					log.Warnw("failed backend read", "err", err)
					return
				}
				combined <- provider
			case http.StatusNotFound, http.StatusNotImplemented:
				log.Debug("no provider")
			default:
				log.Warn("unexpected response while getting provider")
			}
		}(server)
	}
	go func() {
		wg.Wait()
		close(combined)
	}()

	resp := model.ProviderInfo{}
	var count int
	for p := range combined {
		count++
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

	if count == 0 {
		http.Error(w, "", http.StatusNotFound)
		return
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
