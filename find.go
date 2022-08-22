package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-shipyard/indexstar/httpserver"
)

func (s *server) find(w http.ResponseWriter, r *http.Request) {
	combined := make(chan *model.FindResponse, len(s.servers))
	wg := sync.WaitGroup{}

	// Copy the original request body in case it is a POST batch find request.
	var rb []byte
	var err error
	rb, err = io.ReadAll(r.Body)
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
			var body io.Reader
			if len(rb) > 0 {
				body = bytes.NewReader(rb)
			}
			req, err := http.NewRequest(r.Method, endpoint.String(), body)
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
			if resp.StatusCode == http.StatusOK {
				data, err := io.ReadAll(resp.Body)
				if err != nil {
					log.Warnw("failed backend read", "err", err)
					return
				}
				providers, err := model.UnmarshalFindResponse(data)
				if err == nil {
					combined <- providers
				} else {
					log.Warnw("failed backend unmarshal", "err", err)
				}
			}
		}(server)
	}
	go func() {
		wg.Wait()
		close(combined)
	}()

	// TODO: stream out partial response as they come in.
	var resp model.FindResponse
outer:
	for prov := range combined {
		if resp.MultihashResults == nil {
			resp.MultihashResults = prov.MultihashResults
		} else {
			if !bytes.Equal(resp.MultihashResults[0].Multihash, prov.MultihashResults[0].Multihash) {
				// weird / invalid.
				log.Warnw("conflicting results", "q", r.URL.Path, "first", resp.MultihashResults[0].Multihash, "second", prov.MultihashResults[0].Multihash)

				httpserver.HandleError(w, fmt.Errorf("conflicting results"), "get")
				continue
			}
			for _, pr := range prov.MultihashResults[0].ProviderResults {
				for _, rr := range resp.MultihashResults[0].ProviderResults {
					if bytes.Equal(rr.ContextID, pr.ContextID) && bytes.Equal([]byte(rr.Provider.ID), []byte(pr.Provider.ID)) {
						continue outer
					}
				}
				resp.MultihashResults[0].ProviderResults = append(resp.MultihashResults[0].ProviderResults, pr)
			}
		}
	}

	if resp.MultihashResults == nil {
		http.Error(w, "no results for query", http.StatusNotFound)
		return
	}

	// write out combined.
	outData, err := model.MarshalFindResponse(&resp)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, outData)
}
