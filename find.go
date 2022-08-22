package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-shipyard/indexstar/httpserver"
)

func (s *server) find(w http.ResponseWriter, r *http.Request) {
	combined := make(chan *model.FindResponse, len(s.servers))
	wg := sync.WaitGroup{}
	for _, server := range s.servers {
		wg.Add(1)
		go func(server *url.URL) {
			defer wg.Done()
			r.URL.Host = server.Host
			r.URL.Scheme = server.Scheme
			resp, err := s.Client.Get(r.URL.String())
			if err != nil {
				log.Warnw("failed query", "backend", r.URL, "err", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				data, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Warnw("failed backend read", "backend", r.URL, "err", err)
					return
				}
				providers, err := model.UnmarshalFindResponse(data)
				if err == nil {
					combined <- providers
				} else {
					log.Warnw("failed backend unmarshal", "backend", r.URL, "err", err)
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
				log.Warnw("conflicting results", "q", r.URL.Path, "first", resp.MultihashResults[0].Multihash, "secnd", prov.MultihashResults[0].Multihash)

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
