package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-shipyard/indexstar/httpserver"
	"github.com/filecoin-shipyard/indexstar/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

func (s *server) find(w http.ResponseWriter, r *http.Request) {
	// Copy the original request body in case it is a POST batch find request.
	rb, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		log.Warnw("failed to read original request body", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	rcode, resp := s.doFind(r.Context(), r.Method, r.URL, rb)

	if rcode != http.StatusOK {
		http.Error(w, "", rcode)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, resp)
}

func (s *server) doFind(ctx context.Context, method string, req *url.URL, body []byte) (int, []byte) {
	start := time.Now()
	tags := []tag.Mutator{}
	defer func() {
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(tags...),
			stats.WithMeasurements(metrics.FindLatency.M(float64(time.Since(start).Milliseconds()))))
	}()

	sg := &scatterGather[*url.URL, *model.FindResponse]{
		targets: s.servers,
		maxWait: config.Server.ResultMaxWait,
	}

	count := atomic.Int32{}
	if err := sg.scatter(ctx, func(b *url.URL) (<-chan *model.FindResponse, error) {
		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *req
		endpoint.Host = b.Host
		endpoint.Scheme = b.Scheme
		log := log.With("backend", endpoint)

		bodyReader := bytes.NewReader(body)

		req, err := http.NewRequest(method, endpoint.String(), bodyReader)
		if err != nil {
			log.Warnw("Failed to construct backend query", "err", err)
			return nil, err
		}
		req.Header.Set("X-Forwarded-Host", req.Host)
		resp, err := s.Client.Do(req)
		if err != nil {
			log.Warnw("Failed to query backend", "err", err)
			return nil, err
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Warnw("Failed to read backend response", "err", err)
			return nil, err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			_ = count.Add(1)
			providers, err := model.UnmarshalFindResponse(data)
			if err != nil {
				return nil, err
			}
			r := make(chan *model.FindResponse, 1)
			r <- providers
			close(r)
			return r, nil
		case http.StatusNotFound:
			_ = count.Add(1)
			return nil, nil
		default:
			return nil, fmt.Errorf("status %d response from backend %s", resp.StatusCode, b.String())
		}
	}); err != nil {
		log.Errorw("Failed to scatter HTTP find request", "err", err)
		return http.StatusInternalServerError, []byte{}
	}

	// TODO: stream out partial response as they come in.
	var resp model.FindResponse
outer:
	for prov := range sg.gather(ctx) {
		if resp.MultihashResults == nil {
			resp.MultihashResults = prov.MultihashResults
		} else {
			if !bytes.Equal(resp.MultihashResults[0].Multihash, prov.MultihashResults[0].Multihash) {
				// weird / invalid.
				log.Warnw("conflicting results", "q", req, "first", resp.MultihashResults[0].Multihash, "second", prov.MultihashResults[0].Multihash)
				return http.StatusInternalServerError, []byte{}
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

	_ = stats.RecordWithOptions(context.Background(),
		stats.WithMeasurements(metrics.FindBackends.M(float64(count.Load()))))

	if resp.MultihashResults == nil {
		tags = append(tags, tag.Insert(metrics.Found, "no"))
		return http.StatusNotFound, []byte{}
	} else {
		tags = append(tags, tag.Insert(metrics.Found, "yes"))
	}

	// write out combined.
	outData, err := model.MarshalFindResponse(&resp)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		return http.StatusInternalServerError, []byte{}
	}
	return http.StatusOK, outData
}
