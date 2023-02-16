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

	"github.com/ipni/indexstar/httpserver"
	"github.com/ipni/indexstar/metrics"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

const (
	findMethodOrig      = "http-v0"
	findMethodReframe   = "reframe-v1"
	findMethodDelegated = "delegated-v1"
)

func (s *server) findCid(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		discardBody(r)
		handleIPNIOptions(w, false)
	case http.MethodGet:
		s.find(w, r)
	default:
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *server) findMultihash(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		discardBody(r)
		handleIPNIOptions(w, true)
	case http.MethodPost:
		s.find(w, r)
	default:
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *server) findMultihashSubtree(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		discardBody(r)
		handleIPNIOptions(w, false)
	case http.MethodGet:
		s.find(w, r)
	default:
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *server) findMetadataSubtree(w http.ResponseWriter, r *http.Request) {
	discardBody(r)
	if r.Method != http.MethodGet {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	ctx := r.Context()
	method := r.Method
	req := r.URL

	sg := &scatterGather[*url.URL, []byte]{
		targets: s.servers,
		tcb:     s.serverCallers,
		maxWait: config.Server.ResultMaxWait,
	}

	// TODO: wait for the first successful response instead
	if err := sg.scatter(ctx, func(cctx context.Context, b *url.URL) (*[]byte, error) {
		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *req
		endpoint.Host = b.Host
		endpoint.Scheme = b.Scheme
		log := log.With("backend", endpoint)

		req, err := http.NewRequestWithContext(cctx, method, endpoint.String(), nil)
		if err != nil {
			log.Warnw("Failed to construct find-metadata backend query", "err", err)
			return nil, err
		}
		req.Header.Set("X-Forwarded-Host", req.Host)
		resp, err := s.Client.Do(req)
		if err != nil {
			log.Warnw("Failed to query backend for metadata", "err", err)
			return nil, err
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Warnw("Failed to read find-metadata backend response", "err", err)
			return nil, err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			return &data, nil
		case http.StatusNotFound:
			return nil, nil
		default:
			return nil, fmt.Errorf("status %d response from backend %s", resp.StatusCode, b.String())
		}
	}); err != nil {
		log.Errorw("Failed to scatter HTTP find metadata request", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	var res []byte
	for md := range sg.gather(ctx) {
		if len(md) == 0 {
			continue
		}
		// It's ok to return the first encountered metadata. This is because metadata is uniquely identified
		// by ValueKey (peerID + contextID). I.e. it's not possible to have different metadata records for the same ValueKey.
		// In comparison to regular find requests where it's perfectly normal to have different results returned by different IPNI
		// instances and hence they need to be aggregated.
		res = md
		// Continue to iterate to drain channels and avoid memory leak.
	}

	if len(res) == 0 {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, res)
}

func (s *server) find(w http.ResponseWriter, r *http.Request) {
	acc, err := getAccepts(r)
	if err != nil {
		discardBody(r)
		http.Error(w, "invalid Accept header", http.StatusBadRequest)
		return
	}
	var rb []byte
	switch r.Method {
	case http.MethodGet:
		discardBody(r)
	case http.MethodPost:
		if !acc.json && !acc.any && acc.acceptHeaderFound {
			// Only non-streaming JSON is supported for POST requests.
			http.Error(w, "unsupported media type", http.StatusBadRequest)
			return
		}
		// Copy the original request body in case it is a POST batch find request.
		var err error
		rb, err = io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			log.Warnw("Failed to read original request body", "err", err)
			http.Error(w, "", http.StatusBadRequest)
			return
		}
	default:
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
		return
	}
	ctx := r.Context()

	// Use NDJSON response only when the request explicitly accepts it. Otherwise, fallback on
	// JSON unless only unsupported media types are specified.
	switch {
	case acc.ndjson:
		s.doFindNDJson(ctx, w, r.Method, findMethodOrig, r.URL)
	case acc.json || acc.any || !acc.acceptHeaderFound:
		// In a case where the request has no `Accept` header at all, be forgiving and respond with
		// JSON.
		rcode, resp := s.doFind(ctx, r.Method, findMethodOrig, r.URL, rb)
		if rcode != http.StatusOK {
			http.Error(w, "", rcode)
			return
		}
		httpserver.WriteJsonResponse(w, http.StatusOK, resp)
	default:
		// The request must have  specified an explicit media type that we do not support.
		http.Error(w, "unsupported media type", http.StatusBadRequest)
	}
}

func (s *server) doFind(ctx context.Context, method, source string, req *url.URL, body []byte) (int, []byte) {
	start := time.Now()
	latencyTags := []tag.Mutator{tag.Insert(metrics.Method, method)}
	loadTags := []tag.Mutator{tag.Insert(metrics.Method, source)}
	defer func() {
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(latencyTags...),
			stats.WithMeasurements(metrics.FindLatency.M(float64(time.Since(start).Milliseconds()))))
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(loadTags...),
			stats.WithMeasurements(metrics.FindLoad.M(1)))
	}()

	sg := &scatterGather[*url.URL, *model.FindResponse]{
		targets: s.servers,
		tcb:     s.serverCallers,
		maxWait: config.Server.ResultMaxWait,
	}

	var count int32
	if err := sg.scatter(ctx, func(cctx context.Context, b *url.URL) (**model.FindResponse, error) {
		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *req
		endpoint.Host = b.Host
		endpoint.Scheme = b.Scheme
		log := log.With("backend", endpoint)

		bodyReader := bytes.NewReader(body)

		req, err := http.NewRequestWithContext(cctx, method, endpoint.String(), bodyReader)
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
			atomic.AddInt32(&count, 1)
			providers, err := model.UnmarshalFindResponse(data)
			if err != nil {
				return nil, err
			}
			return &providers, nil
		case http.StatusNotFound:
			atomic.AddInt32(&count, 1)
			return nil, nil
		default:
			if resp.StatusCode < http.StatusInternalServerError {
				log.Warnw("Request processing was not successful", "status", resp.StatusCode, "body", data)
				return nil, nil
			}
			log.Errorw("Request processing failed due to server error", "status", resp.StatusCode, "body", data)
			return nil, fmt.Errorf("status %d response from backend %s: %s", resp.StatusCode, b.String(), data)
		}
	}); err != nil {
		log.Errorw("Failed to scatter HTTP find request", "err", err)
		return http.StatusInternalServerError, nil
	}

	// TODO: stream out partial response as they come in.
	var resp model.FindResponse
outer:
	for prov := range sg.gather(ctx) {
		if len(prov.MultihashResults) > 0 {
			if resp.MultihashResults == nil {
				resp.MultihashResults = prov.MultihashResults
			} else {
				if !bytes.Equal(resp.MultihashResults[0].Multihash, prov.MultihashResults[0].Multihash) {
					// weird / invalid.
					log.Warnw("conflicting results", "q", req, "first", resp.MultihashResults[0].Multihash, "second", prov.MultihashResults[0].Multihash)
					return http.StatusInternalServerError, nil
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

		if len(prov.EncryptedMultihashResults) > 0 {
			if resp.EncryptedMultihashResults == nil {
				resp.EncryptedMultihashResults = prov.EncryptedMultihashResults
			} else {
				if !bytes.Equal(resp.EncryptedMultihashResults[0].Multihash, prov.EncryptedMultihashResults[0].Multihash) {
					log.Warnw("conflicting encrypted results", "q", req, "first", resp.EncryptedMultihashResults[0].Multihash, "second", prov.EncryptedMultihashResults[0].Multihash)
					return http.StatusInternalServerError, nil
				}
				resp.EncryptedMultihashResults[0].EncryptedValueKeys = append(resp.EncryptedMultihashResults[0].EncryptedValueKeys, prov.EncryptedMultihashResults[0].EncryptedValueKeys...)
			}
		}
	}

	_ = stats.RecordWithOptions(context.Background(),
		stats.WithMeasurements(metrics.FindBackends.M(float64(atomic.LoadInt32(&count)))))

	if len(resp.MultihashResults) == 0 && len(resp.EncryptedMultihashResults) == 0 {
		latencyTags = append(latencyTags, tag.Insert(metrics.Found, "no"))
		return http.StatusNotFound, nil
	} else {
		latencyTags = append(latencyTags, tag.Insert(metrics.Found, "yes"))
	}

	// write out combined.
	outData, err := model.MarshalFindResponse(&resp)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		return http.StatusInternalServerError, nil
	}
	return http.StatusOK, outData
}

func handleIPNIOptions(w http.ResponseWriter, post bool) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	var methods string
	if post {
		methods = "GET, POST, OPTIONS"
	} else {
		methods = "GET, OPTIONS"
	}
	w.Header().Add("Access-Control-Allow-Methods", methods)
	w.Header().Add("Access-Control-Allow-Headers", "Content-Type, Accept")
	if config.Server.CascadeLabels != "" {
		// TODO Eventually we might want to propagate OPTIONS queries to backends,
		//      and dynamically populate cascade labels with some caching config.
		//      For now this is good enough.
		w.Header().Add("X-IPNI-Allow-Cascade", config.Server.CascadeLabels)
	}
	w.WriteHeader(http.StatusAccepted)
}

func discardBody(r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()
}
