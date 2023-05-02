package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/indexstar/httpserver"
	"github.com/ipni/indexstar/metrics"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/mercari/go-circuitbreaker"
	"github.com/multiformats/go-multihash"
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
		sc := strings.TrimPrefix(path.Base(r.URL.Path), "cid/")
		c, err := cid.Decode(sc)
		if err != nil {
			discardBody(r)
			http.Error(w, "invalid cid: "+err.Error(), http.StatusBadRequest)
		}
		s.find(w, r, c.Hash())
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
		s.find(w, r, nil)
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

		smh := strings.TrimPrefix(path.Base(r.URL.Path), "multihash/")
		mh, err := multihash.FromB58String(smh)
		if err != nil {
			discardBody(r)
			http.Error(w, "invalid multihash: "+err.Error(), http.StatusBadRequest)
		}

		s.find(w, r, mh)
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

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	method := r.Method
	req := r.URL

	sg := &scatterGather[Backend, []byte]{
		backends: s.backends,
		maxWait:  config.Server.ResultMaxWait,
	}

	// TODO: wait for the first successful response instead
	if err := sg.scatter(ctx, func(cctx context.Context, b Backend) (*[]byte, error) {
		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *req
		endpoint.Host = b.URL().Host
		endpoint.Scheme = b.URL().Scheme
		log := log.With("backend", endpoint.Host)

		req, err := http.NewRequestWithContext(cctx, method, endpoint.String(), nil)
		if err != nil {
			log.Warnw("Failed to construct find-metadata backend query", "err", err)
			return nil, err
		}
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("Accept", mediaTypeJson)
		if !b.Matches(req) {
			return nil, nil
		}
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
			body := string(data)
			log := log.With("status", resp.StatusCode, "body", body)
			log.Warn("Request processing was not successful")
			err := fmt.Errorf("status %d response from backend %s", resp.StatusCode, b.URL().Host)
			if resp.StatusCode < http.StatusInternalServerError {
				err = circuitbreaker.MarkAsSuccess(err)
			}
			return nil, err
		}
	}); err != nil {
		log.Errorw("Failed to scatter HTTP find metadata request", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	for md := range sg.gather(ctx) {
		// It's ok to return the first encountered metadata. This is because metadata is uniquely identified
		// by ValueKey (peerID + contextID). I.e. it's not possible to have different metadata records for the same ValueKey.
		// In comparison to regular find requests where it's perfectly normal to have different results returned by different IPNI
		// instances and hence they need to be aggregated.
		if len(md) > 0 {
			httpserver.WriteJsonResponse(w, http.StatusOK, md)
			return
		}
	}
	http.Error(w, "", http.StatusNotFound)
}

func (s *server) find(w http.ResponseWriter, r *http.Request, mh multihash.Multihash) {
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
		s.doFindNDJson(ctx, w, findMethodOrig, r.URL, false, mh)
	case acc.json || acc.any || !acc.acceptHeaderFound:
		if s.translateNonStreaming {
			s.doFindNDJson(ctx, w, findMethodOrig, r.URL, true, mh)
			return
		}
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

	// sgResponse is a struct that exists to capture the backend that the response has been received from
	type sgResponse struct {
		r *model.FindResponse
		b Backend
	}

	sg := &scatterGather[Backend, sgResponse]{
		backends: s.backends,
		maxWait:  config.Server.ResultMaxWait,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var count int32
	if err := sg.scatter(ctx, func(cctx context.Context, b Backend) (*sgResponse, error) {
		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *req
		endpoint.Host = b.URL().Host
		endpoint.Scheme = b.URL().Scheme
		log := log.With("backend", endpoint.Host)

		bodyReader := bytes.NewReader(body)
		req, err := http.NewRequestWithContext(cctx, method, endpoint.String(), bodyReader)
		if err != nil {
			log.Warnw("Failed to construct backend query", "err", err)
			return nil, err
		}
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("Accept", mediaTypeJson)

		if !b.Matches(req) {
			return nil, nil
		}

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
				return nil, circuitbreaker.MarkAsSuccess(err)
			}
			return &sgResponse{b: b, r: providers}, nil
		case http.StatusNotFound:
			atomic.AddInt32(&count, 1)
			return nil, nil
		default:
			body := string(data)
			log := log.With("status", resp.StatusCode, "body", body)
			log.Warn("Request processing was not successful")
			err := fmt.Errorf("status %d response from backend %s", resp.StatusCode, b.URL().Host)
			if resp.StatusCode < http.StatusInternalServerError {
				err = circuitbreaker.MarkAsSuccess(err)
			}
			return nil, err
		}
	}); err != nil {
		log.Errorw("Failed to scatter HTTP find request", "err", err)
		return http.StatusInternalServerError, nil
	}

	// TODO: stream out partial response as they come in.
	var resp model.FindResponse
	var rs resultStats
	var foundRegular, foundCaskade bool
outer:
	for r := range sg.gather(ctx) {
		if len(r.r.MultihashResults) > 0 {
			if resp.MultihashResults == nil {
				resp.MultihashResults = r.r.MultihashResults
			} else {
				if !bytes.Equal(resp.MultihashResults[0].Multihash, r.r.MultihashResults[0].Multihash) {
					// weird / invalid.
					log.Warnw("conflicting results", "q", req, "first", resp.MultihashResults[0].Multihash, "second", r.r.MultihashResults[0].Multihash)
					return http.StatusInternalServerError, nil
				}
				for _, pr := range r.r.MultihashResults[0].ProviderResults {
					for _, rr := range resp.MultihashResults[0].ProviderResults {
						if bytes.Equal(rr.ContextID, pr.ContextID) && bytes.Equal([]byte(rr.Provider.ID), []byte(pr.Provider.ID)) {
							continue outer
						}
					}
					_, isCaskade := r.b.(caskadeBackend)
					foundCaskade = foundCaskade || isCaskade
					foundRegular = foundRegular || !isCaskade

					resp.MultihashResults[0].ProviderResults = append(resp.MultihashResults[0].ProviderResults, pr)
				}
			}
		}

		if len(r.r.EncryptedMultihashResults) > 0 {
			if resp.EncryptedMultihashResults == nil {
				resp.EncryptedMultihashResults = r.r.EncryptedMultihashResults
			} else {
				if !bytes.Equal(resp.EncryptedMultihashResults[0].Multihash, r.r.EncryptedMultihashResults[0].Multihash) {
					log.Warnw("conflicting encrypted results", "q", req, "first", resp.EncryptedMultihashResults[0].Multihash, "second", r.r.EncryptedMultihashResults[0].Multihash)
					return http.StatusInternalServerError, nil
				}
				_, isCaskade := r.b.(caskadeBackend)
				foundCaskade = foundCaskade || isCaskade
				foundRegular = foundRegular || !isCaskade

				resp.EncryptedMultihashResults[0].EncryptedValueKeys = append(resp.EncryptedMultihashResults[0].EncryptedValueKeys, r.r.EncryptedMultihashResults[0].EncryptedValueKeys...)
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
		if foundCaskade {
			latencyTags = append(latencyTags, tag.Insert(metrics.FoundCaskade, "yes"))
		} else {
			latencyTags = append(latencyTags, tag.Insert(metrics.FoundCaskade, "no"))
		}
		if foundRegular {
			latencyTags = append(latencyTags, tag.Insert(metrics.FoundRegular, "yes"))
		} else {
			latencyTags = append(latencyTags, tag.Insert(metrics.FoundRegular, "no"))
		}
	}

	rs.observeFindResponse(&resp)
	rs.reportMetrics(source)

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
