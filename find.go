package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/indexstar/metrics"
	"github.com/mercari/go-circuitbreaker"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

const (
	findMethodOrig      = "http-v0"
	findMethodDelegated = "delegated-v1"
)

func (s *server) findCid(w http.ResponseWriter, r *http.Request, encrypted bool) {
	switch r.Method {
	case http.MethodOptions:
		handleIPNIOptions(w, false)
	case http.MethodGet:
		sc := path.Base(r.URL.Path)
		c, err := cid.Decode(sc)
		if err != nil {
			http.Error(w, "invalid cid: "+err.Error(), http.StatusBadRequest)
			return
		}
		s.find(w, r, c.Hash(), encrypted)
	default:
		w.Header().Set("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodOptions)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
}

func (s *server) findMultihashSubtree(w http.ResponseWriter, r *http.Request, encrypted bool) {
	switch r.Method {
	case http.MethodOptions:
		handleIPNIOptions(w, false)
	case http.MethodGet:
		smh := path.Base(r.URL.Path)
		mh, err := multihash.FromB58String(smh)
		if err != nil {
			var hexErr error
			mh, hexErr = multihash.FromHexString(smh)
			if hexErr != nil {
				http.Error(w, "invalid multihash: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		s.find(w, r, mh, encrypted)
	default:
		w.Header().Set("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodOptions)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
}

func (s *server) findMetadataSubtree(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	method := r.Method
	reqURL := r.URL

	sg := &scatterGather[Backend, []byte]{
		backends: s.backends,
		maxWait:  config.Server.ResultMaxWait,
	}

	// TODO: wait for the first successful response instead
	err := sg.scatter(ctx, func(cctx context.Context, b Backend) (*[]byte, error) {
		// send metadata requests only to dh backends
		if _, isDhBackend := b.(dhBackend); !isDhBackend {
			return nil, nil
		}

		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *reqURL
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
	})
	if err != nil {
		log.Errorw("Failed to scatter HTTP find metadata request", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	for md := range sg.gather(ctx) {
		// It is ok to return the first encountered metadata. This is because
		// metadata is uniquely identified by ValueKey (peerID + contextID).
		// I.e. it is not possible to have different metadata records for the
		// same ValueKey.
		//
		// Whereas in regular find requests it is perfectly normal to have
		// different results returned by different IPNI instances and hence
		// they need to be aggregated.
		if len(md) > 0 {
			writeJsonResponse(w, http.StatusOK, md)
			return
		}
	}
	http.Error(w, "", http.StatusNotFound)
}

func (s *server) find(w http.ResponseWriter, r *http.Request, mh multihash.Multihash, encrypted bool) {
	decoded, err := multihash.Decode(mh)
	if err != nil {
		http.Error(w, "bad multihash: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(decoded.Digest) == 0 {
		http.Error(w, "bad multihash: zero-length digest", http.StatusBadRequest)
		return
	}

	acc, err := getAccepts(r)
	if err != nil {
		http.Error(w, "invalid Accept header", http.StatusBadRequest)
		return
	}

	// Use NDJSON response only when the request explicitly accepts it. Otherwise, fallback on
	// JSON unless only unsupported media types are specified.
	switch {
	case acc.ndjson:
		s.doFindNDJson(r.Context(), w, findMethodOrig, r.URL, false, mh, encrypted)
	case acc.json || acc.any || !acc.acceptHeaderFound:
		if s.translateNonStreaming {
			s.doFindNDJson(r.Context(), w, findMethodOrig, r.URL, true, mh, encrypted)
			return
		}
		// In a case where the request has no `Accept` header at all, be forgiving and respond with
		// JSON.
		rcode, resp := s.doFind(r.Context(), r.Method, findMethodOrig, r.URL, encrypted)
		if rcode != http.StatusOK {
			http.Error(w, "", rcode)
			return
		}
		writeJsonResponse(w, http.StatusOK, resp)
	default:
		// The request must have  specified an explicit media type that we do not support.
		http.Error(w, "unsupported media type", http.StatusBadRequest)
		return
	}
}

func (s *server) doFind(ctx context.Context, method, source string, reqURL *url.URL, encrypted bool) (int, []byte) {
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
		rsp  *model.FindResponse
		bknd Backend
		err  error
	}

	sg := &scatterGather[Backend, sgResponse]{
		backends: s.backends,
		maxWait:  config.Server.ResultMaxWait,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var count int32
	if err := sg.scatter(ctx, func(cctx context.Context, b Backend) (*sgResponse, error) {
		// forward double hashed requests to double hashed backends only and regular requests to regular backends
		_, isDhBackend := b.(dhBackend)
		_, isProvidersBackend := b.(providersBackend)
		if (encrypted != isDhBackend) || isProvidersBackend {
			return nil, nil
		}

		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *reqURL
		endpoint.Host = b.URL().Host
		endpoint.Scheme = b.URL().Scheme
		log := log.With("backend", endpoint.Host)

		req, err := http.NewRequestWithContext(cctx, method, endpoint.String(), nil)
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
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Debugw("Backend query ended", "err", err)
				return nil, err
			} else {
				log.Warnw("Failed to query backend", "err", err)
				return &sgResponse{err: err}, nil
			}
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Debugw("Reading backend response ended", "err", err)
			} else {
				log.Warnw("Failed to read backend response", "err", err)
			}
			return nil, err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			atomic.AddInt32(&count, 1)
			providers, err := model.UnmarshalFindResponse(data)
			if err != nil {
				return nil, circuitbreaker.MarkAsSuccess(err)
			}
			return &sgResponse{bknd: b, rsp: providers}, nil
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
	var gatherErrs []error
	var rs resultStats
	var foundRegular, foundCaskade bool
	updateFoundFlags := func(b Backend) {
		_, isCaskade := b.(caskadeBackend)
		foundCaskade = foundCaskade || isCaskade
		foundRegular = foundRegular || !isCaskade
	}

outer:
	for r := range sg.gather(ctx) {
		if r.err != nil {
			gatherErrs = append(gatherErrs, r.err)
			continue
		}
		if len(r.rsp.MultihashResults) > 0 {
			if resp.MultihashResults == nil {
				resp.MultihashResults = r.rsp.MultihashResults
				updateFoundFlags(r.bknd)
			} else {
				if !bytes.Equal(resp.MultihashResults[0].Multihash, r.rsp.MultihashResults[0].Multihash) {
					// weird / invalid.
					log.Warnw("conflicting results", "q", reqURL, "first", resp.MultihashResults[0].Multihash, "second", r.rsp.MultihashResults[0].Multihash)
					return http.StatusInternalServerError, nil
				}
				for _, pr := range r.rsp.MultihashResults[0].ProviderResults {
					for _, rr := range resp.MultihashResults[0].ProviderResults {
						if bytes.Equal(rr.ContextID, pr.ContextID) && bytes.Equal([]byte(rr.Provider.ID), []byte(pr.Provider.ID)) {
							continue outer
						}
					}
					updateFoundFlags(r.bknd)
					resp.MultihashResults[0].ProviderResults = append(resp.MultihashResults[0].ProviderResults, pr)
					s.pcounts.Add(string(pr.Provider.ID))
				}
			}
		}

		if len(r.rsp.EncryptedMultihashResults) > 0 {
			if resp.EncryptedMultihashResults == nil {
				resp.EncryptedMultihashResults = r.rsp.EncryptedMultihashResults
				updateFoundFlags(r.bknd)
			} else {
				if !bytes.Equal(resp.EncryptedMultihashResults[0].Multihash, r.rsp.EncryptedMultihashResults[0].Multihash) {
					log.Warnw("conflicting encrypted results", "q", reqURL, "first", resp.EncryptedMultihashResults[0].Multihash, "second", r.rsp.EncryptedMultihashResults[0].Multihash)
					return http.StatusInternalServerError, nil
				}
				updateFoundFlags(r.bknd)
				resp.EncryptedMultihashResults[0].EncryptedValueKeys = append(resp.EncryptedMultihashResults[0].EncryptedValueKeys, r.rsp.EncryptedMultihashResults[0].EncryptedValueKeys...)
			}
		}
	}

	_ = stats.RecordWithOptions(context.Background(),
		stats.WithMeasurements(metrics.FindBackends.M(float64(atomic.LoadInt32(&count)))))

	if len(resp.MultihashResults) == 0 && len(resp.EncryptedMultihashResults) == 0 {
		if len(gatherErrs) != 0 {
			log.Warnw("Failed to request from backend", "errs", gatherErrs)
			return http.StatusGatewayTimeout, nil
		}
		latencyTags = append(latencyTags, tag.Insert(metrics.Found, "no"))
		return http.StatusNotFound, nil
	}

	latencyTags = append(latencyTags, tag.Insert(metrics.Found, "yes"))
	yesno := func(yn bool) string {
		if yn {
			return "yes"
		}
		return "no"
	}

	latencyTags = append(latencyTags, tag.Insert(metrics.FoundCaskade, yesno(foundCaskade)))
	latencyTags = append(latencyTags, tag.Insert(metrics.FoundRegular, yesno(foundRegular)))

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
