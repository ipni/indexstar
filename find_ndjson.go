package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/metadata"
	"github.com/ipni/indexstar/metrics"
	"github.com/mercari/go-circuitbreaker"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

type (
	resultSet map[uint32]struct{}

	encryptedOrPlainResult struct {
		model.ProviderResult
		EncryptedValueKey []byte `json:"EncryptedValueKey,omitempty"`
	}
	resultStats struct {
		encCount                int64
		bitswapTransportCount   int64
		graphsyncTransportCount int64
		unknwonTransportCount   int64
		entriesByProvider       map[string]int64
	}
)

const detailedProvidersMetricsCardinalitySafetyLimit = 500

func (r resultSet) putIfAbsent(p *encryptedOrPlainResult) bool {
	// Calculate crc32 hash from provider ID + context ID to check for uniqueness of returned
	// results. The rationale for using crc32 hashing is that it is fast and good enough
	// for uniqueness check within a lookup request, while offering a small memory footprint
	// compared to storing the complete key, i.e. provider ID + context ID.
	var v []byte
	if len(p.EncryptedValueKey) > 0 {
		v = p.EncryptedValueKey
	} else {
		pidb := []byte(p.Provider.ID)
		v = make([]byte, 0, len(pidb)+len(p.ContextID))
		v = append(v, pidb...)
		v = append(v, p.ContextID...)
		v = append(v, p.Metadata...)
	}
	key := crc32.ChecksumIEEE(v)
	if _, seen := r[key]; seen {
		return false
	}
	r[key] = struct{}{}
	return true
}

func newResultSet() resultSet {
	return make(map[uint32]struct{})
}

func (rs *resultStats) observeResult(result *encryptedOrPlainResult) {
	if len(result.EncryptedValueKey) > 0 {
		rs.encCount++
	} else {
		rs.observeProviderResult(&result.ProviderResult)
	}
}

func (rs *resultStats) observeProviderResult(result *model.ProviderResult) {
	if rs.entriesByProvider == nil {
		rs.entriesByProvider = make(map[string]int64)
	}
	rs.entriesByProvider[result.Provider.ID.String()]++

	md := metadata.Default.New()
	if err := md.UnmarshalBinary(result.Metadata); err != nil {
		// TODO Refactor once there is concrete error type in index-provider
		if strings.HasPrefix(err.Error(), "unknown transport id") {
			// There is at least one unknown transport protocol
			rs.unknwonTransportCount++
		}
		// Proceed with checking md, as unmarshal binary may have partially
		// populated md with known transports
	}
	for _, p := range md.Protocols() {
		switch p {
		case multicodec.TransportBitswap:
			rs.bitswapTransportCount++
		case multicodec.TransportGraphsyncFilecoinv1:
			rs.graphsyncTransportCount++
		default:
			// In case new protocols are added to metadata.Default context and this
			// switch is not updated count them as unknown.
			rs.unknwonTransportCount++
		}
	}
}

func (rs *resultStats) observeFindResponse(resp *model.FindResponse) {
	for _, emr := range resp.EncryptedMultihashResults {
		rs.encCount += int64(len(emr.EncryptedValueKeys))
	}
	for _, mhr := range resp.MultihashResults {
		for _, pr := range mhr.ProviderResults {
			rs.observeProviderResult(&pr)
		}
	}
}

func (rs *resultStats) reportMetrics(method string, detailedProvidersMetrics bool) {
	if rs.bitswapTransportCount > 0 {
		metrics.FindResponse.
			WithLabelValues(method, multicodec.TransportBitswap.String()).
			Add(float64(rs.bitswapTransportCount))
	}

	if rs.graphsyncTransportCount > 0 {
		metrics.FindResponse.
			WithLabelValues(method, multicodec.TransportGraphsyncFilecoinv1.String()).
			Add(float64(rs.graphsyncTransportCount))
	}

	if rs.unknwonTransportCount > 0 {
		metrics.FindResponse.
			WithLabelValues(method, "unknown").
			Add(float64(rs.unknwonTransportCount))
	}

	if rs.encCount > 0 {
		metrics.FindResponse.
			WithLabelValues(method, "encrypted").
			Add(float64(rs.encCount))
	}

	if detailedProvidersMetrics {
		rs.reportDetailedProvidersMetrics(method)
	}
}

func (rs *resultStats) reportDetailedProvidersMetrics(method string) {
	if len(rs.entriesByProvider) >= detailedProvidersMetricsCardinalitySafetyLimit {
		// Stop emitting detailed providers metrics to avoid high cardinality.
		metrics.FindProviderResults.DeletePartialMatch(nil)
		metrics.FindProviderResultEntries.DeletePartialMatch(nil)
		metrics.FindProviderResultWeighted.DeletePartialMatch(nil)
		return
	}

	var totalEntries int64
	for provider, count := range rs.entriesByProvider {
		metrics.FindProviderResults.
			WithLabelValues(provider, method).
			Add(1)

		metrics.FindProviderResultEntries.
			WithLabelValues(provider, method).
			Add(float64(count))

		totalEntries += count
	}

	for provider, count := range rs.entriesByProvider {
		metrics.FindProviderResultWeighted.
			WithLabelValues(provider, method).
			Add(float64(count) / float64(totalEntries))
	}
}

type resultWithBackend struct {
	rslt *encryptedOrPlainResult
	bknd Backend
}

func (s *server) fetchUpstreamNDJsonResponses(
	ctx context.Context,
	maxWait time.Duration,
	encrypted bool,
	reqURL *url.URL,
	method string,
) (
	chan *resultWithBackend,
	error,
) {
	sg := &scatterGather[Backend, any]{
		backends: s.backends,
		maxWait:  maxWait,
	}

	resultsChan := make(chan *resultWithBackend, 1)
	var backendsCount atomic.Int32
	var totalResultsCount atomic.Int32
	if err := sg.scatter(ctx, func(cctx context.Context, b Backend) (*any, error) {
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

		req, err := http.NewRequestWithContext(cctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			log.Warnw("Failed to construct backend query", "err", err)
			return nil, err
		}
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("Accept", mediaTypeNDJson)

		if !b.Matches(req) {
			return nil, nil
		}

		log.Debugw("sending ndjson request", "url", req.URL.String())

		validBackendEntriesCount := 0
		malformedBackendEntriesCount := 0

		start := time.Now()
		measureBackendLatency := func(errKind metrics.ErrKind) {
			metrics.ReportFindBackendMetrics(
				b.URL().Host,
				method,
				true,
				errKind,
				time.Since(start),
				validBackendEntriesCount,
				malformedBackendEntriesCount,
			)
		}

		resp, err := s.httpClient.Do(req)
		switch {
		case errors.Is(err, context.Canceled):
			measureBackendLatency(metrics.ErrKindRequestCanceled)
			log.Debugw("Backend query ended", "err", err)
			return nil, err

		case errors.Is(err, context.DeadlineExceeded):
			measureBackendLatency(metrics.ErrKindRequestDeadline)
			log.Debugw("Backend query timed out", "err", err)
			return nil, err

		case err != nil:
			measureBackendLatency(metrics.ErrKindRequestFailed)
			log.Warnw("Failed to query backend", "err", err)
			return nil, err
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			backendsCount.Add(1)

		case http.StatusNotFound:
			measureBackendLatency(metrics.ErrKindNotFound)
			io.Copy(io.Discard, resp.Body)
			backendsCount.Add(1)
			log.Debugw("not found response", "url", req.URL.String())
			return nil, nil

		default:
			measureBackendLatency(metrics.ErrKindHttpStatus(resp.StatusCode))
			bb, _ := io.ReadAll(resp.Body)
			body := string(bb)
			log.Warnw(
				"Request processing was not successful",
				"status", resp.StatusCode,
				"body", body[:min(len(body), 1000)],
			)
			err := fmt.Errorf("status %d response from backend %s", resp.StatusCode, b.URL().Host)
			if resp.StatusCode < http.StatusInternalServerError {
				err = circuitbreaker.MarkAsSuccess(err)
			}
			return nil, err
		}

		scanner := bufio.NewScanner(resp.Body)
		providersCount := 0
		for {
			select {
			case <-cctx.Done():
				return nil, nil
			default:
				if scanner.Scan() {
					var result encryptedOrPlainResult
					line := scanner.Bytes()
					if len(line) == 0 {
						continue
					}

					providersCount++
					if err := json.Unmarshal(line, &result); err != nil {
						measureBackendLatency(metrics.ErrKindUnmarshalFailed)
						log.Debugw(
							"failed to unmarshal backend response line",
							"line", string(line),
							"respContentType", resp.Header.Get("Content-Type"),
							"err", err,
						)
						return nil, circuitbreaker.MarkAsSuccess(err)
					}
					// Sanity check the results in case backends don't respect accept media types;
					// see: https://github.com/ipni/storetheindex/issues/1209
					if len(result.EncryptedValueKey) == 0 && (result.Provider == nil || result.Provider.ID == "" || len(result.Provider.Addrs) == 0) {
						malformedBackendEntriesCount++
						log.Debugw(
							"skipping malformed result",
							"line", string(line),
							"respContentType", resp.Header.Get("Content-Type"),
							"err", err,
						)
						continue
					}

					validBackendEntriesCount++

					select {
					case <-cctx.Done():
						return nil, nil
					case resultsChan <- &resultWithBackend{rslt: &result, bknd: b}:
						totalResultsCount.Add(1)
					}
					continue
				}
				if err := scanner.Err(); err != nil {
					switch {
					case errors.Is(err, context.Canceled):
						measureBackendLatency(metrics.ErrKindReadCanceled)
						log.Debugw("Reading backend response cancelled", "err", err)
					case errors.Is(err, context.DeadlineExceeded):
						measureBackendLatency(metrics.ErrKindReadDeadline)
						log.Debugw("Reading backend response timed out", "err", err)
					default:
						measureBackendLatency(metrics.ErrKindReadFailed)
						log.Warnw("Failed to read backend response", "err", err)
					}
					return nil, circuitbreaker.MarkAsSuccess(err)
				}

				measureBackendLatency(metrics.ErrKindNone)
				log.Debugw(
					"Finished processing results from backend",
					"providersCount", providersCount,
				)
				return nil, nil
			}
		}
	}); err != nil {
		log.Errorw("Failed to scatter HTTP find request", "err", err)
		return nil, err
	}

	// Results chan is done when gathering is finished.
	// Do this in a separate goroutine to avoid potentially closing results chan twice.
	go func() {
		sg.wg.Wait()

		close(resultsChan)

		metrics.FindBackends.Set(float64(backendsCount.Load()))

		log.Debugw(
			"Completed processing find results",
			"backends", backendsCount.Load(),
			"totalResultsCount", totalResultsCount.Load(),
		)
	}()

	return resultsChan, nil
}

func (s *server) doFindNDJson(
	ctx context.Context,
	w http.ResponseWriter,
	method string,
	reqURL *url.URL,
	translateToNonStreaming bool,
	mh multihash.Multihash,
	encrypted bool,
) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var maxWait time.Duration
	if translateToNonStreaming {
		maxWait = config.Server.ResultMaxWait
	} else {
		maxWait = config.Server.ResultStreamMaxWait
	}

	start := time.Now()

	resultsChan, err := s.fetchUpstreamNDJsonResponses(ctx, maxWait, encrypted, reqURL, method)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	w.Header().Set("X-Content-Type-Options", "nosniff")
	if translateToNonStreaming {
		w.Header().Set("Content-Type", mediaTypeJson)
	} else {
		w.Header().Set("Content-Type", mediaTypeNDJson)
	}

	flusher, flushable := w.(http.Flusher)
	encoder := json.NewEncoder(w)
	results := newResultSet()

	var provResults []model.ProviderResult
	var encValKeys [][]byte
	var rs resultStats
	var found, foundCaskade, foundRegular bool

	defer func() {
		if found {
			rs.reportMetrics(method, s.detailedProvidersMetrics)
		}

		metrics.ReportFindLatency(method, found, foundCaskade, foundRegular, time.Since(start))
		metrics.FindLoad.WithLabelValues(method).Inc()
	}()

LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case rwb, ok := <-resultsChan:
			if !ok {
				break LOOP
			}
			result := rwb.rslt
			absent := results.putIfAbsent(result)
			if !absent {
				continue
			}

			rs.observeResult(result)

			_, isCaskade := rwb.bknd.(caskadeBackend)
			foundCaskade = foundCaskade || isCaskade
			foundRegular = foundRegular || !isCaskade
			found = true

			if translateToNonStreaming {
				if len(result.EncryptedValueKey) > 0 {
					encValKeys = append(encValKeys, result.EncryptedValueKey)
				} else {
					provResults = append(provResults, result.ProviderResult)
				}
			} else {
				if err := encoder.Encode(result); err != nil {
					log.Errorw("failed to encode streaming result", "result", result, "err", err)
					continue
				}
				// TODO: optimise the number of time we call flush based on some time-based or result
				//       count heuristic.
				if flushable {
					flusher.Flush()
				}
			}
		}
	}

	if len(results) == 0 {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	if translateToNonStreaming {
		var resp model.FindResponse
		if len(provResults) > 0 {
			resp.MultihashResults = []model.MultihashResult{
				{
					Multihash:       mh,
					ProviderResults: provResults,
				},
			}
		}
		if len(encValKeys) > 0 {
			resp.EncryptedMultihashResults = []model.EncryptedMultihashResult{
				{
					Multihash:          mh,
					EncryptedValueKeys: encValKeys,
				},
			}
		}
		if err := encoder.Encode(resp); err != nil {
			log.Errorw("Failed to encode translated non streaming response", "err", err)
		}
	}
}

func (s *server) doFindStreaming(ctx context.Context, method string, reqURL *url.URL, encrypted bool) (int, chan model.ProviderResult) {
	maxWait := config.Server.ResultStreamMaxWait

	start := time.Now()

	resultsChan, err := s.fetchUpstreamNDJsonResponses(ctx, maxWait, encrypted, reqURL, method)
	if err != nil {
		return http.StatusInternalServerError, nil
	}

	out := make(chan model.ProviderResult)

	go func() {
		found, foundCaskade, foundRegular := false, false, false
		rs := resultStats{}
		results := newResultSet()

		defer func() {
			close(out)

			if found {
				rs.reportMetrics(method, s.detailedProvidersMetrics)
			}

			metrics.ReportFindLatency(method, found, foundCaskade, foundRegular, time.Since(start))
			metrics.FindLoad.WithLabelValues(method).Inc()
		}()

		for {
			select {
			case <-ctx.Done():
				return

			case rwb, ok := <-resultsChan:
				if !ok {
					return
				}

				result := rwb.rslt

				if !results.putIfAbsent(result) {
					continue
				}

				rs.observeResult(result)

				_, isCaskade := rwb.bknd.(caskadeBackend)
				foundCaskade = foundCaskade || isCaskade
				foundRegular = foundRegular || !isCaskade
				found = true

				out <- result.ProviderResult
			}
		}
	}()

	return 200, out
}
