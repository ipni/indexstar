package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/ipni/indexstar/metrics"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/mercari/go-circuitbreaker"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var newline = []byte("\n")

type (
	resultSet map[uint32]struct{}

	encryptedOrPlainResult struct {
		model.ProviderResult
		EncryptedValueKey []byte `json:"EncryptedValueKey,omitempty"`
	}
)

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

func (s *server) doFindNDJson(ctx context.Context, w http.ResponseWriter, source string, req *url.URL, translateNonStreaming bool, mh multihash.Multihash) {
	start := time.Now()
	latencyTags := []tag.Mutator{tag.Insert(metrics.Method, http.MethodGet)}
	loadTags := []tag.Mutator{tag.Insert(metrics.Method, source)}
	defer func() {
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(latencyTags...),
			stats.WithMeasurements(metrics.FindLatency.M(float64(time.Since(start).Milliseconds()))))
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(loadTags...),
			stats.WithMeasurements(metrics.FindLoad.M(1)))
	}()
	var maxWait time.Duration
	if translateNonStreaming {
		maxWait = config.Server.ResultMaxWait
	} else {
		maxWait = config.Server.ResultStreamMaxWait
	}

	sg := &scatterGather[Backend, any]{
		backends: s.backends,
		maxWait:  maxWait,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultsChan := make(chan *encryptedOrPlainResult, 1)
	var count int32
	if err := sg.scatter(ctx, func(cctx context.Context, b Backend) (*any, error) {
		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *req
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

		resp, err := s.Client.Do(req)
		if err != nil {
			log.Warnw("Failed to query backend", "err", err)
			return nil, err
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
		case http.StatusNotFound:
			atomic.AddInt32(&count, 1)
			return nil, nil
		default:
			bb, _ := io.ReadAll(resp.Body)
			body := string(bb)
			log := log.With("status", resp.StatusCode, "body", body)
			log.Warn("Request processing was not successful")
			err := fmt.Errorf("status %d response from backend %s", resp.StatusCode, b.URL().Host)
			if resp.StatusCode < http.StatusInternalServerError {
				err = circuitbreaker.MarkAsSuccess(err)
			}
			return nil, err
		}

		scanner := bufio.NewScanner(resp.Body)
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
					atomic.AddInt32(&count, 1)
					if err := json.Unmarshal(line, &result); err != nil {
						return nil, circuitbreaker.MarkAsSuccess(err)
					}
					// Sanity check the results in case backends don't respect accept media types;
					// see: https://github.com/ipni/storetheindex/issues/1209
					if len(result.EncryptedValueKey) == 0 && (result.Provider.ID == "" || len(result.Provider.Addrs) == 0) {
						continue
					}

					select {
					case <-cctx.Done():
						return nil, nil
					case resultsChan <- &result:
					}
					continue
				}
				if err := scanner.Err(); err != nil {
					log.Warnw("Failed to read backend response", "err", err)
					return nil, circuitbreaker.MarkAsSuccess(err)
				}
				return nil, nil
			}
		}
	}); err != nil {
		log.Errorw("Failed to scatter HTTP find request", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	var provResults []model.ProviderResult
	var encValKeys [][]byte
	if translateNonStreaming {
		w.Header().Set("Content-Type", mediaTypeJson)
	} else {
		w.Header().Set("Content-Type", mediaTypeNDJson)
		w.Header().Set("Connection", "Keep-Alive")
		w.Header().Set("X-Content-Type-Options", "nosniff")
	}

	flusher, flushable := w.(http.Flusher)
	encoder := json.NewEncoder(w)
	results := newResultSet()

	// Results chan is done when gathering is finished.
	// Do this in a separate goroutine to avoid potentially closing results chan twice.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-sg.gather(ctx):
				if !ok {
					close(resultsChan)
					return
				}
			}
		}
	}()

LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case result, ok := <-resultsChan:
			if !ok {
				break LOOP
			}
			absent := results.putIfAbsent(result)
			if !absent {
				continue
			}

			if translateNonStreaming {

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
				if _, err := w.Write(newline); err != nil {
					log.Errorw("failed to write newline while streaming results", "result", result, "err", err)
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
	_ = stats.RecordWithOptions(context.Background(),
		stats.WithMeasurements(metrics.FindBackends.M(float64(atomic.LoadInt32(&count)))))

	if len(results) == 0 {
		latencyTags = append(latencyTags, tag.Insert(metrics.Found, "no"))
		http.Error(w, "", http.StatusNotFound)
		return
	}
	if translateNonStreaming {
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
	latencyTags = append(latencyTags, tag.Insert(metrics.Found, "yes"))
}
