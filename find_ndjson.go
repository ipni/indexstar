package main

import (
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
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var newline = []byte("\n")

type resultSet map[uint32]struct{}

func (r resultSet) putIfAbsent(p *model.ProviderResult) bool {
	// Calculate crc32 hash from provider ID + context ID to check for uniqueness of returned
	// results. The rationale for using crc32 hashing is that it is fast and good enough
	// for uniqueness check within a lookup request, while offering a small memory footprint
	// compared to storing the complete key, i.e. provider ID + context ID.
	pidb := []byte(p.Provider.ID)
	v := make([]byte, 0, len(pidb)+len(p.ContextID))
	v = append(v, pidb...)
	v = append(v, p.ContextID...)
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

func (s *server) doFindNDJson(ctx context.Context, w http.ResponseWriter, method, source string, req *url.URL) {
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

	sg := &scatterGather[*url.URL, *model.ProviderResult]{
		targets: s.servers,
		tcb:     s.serverCallers,
		maxWait: config.Server.ResultMaxWait,
	}

	var count int32
	if err := sg.scatter(ctx, func(cctx context.Context, b *url.URL) (**model.ProviderResult, error) {
		// Copy the URL from original request and override host/schema to point
		// to the server.
		endpoint := *req
		endpoint.Host = b.Host
		endpoint.Scheme = b.Scheme
		log := log.With("backend", endpoint)

		req, err := http.NewRequestWithContext(cctx, method, endpoint.String(), nil)
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
			var result model.ProviderResult
			if err := json.Unmarshal(data, &result); err != nil {
				return nil, err
			}
			// Sanity check the results in case backends don't respect accept media types;
			// see: https://github.com/ipni/storetheindex/issues/1209
			if result.Provider.ID == "" || len(result.Provider.Addrs) == 0 {
				return nil, nil
			}
			p := &result
			return &p, nil
		case http.StatusNotFound:
			atomic.AddInt32(&count, 1)
			return nil, nil
		default:
			return nil, fmt.Errorf("status %d response from backend %s", resp.StatusCode, b.String())
		}
	}); err != nil {
		log.Errorw("Failed to scatter HTTP find request", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", mediaTypeNDJson)
	w.Header().Set("Connection", "Keep-Alive")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	flusher, flushable := w.(http.Flusher)
	encoder := json.NewEncoder(w)
	results := newResultSet()
	for result := range sg.gather(ctx) {
		absent := results.putIfAbsent(result)
		if !absent {
			continue
		}
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
	_ = stats.RecordWithOptions(context.Background(),
		stats.WithMeasurements(metrics.FindBackends.M(float64(atomic.LoadInt32(&count)))))

	if len(results) == 0 {
		latencyTags = append(latencyTags, tag.Insert(metrics.Found, "no"))
		http.Error(w, "", http.StatusNotFound)
		return
	}
	latencyTags = append(latencyTags, tag.Insert(metrics.Found, "yes"))
}
