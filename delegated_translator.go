package main

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"net/http"
	"net/url"
	"path"

	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/metadata"
	"github.com/ipni/indexstar/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

const (
	peerSchema = "peer"
)

type findFunc func(ctx context.Context, method, source string, req *url.URL, encrypted bool) (int, []byte)
type findStreamFunc func(ctx context.Context, method string, req *url.URL, encrypted bool) (int, chan model.ProviderResult)

func NewDelegatedTranslator(backend findFunc, streamingBackend findStreamFunc) (http.Handler, error) {
	finder := delegatedTranslator{backend, streamingBackend}
	m := http.NewServeMux()
	m.HandleFunc("/providers", finder.provide)
	m.HandleFunc("/encrypted/providers", finder.provide)
	m.HandleFunc("/providers/", func(w http.ResponseWriter, r *http.Request) { finder.find(w, r, false) })
	m.HandleFunc("/encrypted/providers/", func(w http.ResponseWriter, r *http.Request) { finder.find(w, r, true) })
	return m, nil
}

type delegatedTranslator struct {
	be  findFunc
	sbe findStreamFunc
}

func (dt *delegatedTranslator) provide(w http.ResponseWriter, r *http.Request) {
	_ = stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, r.Method)),
		stats.WithMeasurements(metrics.HttpDelegatedRoutingMethod.M(1)))

	h := w.Header()
	h.Add("Access-Control-Allow-Origin", "*")
	h.Add("Access-Control-Allow-Methods", "GET, OPTIONS")
	switch r.Method {
	case http.MethodOptions:
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		http.Error(w, "", http.StatusNotImplemented)
	default:
		h.Add("Allow", http.MethodGet)
		h.Add("Allow", http.MethodOptions)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (dt *delegatedTranslator) find(w http.ResponseWriter, r *http.Request, encrypted bool) {
	_ = stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, r.Method)),
		stats.WithMeasurements(metrics.HttpDelegatedRoutingMethod.M(1)))

	h := w.Header()
	h.Add("Access-Control-Allow-Origin", "*")
	h.Add("Access-Control-Allow-Methods", "GET, OPTIONS")
	switch r.Method {
	case http.MethodGet:
	case http.MethodOptions:
		w.WriteHeader(http.StatusOK)
		return
	default:
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	// Get the CID resource from the last element in the URL path.
	cidUrlParam := path.Base(r.URL.Path)

	// Translate URL by mapping `/providers/{CID}` to `/cid/{CID}`.
	uri := r.URL.JoinPath("../../cid", cidUrlParam)

	acc, err := getAccepts(r)
	if err != nil {
		http.Error(w, "invalid Accept header", http.StatusBadRequest)
		return
	}

	switch {
	case acc.ndjson:
		rcode, respChan := dt.sbe(r.Context(), findMethodDelegated, uri, encrypted)
		if rcode != http.StatusOK {
			http.Error(w, "", rcode)
			return
		}
		out := &drResp{}
		hasWritten := false
		encoder := json.NewEncoder(w)

		for rcrd := range respChan {
			if !hasWritten {
				w.Header().Set("Content-Type", mediaTypeNDJson)
				w.Header().Set("Connection", "Keep-Alive")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				w.WriteHeader(200)
				hasWritten = true
			}
			prov := drProvFromResult(rcrd)
			// if new
			if out.append(prov) {
				if err := encoder.Encode(prov); err != nil {
					return
				}
			}
		}
		if len(out.seenProviders) == 0 {
			// no response.
			w.WriteHeader(http.StatusNotFound)
		}
		return
	default:
	}

	rcode, resp := dt.be(r.Context(), http.MethodGet, findMethodDelegated, uri, encrypted)
	if rcode != http.StatusOK {
		http.Error(w, "", rcode)
		return
	}

	// reformat response.
	var parsed model.FindResponse
	if err := json.Unmarshal(resp, &parsed); err != nil {
		// server err
		log.Warnw("failed to parse backend response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	if len(parsed.MultihashResults) != 1 {
		// serverr
		log.Warnw("failed to parse backend response", "number_multihash", len(parsed.MultihashResults))
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	res := parsed.MultihashResults[0]

	out := &drResp{}

	// Records returned from IPNI via Delegated Routing don't have ContextID in them. Becuase of that,
	// some records that are valid from the IPNI point of view might look like duplicates from the Delegated Routing point of view.
	// To make the Delegated Routing output nicer, deduplicate identical records.

	for _, p := range res.ProviderResults {
		out.append(drProvFromResult(p))
	}

	outBytes, err := json.Marshal(out)
	if err != nil {
		log.Warnw("failed to serialize response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	}

	writeJsonResponse(w, http.StatusOK, outBytes)
}

type drResp struct {
	Providers     []drProvider
	seenProviders map[uint32]struct{}
}

func (dr *drResp) append(drp *drProvider) bool {
	capacity := len(drp.ID) + len(drp.Schema)
	for _, proto := range drp.Protocols {
		capacity += len(proto)
	}
	for _, meta := range drp.Metadata {
		capacity += len(meta)
	}
	drpb := make([]byte, 0, capacity)
	drpb = append(drpb, []byte(drp.ID)...)
	for _, proto := range drp.Protocols {
		drpb = append(drpb, []byte(proto)...)
	}
	drpb = append(drpb, []byte(drp.Schema)...)
	for _, meta := range drp.Metadata {
		drpb = append(drpb, meta...)
	}
	key := crc32.ChecksumIEEE(drpb)
	if _, ok := dr.seenProviders[key]; ok {
		return false
	}
	dr.seenProviders[key] = struct{}{}
	dr.Providers = append(dr.Providers, *drp)
	return true
}

type drProvider struct {
	Protocols []string
	Schema    string
	ID        peer.ID
	Addrs     []multiaddr.Multiaddr
	Metadata  map[string][]byte
}

func drProvFromResult(p model.ProviderResult) *drProvider {
	md := metadata.Default.New()
	err := md.UnmarshalBinary(p.Metadata)
	if err != nil {
		return &drProvider{
			Schema: peerSchema,
			ID:     p.Provider.ID,
			Addrs:  p.Provider.Addrs,
		}
	} else {
		provider := &drProvider{
			Schema:   peerSchema,
			ID:       p.Provider.ID,
			Addrs:    p.Provider.Addrs,
			Metadata: make(map[string][]byte),
		}

		for _, proto := range md.Protocols() {
			pl := md.Get(proto)
			plb, _ := pl.MarshalBinary()
			provider.Protocols = append(provider.Protocols, proto.String())
			provider.Metadata[proto.String()] = plb
		}
		return provider
	}
}

func (dp drProvider) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	if dp.Metadata != nil {
		for key, val := range dp.Metadata {
			m[key] = val
		}
	}

	m["Schema"] = dp.Schema
	m["ID"] = dp.ID

	if dp.Addrs != nil {
		m["Addrs"] = dp.Addrs
	}

	if dp.Protocols != nil {
		m["Protocols"] = dp.Protocols
	}

	return json.Marshal(m)
}
