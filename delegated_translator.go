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
)

const (
	peerSchema = "peer"
)

type findFunc func(ctx context.Context, method, source string, req *url.URL, encrypted bool) (int, []byte)
type findStreamFunc func(ctx context.Context, method string, req *url.URL, encrypted bool) (int, chan model.ProviderResult)

func NewDelegatedTranslator(
	backend findFunc,
	streamingBackend findStreamFunc,
	cacheControlSuccessHeader string,
	cacheControlNotFoundHeader string,
	maxJSONEntries int,
) (http.Handler, error) {
	finder := delegatedTranslator{
		be:                         backend,
		sbe:                        streamingBackend,
		cacheControlSuccessHeader:  cacheControlSuccessHeader,
		cacheControlNotFoundHeader: cacheControlNotFoundHeader,
		maxJSONEntries:             maxJSONEntries,
	}
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

	cacheControlSuccessHeader  string
	cacheControlNotFoundHeader string
	maxJSONEntries             int
}

func (dt *delegatedTranslator) provide(w http.ResponseWriter, r *http.Request) {
	metrics.HttpDelegatedRoutingMethod.WithLabelValues(r.Method).Inc()

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
	metrics.HttpDelegatedRoutingMethod.WithLabelValues(r.Method).Inc()

	h := w.Header()
	h.Add("Access-Control-Allow-Origin", "*")
	h.Add("Access-Control-Allow-Methods", "GET, OPTIONS")
	h.Add("X-Content-Type-Options", "nosniff")
	h.Add("Vary", "Accept")

	switch r.Method {
	case http.MethodGet:
		// continue with the request

	case http.MethodOptions:
		w.WriteHeader(http.StatusOK)
		return

	default:
		h.Add("Allow", http.MethodGet+", "+http.MethodOptions)
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

	flt, err := getFilters(r)
	if err != nil {
		http.Error(w, "invalid filter query parameter", http.StatusBadRequest)
		return
	}

	switch {
	case acc.ndjson:
		dt.findNDJSon(r, uri, encrypted, w, flt)

	default:
		dt.findJSON(r, uri, encrypted, w, flt)
	}
}

func (dt *delegatedTranslator) findNDJSon(r *http.Request, uri *url.URL, encrypted bool, w http.ResponseWriter, flt *filters) {
	rcode, respChan := dt.sbe(r.Context(), findMethodDelegated, uri, encrypted)
	if rcode != http.StatusOK {
		http.Error(w, "", rcode)
		return
	}

	w.Header().Set("Content-Type", mediaTypeNDJson)
	w.Header().Set("Cache-Control", dt.cacheControlSuccessHeader)

	out := &drResp{}
	encoder := json.NewEncoder(w)

	for rcrd := range respChan {
		prov := drProvFromResult(rcrd)

		if !flt.apply(prov) {
			// provider does not pass the filters, skip it.
			continue
		}

		if !out.append(prov) {
			// duplicate provider, skip it.
			continue
		}

		if err := encoder.Encode(prov); err != nil {
			return
		}
	}

	if len(out.seenProviders) == 0 {
		// no response.
		w.Header().Set("Cache-Control", dt.cacheControlNotFoundHeader)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (dt *delegatedTranslator) findJSON(r *http.Request, uri *url.URL, encrypted bool, w http.ResponseWriter, flt *filters) {
	rcode, resp := dt.be(r.Context(), http.MethodGet, findMethodDelegated, uri, encrypted)
	if rcode == http.StatusNotFound {
		w.Header().Set("Cache-Control", dt.cacheControlNotFoundHeader)
		http.Error(w, "", http.StatusNotFound)
		return
	} else if rcode != http.StatusOK {
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
		prov := drProvFromResult(p)

		if !flt.apply(prov) {
			continue
		}

		out.append(prov)

		if len(out.seenProviders) >= dt.maxJSONEntries {
			break
		}
	}

	outBytes, err := json.Marshal(out)
	if err != nil {
		log.Warnw("failed to serialize response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	}

	w.Header().Set("Cache-Control", dt.cacheControlSuccessHeader)
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
	if dr.seenProviders == nil {
		dr.seenProviders = make(map[uint32]struct{})
	}
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
	m := map[string]any{}
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
