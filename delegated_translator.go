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
	"github.com/multiformats/go-multicodec"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

const (
	unknownProtocol = "unknown"
	unknownSchema   = unknownProtocol
)

type findFunc func(ctx context.Context, method, source string, req *url.URL, encrypted bool) (int, []byte)

func NewDelegatedTranslator(backend findFunc) (http.Handler, error) {
	finder := delegatedTranslator{backend}
	m := http.NewServeMux()
	m.HandleFunc("/providers", finder.provide)
	m.HandleFunc("/encrypted/providers", finder.provide)
	m.HandleFunc("/providers/", func(w http.ResponseWriter, r *http.Request) { finder.find(w, r, false) })
	m.HandleFunc("/encrypted/providers/", func(w http.ResponseWriter, r *http.Request) { finder.find(w, r, true) })
	return m, nil
}

type delegatedTranslator struct {
	be findFunc
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
	uri := r.URL.JoinPath("../cid", cidUrlParam)
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

	out := drResp{}

	// Records returned from IPNI via Delegated Routing don't have ContextID in them. Becuase of that,
	// some records that are valid from the IPNI point of view might look like duplicates from the Delegated Routing point of view.
	// To make the Delegated Routing output nicer, deduplicate identical records.
	uniqueProviders := map[uint32]struct{}{}
	appendIfUnique := func(drp *drProvider) {
		drpb := make([]byte, 0, len(drp.ID)+len(drp.Protocol)+len(drp.Schema)+len(drp.Metadata))
		drpb = append(drpb, []byte(drp.ID)...)
		drpb = append(drpb, []byte(drp.Protocol)...)
		drpb = append(drpb, []byte(drp.Schema)...)
		drpb = append(drpb, drp.Metadata...)
		key := crc32.ChecksumIEEE(drpb)
		if _, ok := uniqueProviders[key]; ok {
			return
		}
		uniqueProviders[key] = struct{}{}
		out.Providers = append(out.Providers, *drp)
	}

	for _, p := range res.ProviderResults {
		md := metadata.Default.New()
		err := md.UnmarshalBinary(p.Metadata)
		if err != nil {
			appendIfUnique(&drProvider{
				Protocol: unknownProtocol,
				Schema:   unknownSchema,
				ID:       p.Provider.ID,
				Addrs:    p.Provider.Addrs,
			})
		} else {
			for _, proto := range md.Protocols() {
				pl := md.Get(proto)
				plb, _ := pl.MarshalBinary()
				appendIfUnique(&drProvider{
					Protocol: proto.String(),
					Schema:   schemaByProtocolID(proto),
					ID:       p.Provider.ID,
					Addrs:    p.Provider.Addrs,
					Metadata: plb,
				})
			}
		}
	}

	outBytes, err := json.Marshal(out)
	if err != nil {
		log.Warnw("failed to serialize response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	}

	writeJsonResponse(w, http.StatusOK, outBytes)
}

func schemaByProtocolID(p multicodec.Code) string {
	switch p {
	case multicodec.TransportBitswap:
		return "bitswap"
	case multicodec.TransportGraphsyncFilecoinv1:
		return "graphsync-filecoinv1"
	default:
		return unknownProtocol
	}
}

type drResp struct {
	Providers []drProvider
}

type drProvider struct {
	Protocol string
	Schema   string
	ID       peer.ID
	Addrs    []multiaddr.Multiaddr
	Metadata []byte `json:",omitempty"`
}
