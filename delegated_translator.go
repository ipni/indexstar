package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipni/indexstar/httpserver"
	"github.com/ipni/indexstar/metrics"
	"github.com/ipni/storetheindex/api/v0/finder/model"
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

func NewDelegatedTranslator(backend findFunc) (http.Handler, error) {
	finder := delegatedTranslator{backend}
	m := http.NewServeMux()
	m.HandleFunc("/providers", finder.provide)
	m.HandleFunc("/providers/", finder.find)
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
	h.Add("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte{})
		return
	}

	http.Error(w, "", http.StatusNotImplemented)
}

func (dt *delegatedTranslator) find(w http.ResponseWriter, r *http.Request) {
	_ = stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, r.Method)),
		stats.WithMeasurements(metrics.HttpDelegatedRoutingMethod.M(1)))

	// read out / close the request body.
	_, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		log.Warnw("failed to read original request body", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	h := w.Header()
	h.Add("Access-Control-Allow-Origin", "*")
	h.Add("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte{})
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "", http.StatusNotImplemented)
		return
	}

	// Translate URL by mapping `/providers/{CID}` to `/cid/{CID}`.
	cidUrlParam := strings.TrimPrefix(r.URL.Path, "/providers/")
	// TODO: replace with URL.JoinPath once upgraded to go 1.19
	findByCid := path.Join("/cid", cidUrlParam)
	if err != nil {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	uri, err := url.ParseRequestURI(findByCid)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	rcode, resp := dt.be(r.Context(), http.MethodGet, findMethodDelegated, uri, []byte{})

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

	}

	res := parsed.MultihashResults[0]

	out := drResp{}
	for _, p := range res.ProviderResults {
		md := metadata.Default.New()
		err := md.UnmarshalBinary(p.Metadata)
		if err != nil {
			out.Providers = append(out.Providers, drProvider{
				Protocol: unknownProtocol,
				Schema:   unknownSchema,
				ID:       p.Provider.ID,
				Addrs:    p.Provider.Addrs,
			})
		} else {
			for _, proto := range md.Protocols() {
				pl := md.Get(proto)
				plb, _ := pl.MarshalBinary()
				out.Providers = append(out.Providers, drProvider{
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

	httpserver.WriteJsonResponse(w, http.StatusOK, outBytes)
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
