package main

import (
	"encoding/json"
	"net/http"
	"path"

	//"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (s *server) providers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	pinfos := s.pcache.List()

	// Write out combined.
	//
	// Note that /providers never returns 404. Instead, when there are no
	// providers, an empty JSON array is returned.
	outData, err := json.Marshal(pinfos)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	writeJsonResponse(w, http.StatusOK, outData)
}

// provider returns most recent state of a single provider.
func (s *server) provider(w http.ResponseWriter, r *http.Request) {
	pid, err := peer.Decode(path.Base(r.URL.Path))
	if err != nil {
		log.Warnw("bad provider ID", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	pinfo, err := s.pcache.Get(r.Context(), pid)
	if err != nil {
		log.Warnw("count not get provider information", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	if pinfo == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	outData, err := json.Marshal(pinfo)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	writeJsonResponse(w, http.StatusOK, outData)
}
