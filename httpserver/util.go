// Package httpserver provides functionality common to all storetheindex HTTP
// servers
package httpserver

import (
	"net/http"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("indexstar/http")

func WriteJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		log.Errorw("cannot write response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}
