package main

import (
	"mime"
	"net/http"
	"strings"
)

const (
	mediaTypeNDJson = "application/x-ndjson"
	mediaTypeJson   = "application/json"
	mediaTypeAny    = "*/*"
)

type accepts struct {
	any               bool
	ndjson            bool
	json              bool
	acceptHeaderFound bool
}

func getAccepts(r *http.Request) (accepts, error) {
	var a accepts
	values := r.Header.Values("Accept")
	a.acceptHeaderFound = len(values) > 0
	for _, accept := range values {
		amts := strings.Split(accept, ",")
		for _, amt := range amts {
			if mt, _, err := mime.ParseMediaType(amt); err != nil {
				return a, err
			} else if mt == mediaTypeNDJson {
				a.ndjson = true
			} else if mt == mediaTypeJson {
				a.json = true
			} else if mt == mediaTypeAny {
				a.any = true
			}
		}
	}
	return a, nil
}
