package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/filecoin-shipyard/indexstar/httpserver"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("indexstar/mux")

type server struct {
	context.Context
	http.Client
	net.Listener
	servers []*url.URL
	base    http.Handler
}

func NewServer(c *cli.Context) (*server, error) {
	bound, err := net.Listen("tcp", c.String("listen"))
	if err != nil {
		return nil, err
	}
	servers := c.StringSlice("backends")
	if len(servers) == 0 {
		return nil, fmt.Errorf("no backends specified")
	}
	surls := make([]*url.URL, 0, len(servers))
	for _, s := range servers {
		surl, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		surls = append(surls, surl)
	}
	s := server{
		Context:  c.Context,
		Client:   http.Client{},
		Listener: bound,
		servers:  surls,
		base:     httputil.NewSingleHostReverseProxy(surls[0]),
	}
	return &s, nil
}

func (s *server) Serve() chan error {
	ec := make(chan error)
	defer close(ec)

	mux := http.NewServeMux()
	mux.HandleFunc("/multihash/", s.find)
	mux.Handle("/", s)

	serv := http.Server{
		Handler: mux,
	}
	go func() {
		log.Infow("finder http server listening", "listen_addr", s.Listener.Addr())
		e := serv.Serve(s.Listener)
		if s.Context.Err() == nil {
			ec <- e
		}
	}()
	select {
	case <-s.Context.Done():
		err := serv.Shutdown(s.Context)
		if err != nil {
			log.Warnw("failed shutdown", "err", err)
			ec <- err
		}
	}
	return ec
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// default behavior.
	r.URL.Host = s.servers[0].Host
	r.URL.Scheme = s.servers[0].Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	s.base.ServeHTTP(w, r)
}

func (s *server) find(w http.ResponseWriter, r *http.Request) {
	combined := make(chan *model.FindResponse, len(s.servers))
	wg := sync.WaitGroup{}
	for _, server := range s.servers {
		wg.Add(1)
		go func(server *url.URL) {
			defer wg.Done()
			r.URL.Host = server.Host
			r.URL.Scheme = server.Scheme
			r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
			resp, err := s.Client.Get(r.URL.String())
			if err != nil {
				log.Warnw("failed query", "backend", r.URL, "err", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				data, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Warnw("failed backend read", "backend", r.URL, "err", err)
					return
				}
				providers, err := model.UnmarshalFindResponse(data)
				if err == nil {
					combined <- providers
				} else {
					log.Warnw("failed backend unmarshal", "backend", r.URL, "err", err)
				}
			}
		}(server)
	}
	go func() {
		wg.Wait()
		close(combined)
	}()

	// TODO: stream out partial response as they come in.
	var resp model.FindResponse
outer:
	for prov := range combined {
		if resp.MultihashResults == nil {
			resp.MultihashResults = prov.MultihashResults
		} else {
			if !bytes.Equal(resp.MultihashResults[0].Multihash, prov.MultihashResults[0].Multihash) {
				// weird / invalid.
				log.Warnw("conflicting results", "q", r.URL.Path, "first", resp.MultihashResults[0].Multihash, "secnd", prov.MultihashResults[0].Multihash)

				httpserver.HandleError(w, fmt.Errorf("conflicting results"), "get")
				continue
			}
			for _, pr := range prov.MultihashResults[0].ProviderResults {
				for _, rr := range resp.MultihashResults[0].ProviderResults {
					if bytes.Equal(rr.ContextID, pr.ContextID) && bytes.Equal([]byte(rr.Provider.ID), []byte(pr.Provider.ID)) {
						continue outer
					}
				}
				resp.MultihashResults[0].ProviderResults = append(resp.MultihashResults[0].ProviderResults, pr)
			}
		}
	}

	if resp.MultihashResults == nil {
		http.Error(w, "no results for query", http.StatusNotFound)
		return
	}

	// write out combined.
	outData, err := model.MarshalFindResponse(&resp)
	if err != nil {
		log.Warnw("failed marshal response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, outData)
}
