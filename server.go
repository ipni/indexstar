package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"

	logging "github.com/ipfs/go-log/v2"

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

	mux := http.NewServeMux()
	mux.HandleFunc("/multihash/", s.find)
	reframe, err := NewReframeHTTPHandler(s.servers)
	if err != nil {
		ec <- err
		close(ec)
		return ec
	}
	mux.HandleFunc("/reframe/", reframe)
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
	go func() {
		defer close(ec)

		<-s.Context.Done()
		err := serv.Shutdown(s.Context)
		if err != nil {
			log.Warnw("failed shutdown", "err", err)
			ec <- err
		}
	}()
	return ec
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// default behavior.
	r.URL.Host = s.servers[0].Host
	r.URL.Scheme = s.servers[0].Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	s.base.ServeHTTP(w, r)
}
