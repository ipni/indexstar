package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/filecoin-shipyard/indexstar/httpserver"
	"github.com/filecoin-shipyard/indexstar/metrics"
	logging "github.com/ipfs/go-log/v2"

	"github.com/urfave/cli/v2"
)

var log = logging.Logger("indexstar/mux")

type server struct {
	context.Context
	http.Client
	net.Listener
	metricsListener net.Listener
	servers         []*url.URL
	base            http.Handler
}

func NewServer(c *cli.Context) (*server, error) {
	bound, err := net.Listen("tcp", c.String("listen"))
	if err != nil {
		return nil, err
	}
	mb, err := net.Listen("tcp", c.String("metrics"))
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

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = config.Server.MaxIdleConns
	t.MaxConnsPerHost = config.Server.MaxConnsPerHost
	t.MaxIdleConnsPerHost = config.Server.MaxIdleConnsPerHost

	s := server{
		Context: c.Context,
		Client: http.Client{
			Timeout:   config.Server.HttpClientTimeout,
			Transport: t,
		},
		Listener:        bound,
		metricsListener: mb,
		servers:         surls,
		base:            httputil.NewSingleHostReverseProxy(surls[0]),
	}
	return &s, nil
}

func (s *server) Serve() chan error {
	ec := make(chan error)

	mux := http.NewServeMux()
	mux.HandleFunc("/cid/", s.find)
	mux.HandleFunc("/multihash", s.find)
	mux.HandleFunc("/multihash/", s.find)
	mux.HandleFunc("/providers", s.providers)
	mux.HandleFunc("/health", s.health)
	reframe, err := NewReframeHTTPHandler(s.servers)
	if err != nil {
		ec <- err
		close(ec)
		return ec
	}
	mux.HandleFunc("/reframe", reframe)
	mux.Handle("/", s)

	serv := http.Server{
		Handler: http.MaxBytesHandler(mux, config.Server.MaxRequestBodySize),
	}
	go func() {
		log.Infow("finder http server listening", "listen_addr", s.Listener.Addr())
		e := serv.Serve(s.Listener)
		if s.Context.Err() == nil {
			ec <- e
		}
	}()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Start(nil))
	metricsMux.Handle("/pprof", metrics.WithProfile())
	metricsServ := http.Server{
		Handler: http.MaxBytesHandler(metricsMux, config.Server.MaxRequestBodySize),
	}
	go func() {
		log.Infow("metrics server listening", "listen_addr", s.metricsListener.Addr())
		e := metricsServ.Serve(s.metricsListener)
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

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	httpserver.WriteJsonResponse(w, http.StatusOK, []byte("ready"))
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// default behavior.
	r.URL.Host = s.servers[0].Host
	r.URL.Scheme = s.servers[0].Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Header.Set("Host", s.servers[0].Host)
	s.base.ServeHTTP(w, r)
}
