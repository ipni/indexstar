package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/indexstar/httpserver"
	"github.com/ipni/indexstar/metrics"

	"github.com/mercari/go-circuitbreaker"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("indexstar/mux")

type server struct {
	context.Context
	http.Client
	net.Listener
	metricsListener  net.Listener
	cfgBase          string
	servers          []*url.URL
	serverCallers    []*circuitbreaker.CircuitBreaker
	base             http.Handler
	translateReframe bool
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

	var surls []*url.URL
	if len(servers) == 0 {
		if !c.IsSet("config") {
			return nil, fmt.Errorf("no backends specified")
		}
		surls, err = Load(c.String("config"))
		if err != nil {
			return nil, fmt.Errorf("could not load backends from config: %w", err)
		}
	} else {
		surls = make([]*url.URL, len(servers))
		for i, s := range servers {
			surls[i], err = url.Parse(s)
			if err != nil {
				return nil, err
			}
		}
	}

	scallers := make([]*circuitbreaker.CircuitBreaker, len(surls))
	for i, surl := range surls {
		scallers[i] = circuitbreaker.New(
			circuitbreaker.WithFailOnContextCancel(false),
			circuitbreaker.WithHalfOpenMaxSuccesses(int64(config.Circuit.HalfOpenSuccesses)),
			circuitbreaker.WithOpenTimeout(config.Circuit.OpenTimeout),
			circuitbreaker.WithCounterResetInterval(config.Circuit.CounterReset),
			circuitbreaker.WithOnStateChangeHookFn(func(from, to circuitbreaker.State) {
				log.Infof("circuit state for %s changed from %s to %s", surl.String(), from, to)
			}))
	}

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = config.Server.MaxIdleConns
	t.MaxConnsPerHost = config.Server.MaxConnsPerHost
	t.MaxIdleConnsPerHost = config.Server.MaxIdleConnsPerHost
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout:   config.Server.DialerTimeout,
			KeepAlive: config.Server.DialerKeepAlive,
		}
		return dialer.DialContext(ctx, network, addr)
	}

	return &server{
		Context: c.Context,
		Client: http.Client{
			Timeout:   config.Server.HttpClientTimeout,
			Transport: t,
		},
		cfgBase:          c.String("config"),
		Listener:         bound,
		metricsListener:  mb,
		servers:          surls,
		serverCallers:    scallers,
		base:             httputil.NewSingleHostReverseProxy(surls[0]),
		translateReframe: c.Bool("translateReframe"),
	}, nil
}

func (s *server) Reload() error {
	surls, err := Load(s.cfgBase)
	if err != nil {
		return err
	}
	s.servers = surls
	s.base = httputil.NewSingleHostReverseProxy(surls[0])
	return nil
}

func (s *server) Serve() chan error {
	ec := make(chan error)

	mux := http.NewServeMux()
	mux.HandleFunc("/cid/", s.findCid)
	mux.HandleFunc("/multihash", s.findMultihash)
	mux.HandleFunc("/multihash/", s.findMultihashSubtree)
	mux.HandleFunc("/metadata/", s.findMetadataSubtree)
	mux.HandleFunc("/providers", s.providers)
	mux.HandleFunc("/providers/", s.provider)
	mux.HandleFunc("/health", s.health)

	if s.translateReframe {
		reframe, err := NewReframeTranslatorHTTPHandler(s.doFind)
		if err != nil {
			ec <- err
			close(ec)
			return ec
		}
		mux.HandleFunc("/reframe", reframe)
	} else {
		reframe, err := NewReframeHTTPHandler(s.servers)
		if err != nil {
			ec <- err
			close(ec)
			return ec
		}
		mux.HandleFunc("/reframe", reframe)
	}

	delegated, err := NewDelegatedTranslator(s.doFind)
	if err != nil {
		ec <- err
		close(ec)
		return ec
	}
	// Strip prefix URI since DelegatedTranslator uses a nested mux.
	mux.Handle("/routing/v1/", http.StripPrefix("/routing/v1", delegated))

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
	discardBody(r)
	if r.Method != http.MethodGet {
		http.Error(w, "", http.StatusNotFound)
		return
	}
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
