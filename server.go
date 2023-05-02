package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"

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
	metricsListener       net.Listener
	cfgBase               string
	backends              []Backend
	base                  http.Handler
	translateReframe      bool
	translateNonStreaming bool
}

// caskadeBackend is a marker for caskade backends
type caskadeBackend struct {
	Backend
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
	cascadeServers := c.StringSlice("cascadeBackends")

	if len(servers) == 0 {
		if !c.IsSet("config") {
			return nil, fmt.Errorf("no backends specified")
		}
		servers, err = Load(c.String("config"))
		if err != nil {
			return nil, fmt.Errorf("could not load backends from config: %w", err)
		}
	}

	backends, err := loadBackends(servers, cascadeServers)
	if err != nil {
		return nil, err
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
		cfgBase:               c.String("config"),
		Listener:              bound,
		metricsListener:       mb,
		backends:              backends,
		base:                  httputil.NewSingleHostReverseProxy(backends[0].URL()),
		translateReframe:      c.Bool("translateReframe"),
		translateNonStreaming: c.Bool("translateNonStreaming"),
	}, nil
}

func loadBackends(servers, cascadeServers []string) ([]Backend, error) {
	var backends []Backend
	for _, s := range servers {
		b, err := NewBackend(s, circuitbreaker.New(
			circuitbreaker.WithFailOnContextCancel(false),
			circuitbreaker.WithHalfOpenMaxSuccesses(int64(config.Circuit.HalfOpenSuccesses)),
			circuitbreaker.WithOpenTimeout(config.Circuit.OpenTimeout),
			circuitbreaker.WithCounterResetInterval(config.Circuit.CounterReset),
			circuitbreaker.WithOnStateChangeHookFn(func(from, to circuitbreaker.State) {
				log.Infof("circuit state for %s changed from %s to %s", s, from, to)
			})), Matchers.Any)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate backend: %w", err)
		}
		backends = append(backends, b)
	}

	for _, cs := range cascadeServers {
		matcher := Matchers.Any
		if config.Server.CascadeLabels != "" {
			labels := strings.Split(config.Server.CascadeLabels, ",")
			if len(labels) > 0 {
				labelMatchers := make([]HttpRequestMatcher, 0, len(labels))
				for _, label := range labels {
					labelMatchers = append(labelMatchers, Matchers.QueryParam("cascade", label))
				}
				matcher = Matchers.AnyOf(labelMatchers...)
			}
		}
		b, err := NewBackend(cs, circuitbreaker.New(
			circuitbreaker.WithFailOnContextCancel(false),
			circuitbreaker.WithHalfOpenMaxSuccesses(int64(config.CascadeCircuit.HalfOpenSuccesses)),
			circuitbreaker.WithOpenTimeout(config.CascadeCircuit.OpenTimeout),
			circuitbreaker.WithCounterResetInterval(config.CascadeCircuit.CounterReset),
			circuitbreaker.WithOnStateChangeHookFn(func(from, to circuitbreaker.State) {
				log.Infof("cascade circuit state for %s changed from %s to %s", cs, from, to)
			})), matcher)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate cascade backend: %w", err)
		}
		backends = append(backends, caskadeBackend{Backend: b})
	}

	if len(backends) == 0 {
		return nil, fmt.Errorf("no backends specified")
	}
	return backends, nil
}

func (s *server) Reload(cctx *cli.Context) error {
	surls, err := Load(s.cfgBase)
	if err != nil {
		return err
	}
	b, err := loadBackends(surls, cctx.StringSlice("cascadeBackends"))
	if err != nil {
		return err
	}
	s.backends = b
	s.base = httputil.NewSingleHostReverseProxy(b[0].URL())
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
		reframe, err := NewReframeHTTPHandler(s.backends)
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
	firstBackend := s.backends[0].URL()
	r.URL.Host = firstBackend.Host
	r.URL.Scheme = firstBackend.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Header.Set("Host", firstBackend.Host)
	s.base.ServeHTTP(w, r)
}
