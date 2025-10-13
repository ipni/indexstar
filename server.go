package main

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"net"
	"net/http"
	"strings"
	"text/template"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/pcache"
	"github.com/ipni/indexstar/metrics"
	"github.com/mercari/go-circuitbreaker"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var (
	log = logging.Logger("indexstar/mux")

	//go:embed *.html
	webUI embed.FS
)

const (
	backendsArg          = "backends"
	cascadeBackendsArg   = "cascadeBackends"
	dhBackendsArg        = "dhBackends"
	providersBackendsArg = "providersBackends"
)

type server struct {
	context.Context
	http.Client
	net.Listener
	metricsListener       net.Listener
	cfgBase               string
	backends              []Backend
	translateNonStreaming bool

	indexPage            []byte
	indexPageCompileTime time.Time
	pcache               *pcache.ProviderCache
	pcounts              *ProviderMap
}

// caskadeBackend is a marker for caskade backends
type caskadeBackend struct {
	Backend
}

type dhBackend struct {
	Backend
}

type providersBackend struct {
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
	servers := c.StringSlice(backendsArg)
	cascadeServers := c.StringSlice(cascadeBackendsArg)
	dhServers := c.StringSlice(dhBackendsArg)
	providersServers := c.StringSlice(providersBackendsArg)
	pCounts := NewProviderMap(config.Server.TopProviderCardinality)

	if len(servers) == 0 {
		if !c.IsSet("config") {
			return nil, fmt.Errorf("no backends specified")
		}
		servers, err = Load(c.String("config"))
		if err != nil {
			return nil, fmt.Errorf("could not load backends from config: %w", err)
		}
	}

	backends, err := loadBackends(servers, cascadeServers, dhServers, providersServers)
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

	httpClient := http.Client{
		Timeout:   config.Server.HttpClientTimeout,
		Transport: t,
	}

	var providerSources []pcache.ProviderSource
	for _, backend := range backends {
		// do not send providers requests to not providers backends
		if _, ok := backend.(providersBackend); !ok {
			continue
		}
		httpSrc, err := pcache.NewHTTPSource(backend.URL().String(), &httpClient)
		if err != nil {
			return nil, fmt.Errorf("cannot create http provider source: %w", err)
		}
		providerSources = append(providerSources, httpSrc)
	}
	pc, err := pcache.New(pcache.WithSource(providerSources...))
	if err != nil {
		return nil, fmt.Errorf("cannot create provider cache: %w", err)
	}

	indexTemplate, err := template.ParseFS(webUI, "index.html")
	if err != nil {
		return nil, err
	}
	var indexPageBuf bytes.Buffer
	if err = indexTemplate.Execute(&indexPageBuf, struct {
		URL string
	}{
		URL: c.String("homepageURL"),
	}); err != nil {
		return nil, err
	}
	compileTime := time.Now()

	s := server{
		Context:               c.Context,
		Client:                httpClient,
		cfgBase:               c.String("config"),
		Listener:              bound,
		metricsListener:       mb,
		backends:              backends,
		translateNonStreaming: c.Bool("translateNonStreaming"),
		indexPage:             indexPageBuf.Bytes(),
		indexPageCompileTime:  compileTime,
		pcache:                pc,
		pcounts:               pCounts,
	}

	go func() {
		for {
			select {
			case <-c.Context.Done():
				return
			case <-time.After(config.Server.TopProviderReportInterval):
			}
			s.updateTopProviders()
		}
	}()

	return &s, nil
}

func loadBackends(servers, cascadeServers, dhServers, providersServers []string) ([]Backend, error) {
	newBackendFunc := func(s string) (Backend, error) {
		return NewBackend(s, circuitbreaker.New(
			circuitbreaker.WithFailOnContextCancel(false),
			circuitbreaker.WithHalfOpenMaxSuccesses(int64(config.Circuit.HalfOpenSuccesses)),
			circuitbreaker.WithOpenTimeout(config.Circuit.OpenTimeout),
			circuitbreaker.WithCounterResetInterval(config.Circuit.CounterReset),
			circuitbreaker.WithOnStateChangeHookFn(func(from, to circuitbreaker.State) {
				log.Infof("circuit state for %s changed from %s to %s", s, from, to)
			})), Matchers.Any)
	}

	backends := make([]Backend, 0, len(servers)+len(dhServers)+len(providersServers)+len(cascadeServers))
	for _, s := range servers {
		b, err := newBackendFunc(s)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate backend: %w", err)
		}
		backends = append(backends, b)
	}
	for _, s := range dhServers {
		b, err := newBackendFunc(s)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate dh backend: %w", err)
		}
		backends = append(backends, dhBackend{Backend: b})
	}
	for _, s := range providersServers {
		b, err := newBackendFunc(s)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate provider backend: %w", err)
		}
		backends = append(backends, providersBackend{Backend: b})
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
	b, err := loadBackends(surls,
		cctx.StringSlice(cascadeBackendsArg),
		cctx.StringSlice(dhBackendsArg),
		cctx.StringSlice(providersBackendsArg))
	if err != nil {
		return err
	}
	s.backends = b

	return nil
}

func (s *server) updateTopProviders() {
	top := s.pcounts.Top()
	for _, rcrd := range top {
		_ = stats.RecordWithOptions(
			context.Background(),
			stats.WithTags(tag.Insert(metrics.Provider, rcrd.Provider)),
			stats.WithMeasurements(metrics.TopProvider.M(rcrd.Count)))
	}
}

func (s *server) Serve() chan error {
	mux := http.NewServeMux()
	mux.HandleFunc("/cid/", func(w http.ResponseWriter, r *http.Request) { s.findCid(w, r, false) })
	mux.HandleFunc("/encrypted/cid/", func(w http.ResponseWriter, r *http.Request) { s.findCid(w, r, true) })
	mux.HandleFunc("/multihash/", func(w http.ResponseWriter, r *http.Request) { s.findMultihashSubtree(w, r, false) })
	mux.HandleFunc("/encrypted/multihash/", func(w http.ResponseWriter, r *http.Request) { s.findMultihashSubtree(w, r, true) })
	mux.HandleFunc("/metadata/", s.findMetadataSubtree)
	mux.HandleFunc("/providers", s.providers)
	mux.HandleFunc("/providers/", s.provider)
	mux.HandleFunc("/health", s.health)
	mux.Handle("/metrics", metrics.Start(nil))

	ec := make(chan error)
	delegated, err := NewDelegatedTranslator(s.doFind, s.doFindStreaming)
	if err != nil {
		ec <- err
		close(ec)
		return ec
	}
	// Strip prefix URI since DelegatedTranslator uses a nested mux.
	mux.Handle("/routing/v1/", http.StripPrefix("/routing/v1", delegated))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Do not fall back on web-ui on unknown paths. Instead, strictly check the path and
		// return 404 on anything but "/" and "index.html". Otherwise, paths that are supported by
		// some backends and not others, like "/metadata" will return text/html.
		switch r.URL.Path {
		case "/", "/index.html":
			if r.Method == http.MethodGet {
				http.ServeContent(w, r, "index.html", s.indexPageCompileTime, bytes.NewReader(s.indexPage))
				return
			}
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		default:
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		}
	})

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
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	writeJsonResponse(w, http.StatusOK, []byte("ready"))
}

func writeJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	if _, err := w.Write(body); err != nil {
		log.Errorw("cannot write response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}
