package metrics

import (
	"net/http"
	"net/http/pprof"
	"runtime"

	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	"contrib.go.opencensus.io/exporter/prometheus"
	promclient "github.com/prometheus/client_golang/prometheus"
)

var log = logging.Logger("indexstar/metrics")

// Global Tags
var (
	ErrKind, _      = tag.NewKey("errKind")
	Method, _       = tag.NewKey("method")
	Found, _        = tag.NewKey("found")
	FoundCaskade, _ = tag.NewKey("foundCaskade")
	FoundRegular, _ = tag.NewKey("foundRegular")
	Version, _      = tag.NewKey("version")
	Transport, _    = tag.NewKey("transport")
	Provider, _     = tag.NewKey("provider")
)

// Measures
var (
	FindLatency                = stats.Float64("indexstar/find/latency", "Time to respond to a find request", stats.UnitMilliseconds)
	FindBackends               = stats.Float64("indexstar/find/backends", "Backends reached in a find request", stats.UnitDimensionless)
	FindLoad                   = stats.Int64("indexstar/find/load", "Amount of calls to find", stats.UnitDimensionless)
	FindResponse               = stats.Int64("indexstar/find/response", "Find response stats", stats.UnitDimensionless)
	TopProvider                = stats.Int64("indexstar/top_provider", "Top providers in responses", stats.UnitDimensionless)
	HttpDelegatedRoutingMethod = stats.Int64("indexstar/http_delegated_routing/load", "Amount of HTTP delegated routing calls by tagged method", stats.UnitDimensionless)
)

// Views
var (
	findLatencyView = &view.View{
		Measure:     FindLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
		TagKeys:     []tag.Key{Method, Found, FoundCaskade, FoundRegular},
	}
	findBackendView = &view.View{
		Measure:     FindBackends,
		Aggregation: view.LastValue(),
	}
	findLoadView = &view.View{
		Measure:     FindLoad,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Method},
	}
	findResponseView = &view.View{
		Measure:     FindResponse,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Method, Transport},
	}
	TopProviderView = &view.View{
		Measure:     TopProvider,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{Provider},
	}
	httpDelegRoutingMethodView = &view.View{
		Measure:     HttpDelegatedRoutingMethod,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Method},
	}
)

// Start creates an HTTP router for serving metric info
func Start(views []*view.View) http.Handler {
	// Register default views
	err := view.Register(
		findLatencyView,
		findBackendView,
		findLoadView,
		findResponseView,
		TopProviderView,
		httpDelegRoutingMethodView,
	)
	if err != nil {
		log.Errorf("cannot register metrics default views: %s", err)
	}
	// Register other views
	err = view.Register(views...)
	if err != nil {
		log.Errorf("cannot register metrics views: %s", err)
	}
	registry, ok := promclient.DefaultRegisterer.(*promclient.Registry)
	if !ok {
		log.Warnf("failed to export default prometheus registry; some metrics will be unavailable; unexpected type: %T", promclient.DefaultRegisterer)
	}
	exporter, err := prometheus.NewExporter(prometheus.Options{
		Registry:  registry,
		Namespace: "storetheindex",
	})
	if err != nil {
		log.Errorf("could not create the prometheus stats exporter: %v", err)
	}

	return exporter
}

func WithProfile() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/gc", func(w http.ResponseWriter, req *http.Request) {
		runtime.GC()
	})

	return mux
}
