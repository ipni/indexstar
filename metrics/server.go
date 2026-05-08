package metrics

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Global Labels
const (
	LabelErrKind      = "errKind"
	LabelMethod       = "method"
	LabelFound        = "found"
	LabelFoundCaskade = "foundCaskade"
	LabelFoundRegular = "foundRegular"
	LabelVersion      = "version"
	LabelTransport    = "transport"
	LabelProvider     = "provider"
)

// Measures
var (
	promRegistry = prometheus.NewRegistry()

	promAuto = promauto.With(promRegistry)

	FindLatency = promAuto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "indexstar_find_latency",
			Help: "Time to respond to a find request in milliseconds",
			Buckets: []float64{
				1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500,
				1000, 2000, 5000, 10_000, 20_000, 30_000, 50_000,
			},
		},
		[]string{LabelMethod, LabelFound, LabelFoundCaskade, LabelFoundRegular},
	)
	FindBackends = promAuto.NewGauge(
		prometheus.GaugeOpts{
			Name: "indexstar_find_backends",
			Help: "Backends reached in a find request",
		},
	)
	FindLoad = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_find_load",
			Help: "Amount of calls to find",
		},
		[]string{LabelMethod},
	)
	FindResponse = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_find_response",
			Help: "Find response stats",
		},
		[]string{LabelMethod, LabelTransport},
	)
	TopProvider = promAuto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "indexstar_top_provider",
			Help: "Top providers in responses",
		},
		[]string{LabelProvider},
	)
	HttpDelegatedRoutingMethod = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_http_delegated_routing_load",
			Help: "Amount of HTTP delegated routing calls by tagged method",
		},
		[]string{LabelMethod},
	)
)

func init() {
	promRegistry.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
}

func yesno(yn bool) string {
	if yn {
		return "yes"
	}
	return "no"
}

func ReportFindLatency(method string, found, foundCaskade, foundRegular bool, timeStarted time.Time) {
	FindLatency.WithLabelValues(
		method,
		yesno(found),
		yesno(foundCaskade),
		yesno(foundRegular),
	).Observe(
		time.Since(timeStarted).Seconds() * 1000,
	)
}

// WithMetrics creates an HTTP router for serving metric info
func WithMetrics() http.Handler {
	return promhttp.InstrumentMetricHandler(
		promRegistry,
		promhttp.HandlerFor(
			promRegistry,
			promhttp.HandlerOpts{},
		),
	)
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
