package metrics

import (
	"fmt"
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
	LabelBackend      = "backend"
	LabelIsStreaming  = "isStreaming"
)

// Error kinds
type ErrKind struct{ string }

var (
	ErrKindNone            = ErrKind{"none"}
	ErrKindRequestCanceled = ErrKind{"request_canceled"}
	ErrKindRequestDeadline = ErrKind{"request_deadline_exceeded"}
	ErrKindRequestFailed   = ErrKind{"request_failed"}
	ErrKindReadCanceled    = ErrKind{"read_canceled"}
	ErrKindReadDeadline    = ErrKind{"read_deadline_exceeded"}
	ErrKindReadFailed      = ErrKind{"read_failed"}
	ErrKindUnmarshalFailed = ErrKind{"unmarshal_failed"}
	ErrKindNotFound        = ErrKind{"not_found"}
)

func ErrKindHttpStatus(statusCode int) ErrKind {
	return ErrKind{fmt.Sprintf("http_status_%d", statusCode)}
}

// Measures
var (
	promRegistry = prometheus.NewRegistry()

	promAuto = promauto.With(promRegistry)

	responseLatencyBuckets = []float64{
		0.001, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09,
		0.1, 0.2, 0.3, 0.4, 0.5, 1, 2, 5, 10, 20, 30, 50,
	}

	FindLatency = promAuto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "indexstar_find_latency_seconds",
			Help:    "Time to respond to a find request",
			Buckets: prometheus.DefBuckets,
		},
		[]string{LabelMethod, LabelFound, LabelFoundCaskade, LabelFoundRegular},
	)
	FindBackends = promAuto.NewGauge(
		prometheus.GaugeOpts{
			Name: "indexstar_find_backends",
			Help: "Backends reached in a find request",
		},
	)
	FindBackendLatency = promAuto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "indexstar_find_backend_latency_seconds",
			Help:    "Time to respond to a find request by backend",
			Buckets: responseLatencyBuckets,
		},
		[]string{LabelBackend, LabelMethod, LabelIsStreaming, LabelErrKind},
	)
	FindBackendEntriesFetched = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_find_backend_entries_fetched_total",
			Help: "Number of entries fetched from backends in find requests",
		},
		[]string{LabelBackend, LabelMethod, LabelIsStreaming, LabelErrKind},
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

func ReportFindLatency(
	method string,
	found bool,
	foundCaskade bool,
	foundRegular bool,
	duration time.Duration,
) {
	FindLatency.WithLabelValues(
		method,
		yesno(found),
		yesno(foundCaskade),
		yesno(foundRegular),
	).Observe(
		duration.Seconds(),
	)
}

func ReportFindBackendMetrics(
	backend string,
	method string,
	isStreaming bool,
	errKind ErrKind,
	duration time.Duration,
	validEntriesCount int,
	malformedEntriesCount int,
) {
	FindBackendLatency.WithLabelValues(
		backend,
		method,
		yesno(isStreaming),
		errKind.string,
	).Observe(
		duration.Seconds(),
	)

	if validEntriesCount > 0 {
		FindBackendEntriesFetched.WithLabelValues(
			backend,
			method,
			yesno(isStreaming),
			"",
		).Add(float64(validEntriesCount))
	}

	if malformedEntriesCount > 0 {
		FindBackendEntriesFetched.WithLabelValues(
			backend,
			method,
			yesno(isStreaming),
			"malformed",
		).Add(float64(malformedEntriesCount))
	}
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
