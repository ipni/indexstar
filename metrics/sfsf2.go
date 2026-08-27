package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Throw-away sf vs sf2 comparison metrics.

const (
	LabelSite    = "site"
	LabelOutcome = "outcome"

	SfSf2OutcomeEqual      = "equal"
	SfSf2OutcomeDiff       = "diff"
	SfSf2OutcomeIncomplete = "incomplete"
)

var (
	SfSf2Compare = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_sfsf2_compare_total",
			Help: "Find requests compared between sf and sf2 (throw-away)",
		},
		[]string{LabelOutcome},
	)
	SfSf2Entries = promAuto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "indexstar_sfsf2_entries",
			Help:    "Entries returned per compared find request by site (throw-away)",
			Buckets: []float64{0, 1, 2, 5, 10, 20, 50, 100, 200, 500},
		},
		[]string{LabelSite},
	)
	SfSf2EntryDiffRatio = promAuto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "indexstar_sfsf2_entry_diff_ratio",
			Help:    "Relative |sf-sf2| / max(sf,sf2) entry-count difference (throw-away)",
			Buckets: []float64{0, 0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1},
		},
	)
	SfSf2LargeDiff = promAuto.NewCounter(
		prometheus.CounterOpts{
			Name: "indexstar_sfsf2_large_entry_diff_total",
			Help: "Compared finds where entry counts differ by more than 20% (throw-away)",
		},
	)
	SfSf2ExclusiveProviders = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_sfsf2_exclusive_providers_total",
			Help: "Times a provider appeared in only one of sf/sf2 for a find (throw-away)",
		},
		[]string{LabelSite},
	)
	SfSf2ExclusiveProvider = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_sfsf2_exclusive_provider",
			Help: "Times a given provider was exclusive to one site (throw-away)",
		},
		[]string{LabelSite, LabelProvider},
	)
	SfSf2ProviderShort = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_sfsf2_provider_short_total",
			Help: "Finds where a site returned fewer entries than the other for a given provider (throw-away)",
		},
		[]string{LabelSite, LabelProvider},
	)
	SfSf2ProviderShortEntries = promAuto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indexstar_sfsf2_provider_short_entries_total",
			Help: "Entries a site was missing relative to the other for a given provider (throw-away)",
		},
		[]string{LabelSite, LabelProvider},
	)
)

func ReportSfSf2Compare(
	outcome string,
	sfEntries, sf2Entries int,
	ratio float64,
	largeDiff bool,
	exclusiveSf, exclusiveSf2 int,
) {
	SfSf2Compare.WithLabelValues(outcome).Inc()
	if outcome == SfSf2OutcomeIncomplete {
		return
	}
	SfSf2Entries.WithLabelValues("sf").Observe(float64(sfEntries))
	SfSf2Entries.WithLabelValues("sf2").Observe(float64(sf2Entries))
	SfSf2EntryDiffRatio.Observe(ratio)
	if largeDiff {
		SfSf2LargeDiff.Inc()
	}
	if exclusiveSf > 0 {
		SfSf2ExclusiveProviders.WithLabelValues("sf").Add(float64(exclusiveSf))
	}
	if exclusiveSf2 > 0 {
		SfSf2ExclusiveProviders.WithLabelValues("sf2").Add(float64(exclusiveSf2))
	}
}

func ReportSfSf2ExclusiveProvider(site, provider string) {
	SfSf2ExclusiveProvider.WithLabelValues(site, provider).Inc()
}

// ReportSfSf2ProviderShort records that site returned missing fewer entries
// than the other site did for provider.
func ReportSfSf2ProviderShort(site, provider string, missing int) {
	SfSf2ProviderShort.WithLabelValues(site, provider).Inc()
	SfSf2ProviderShortEntries.WithLabelValues(site, provider).Add(float64(missing))
}
