package main

import (
	"cmp"
	"fmt"
	"net"
	"path"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/indexstar/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Throw-away sf vs sf2 find-result comparison. Not intended to merge.
//
// Compares responses from backends classified as sf vs sf2 on every find
// request. Emits discrepancy metrics, and logs sample CIDs whose entry counts
// differ by more than 20% (at most once per 15s).
//
// All comparison entry points recover from panics so this cannot take down
// indexstar. finish() reports in a background goroutine.

const (
	siteSf  = "sf"
	siteSf2 = "sf2"

	largeEntryDiffRatio = 0.20
	sampleLogInterval   = 15 * time.Second
	panicLogInterval    = time.Second
	maxLoggedProviders  = 8
)

// providers is keyed by the raw peer.ID bytes rather than its base58 form,
// which is ~90x cheaper per entry on large responses. Encoding happens only
// for the (small) exclusive-provider lists in diff.
type siteBag struct {
	done      bool
	entries   int
	providers map[peer.ID]int
}

type sfSf2Cmp struct {
	path string
	sf   Backend
	sf2  Backend

	sfBag siteBag
	s2Bag siteBag

	finished sync.Once
}

// providerDelta is the per-provider entry count on each site, recorded only
// when the two disagree. A zero on either side means the provider was absent
// from that site entirely.
type providerDelta struct {
	id  peer.ID
	sf  int
	sf2 int
}

func (p providerDelta) shortSite() (site string, missing int) {
	defer recoverCmp("shortSite")
	if p.sf2 < p.sf {
		return siteSf2, p.sf - p.sf2
	}
	return siteSf, p.sf2 - p.sf
}

type sfSf2Diff struct {
	incomplete bool
	equal      bool
	largeDiff  bool

	sfEntries  int
	sf2Entries int
	ratio      float64

	onlySf  []peer.ID
	onlySf2 []peer.ID

	// deltas covers every provider whose entry counts differ, which is what
	// distinguishes "sf2 never ingested this provider" from "sf2 is missing
	// some of this provider's ads".
	deltas []providerDelta

	exclusiveSfEntries  int
	exclusiveSf2Entries int
}

var (
	lastSampleLog atomic.Int64
	lastPanicLog  atomic.Int64
)

// recoverCmp swallows panics from comparison code so that this throw-away
// instrumentation cannot crash indexstar. Logs at most once per second.
func recoverCmp(method string) {
	p := recover()
	if p == nil {
		return
	}
	now := time.Now().UnixNano()
	last := lastPanicLog.Load()
	if now-last < int64(panicLogInterval) {
		return
	}
	if !lastPanicLog.CompareAndSwap(last, now) {
		return
	}
	log.Errorw(
		"sf/sf2 comparison panic",
		"method", method,
		"panic", p,
		"stack", string(debug.Stack()),
	)
}

func newSfSf2Cmp(reqPath string, backends []Backend) *sfSf2Cmp {
	defer recoverCmp("newSfSf2Cmp")
	var local, sf, sf2 Backend
	for _, b := range backends {
		if b == nil || b.URL() == nil {
			continue
		}

		host := b.URL().Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}

		switch host {
		case "find.sf.cid.contact":
			if sf == nil {
				sf = b
			}
		case "find.sf2.cid.contact":
			if sf2 == nil {
				sf2 = b
			}
		case "sf-indexer":
			if local == nil {
				local = b
			}
		}
	}
	if local != nil {
		if sf == nil {
			sf = local
		} else if sf2 == nil {
			sf2 = local
		}
	}
	if sf == nil || sf2 == nil {
		return nil
	}
	return &sfSf2Cmp{
		path:  reqPath,
		sf:    sf,
		sf2:   sf2,
		sfBag: siteBag{providers: make(map[peer.ID]int)},
		s2Bag: siteBag{providers: make(map[peer.ID]int)},
	}
}

func (c *sfSf2Cmp) bag(b Backend) *siteBag {
	defer recoverCmp("bag")
	if c == nil {
		return nil
	}
	switch b {
	case c.sf:
		return &c.sfBag
	case c.sf2:
		return &c.s2Bag
	}
	return nil
}

func (c *sfSf2Cmp) addResult(b Backend, r *encryptedOrPlainResult) {
	defer recoverCmp("addResult")
	bag := c.bag(b)
	if bag == nil {
		return
	}
	bag.entries++
	if r != nil && r.Provider != nil && r.Provider.ID != "" {
		bag.providers[r.Provider.ID]++
	}
}

func (c *sfSf2Cmp) addFindResponse(b Backend, resp *model.FindResponse) {
	defer recoverCmp("addFindResponse")
	bag := c.bag(b)
	if bag == nil {
		return
	}
	if resp != nil {
		for _, mhr := range resp.MultihashResults {
			for i := range mhr.ProviderResults {
				pr := &mhr.ProviderResults[i]
				bag.entries++
				if pr.Provider != nil && pr.Provider.ID != "" {
					bag.providers[pr.Provider.ID]++
				}
			}
		}
		for _, emr := range resp.EncryptedMultihashResults {
			bag.entries += len(emr.EncryptedValueKeys)
		}
	}
	bag.done = true
}

func (c *sfSf2Cmp) done(b Backend) {
	defer recoverCmp("done")
	bag := c.bag(b)
	if bag == nil {
		return
	}
	bag.done = true
}

func (c *sfSf2Cmp) diff() (d sfSf2Diff) {
	defer recoverCmp("diff")
	d.incomplete = true
	if c == nil {
		return
	}
	if !c.sfBag.done || !c.s2Bag.done {
		return
	}

	out := sfSf2Diff{
		sfEntries:  c.sfBag.entries,
		sf2Entries: c.s2Bag.entries,
	}
	den := max(out.sfEntries, out.sf2Entries)
	if den == 0 {
		out.ratio = 0
	} else {
		out.ratio = float64(abs(out.sfEntries-out.sf2Entries)) / float64(den)
	}
	out.largeDiff = out.ratio > largeEntryDiffRatio

	for pid, n := range c.sfBag.providers {
		n2, both := c.s2Bag.providers[pid]
		if !both {
			out.onlySf = append(out.onlySf, pid)
			out.exclusiveSfEntries += n
		}
		if n != n2 {
			out.deltas = append(out.deltas, providerDelta{id: pid, sf: n, sf2: n2})
		}
	}
	for pid, n := range c.s2Bag.providers {
		if _, both := c.sfBag.providers[pid]; !both {
			out.onlySf2 = append(out.onlySf2, pid)
			out.exclusiveSf2Entries += n
			out.deltas = append(out.deltas, providerDelta{id: pid, sf2: n})
		}
	}
	slices.Sort(out.onlySf)
	slices.Sort(out.onlySf2)
	// Biggest offenders first so that a clipped sample log shows the providers
	// that actually account for the difference.
	slices.SortFunc(out.deltas, func(a, b providerDelta) int {
		if n := cmp.Compare(abs(b.sf-b.sf2), abs(a.sf-a.sf2)); n != 0 {
			return n
		}
		return cmp.Compare(a.id, b.id)
	})

	// deltas subsumes the exclusive lists, and also catches the case where the
	// totals happen to match but the per-provider counts do not.
	out.equal = out.sfEntries == out.sf2Entries && len(out.deltas) == 0
	d = out
	return
}

func (c *sfSf2Cmp) finish() {
	defer recoverCmp("finish")
	if c == nil {
		return
	}
	c.finished.Do(func() {
		go func() {
			defer recoverCmp("finish.report")
			reportSfSf2Diff(c.path, c.diff())
		}()
	})
}

func reportSfSf2Diff(reqPath string, d sfSf2Diff) {
	defer recoverCmp("reportSfSf2Diff")
	if d.incomplete {
		metrics.ReportSfSf2Compare(metrics.SfSf2OutcomeIncomplete, 0, 0, 0, false, 0, 0)
		return
	}

	outcome := metrics.SfSf2OutcomeEqual
	if !d.equal {
		outcome = metrics.SfSf2OutcomeDiff
	}
	metrics.ReportSfSf2Compare(
		outcome,
		d.sfEntries,
		d.sf2Entries,
		d.ratio,
		d.largeDiff,
		len(d.onlySf),
		len(d.onlySf2),
	)

	for _, pid := range d.onlySf {
		metrics.ReportSfSf2ExclusiveProvider(siteSf, pid.String())
	}
	for _, pid := range d.onlySf2 {
		metrics.ReportSfSf2ExclusiveProvider(siteSf2, pid.String())
	}
	for _, pd := range d.deltas {
		site, missing := pd.shortSite()
		metrics.ReportSfSf2ProviderShort(site, pd.id.String(), missing)
	}

	if d.largeDiff {
		maybeLogSample(reqPath, d)
	}
}

func maybeLogSample(reqPath string, d sfSf2Diff) {
	defer recoverCmp("maybeLogSample")
	now := time.Now().UnixNano()
	last := lastSampleLog.Load()
	if now-last < int64(sampleLogInterval) {
		return
	}
	if !lastSampleLog.CompareAndSwap(last, now) {
		return
	}

	// Logged at error level so that it survives the GOLOG_LOG_LEVEL=ERROR
	// setting used in production.
	log.Errorw(
		"sf/sf2 large entry-count difference",
		"cid", path.Base(reqPath),
		"path", reqPath,
		"sfEntries", d.sfEntries,
		"sf2Entries", d.sf2Entries,
		"ratio", d.ratio,
		"providers", clipDeltas(d.deltas),
	)
}

// clipDeltas renders the providers accounting for the difference as
// "<peerID> sf=<n> sf2=<n>", most divergent first.
func clipDeltas(deltas []providerDelta) []string {
	defer recoverCmp("clipDeltas")
	n := min(len(deltas), maxLoggedProviders)
	clipped := make([]string, 0, n+1)
	for _, pd := range deltas[:n] {
		clipped = append(clipped, fmt.Sprintf("%s sf=%d sf2=%d", pd.id, pd.sf, pd.sf2))
	}
	if len(deltas) > n {
		clipped = append(clipped, fmt.Sprintf("...+%d", len(deltas)-n))
	}
	return clipped
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
