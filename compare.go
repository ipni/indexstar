package main

import (
	"cmp"
	"fmt"
	"net"
	"path"
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

const (
	siteSf  = "sf"
	siteSf2 = "sf2"

	largeEntryDiffRatio = 0.20
	sampleLogInterval   = 15 * time.Second
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

func (p providerDelta) shortSite() (string, int) {
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

var lastSampleLog atomic.Int64

func newSfSf2Cmp(reqPath string, backends []Backend) *sfSf2Cmp {
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
	bag := c.bag(b)
	if bag == nil {
		return
	}
	bag.done = true
}

func (c *sfSf2Cmp) diff() sfSf2Diff {
	if c == nil {
		return sfSf2Diff{incomplete: true}
	}

	if !c.sfBag.done || !c.s2Bag.done {
		return sfSf2Diff{incomplete: true}
	}

	d := sfSf2Diff{
		sfEntries:  c.sfBag.entries,
		sf2Entries: c.s2Bag.entries,
	}
	den := max(d.sfEntries, d.sf2Entries)
	if den == 0 {
		d.ratio = 0
	} else {
		d.ratio = float64(abs(d.sfEntries-d.sf2Entries)) / float64(den)
	}
	d.largeDiff = d.ratio > largeEntryDiffRatio

	for pid, n := range c.sfBag.providers {
		n2, both := c.s2Bag.providers[pid]
		if !both {
			d.onlySf = append(d.onlySf, pid)
			d.exclusiveSfEntries += n
		}
		if n != n2 {
			d.deltas = append(d.deltas, providerDelta{id: pid, sf: n, sf2: n2})
		}
	}
	for pid, n := range c.s2Bag.providers {
		if _, both := c.sfBag.providers[pid]; !both {
			d.onlySf2 = append(d.onlySf2, pid)
			d.exclusiveSf2Entries += n
			d.deltas = append(d.deltas, providerDelta{id: pid, sf2: n})
		}
	}
	slices.Sort(d.onlySf)
	slices.Sort(d.onlySf2)
	// Biggest offenders first so that a clipped sample log shows the providers
	// that actually account for the difference.
	slices.SortFunc(d.deltas, func(a, b providerDelta) int {
		if n := cmp.Compare(abs(b.sf-b.sf2), abs(a.sf-a.sf2)); n != 0 {
			return n
		}
		return cmp.Compare(a.id, b.id)
	})

	// deltas subsumes the exclusive lists, and also catches the case where the
	// totals happen to match but the per-provider counts do not.
	d.equal = d.sfEntries == d.sf2Entries && len(d.deltas) == 0
	return d
}

func (c *sfSf2Cmp) finish() {
	if c == nil {
		return
	}
	c.finished.Do(func() {
		reportSfSf2Diff(c.path, c.diff())
	})
}

func reportSfSf2Diff(reqPath string, d sfSf2Diff) {
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
