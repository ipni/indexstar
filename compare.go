package main

import (
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

type sfSf2Diff struct {
	incomplete bool
	equal      bool
	largeDiff  bool

	sfEntries  int
	sf2Entries int
	ratio      float64

	onlySf  []peer.ID
	onlySf2 []peer.ID

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
		diff := d.sfEntries - d.sf2Entries
		if diff < 0 {
			diff = -diff
		}
		d.ratio = float64(diff) / float64(den)
	}
	d.largeDiff = d.ratio > largeEntryDiffRatio

	for pid, n := range c.sfBag.providers {
		if _, ok := c.s2Bag.providers[pid]; !ok {
			d.onlySf = append(d.onlySf, pid)
			d.exclusiveSfEntries += n
		}
	}
	for pid, n := range c.s2Bag.providers {
		if _, ok := c.sfBag.providers[pid]; !ok {
			d.onlySf2 = append(d.onlySf2, pid)
			d.exclusiveSf2Entries += n
		}
	}
	slices.Sort(d.onlySf)
	slices.Sort(d.onlySf2)

	d.equal = d.sfEntries == d.sf2Entries && len(d.onlySf) == 0 && len(d.onlySf2) == 0
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
		"onlySf", clipProviders(d.onlySf),
		"onlySf2", clipProviders(d.onlySf2),
	)
}

func clipProviders(ids []peer.ID) []string {
	n := min(len(ids), maxLoggedProviders)
	clipped := make([]string, 0, n+1)
	for _, id := range ids[:n] {
		clipped = append(clipped, id.String())
	}
	if len(ids) > n {
		clipped = append(clipped, fmt.Sprintf("...+%d", len(ids)-n))
	}
	return clipped
}
