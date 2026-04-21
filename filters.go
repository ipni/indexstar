package main

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	ma "github.com/multiformats/go-multiaddr"
)

const (
	filterAddrs     = "filter-addrs"
	filterProtocols = "filter-protocols"
	unknownEntry    = "unknown"
)

type singleFilter struct {
	include        []string
	exclude        []string
	includeUnknown bool
}

type filters struct {
	addrs     *singleFilter
	protocols *singleFilter
}

func parseSingleFilter(arg string, withNegative bool) *singleFilter {
	f := &singleFilter{}

	for entry := range strings.SplitSeq(arg, ",") {
		entry := strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		entry = strings.ToLower(entry)

		if withNegative && entry[0] == '!' {
			f.exclude = append(f.exclude, entry[1:])
		} else if entry == unknownEntry {
			f.includeUnknown = true
		} else {
			f.include = append(f.include, entry)
		}
	}

	return f
}

func getFilters(r *http.Request) (*filters, error) {
	q := r.URL.Query()

	var addrsFilter, protocolsFilter *singleFilter

	if len(q[filterAddrs]) > 1 {
		return nil, fmt.Errorf("more than one %s get parameter provided", filterAddrs)
	} else if len(q[filterAddrs]) == 1 {
		addrsFilter = parseSingleFilter(q[filterAddrs][0], true)
	}

	if len(q[filterProtocols]) > 1 {
		return nil, fmt.Errorf("more than one %s get parameter provided", filterProtocols)
	} else if len(q[filterProtocols]) == 1 {
		protocolsFilter = parseSingleFilter(q[filterProtocols][0], false)
	}

	return &filters{
		addrs:     addrsFilter,
		protocols: protocolsFilter,
	}, nil
}

func (f *filters) apply(prov *drProvider) bool {
	return f.applyAddrs(prov) && f.applyProtocols(prov)
}

// applyAddrs implements address filtering according to
// https://specs.ipfs.tech/routing/http-routing-v1/#filter-addrs-providers-request-query-parameter
func (f *filters) applyAddrs(prov *drProvider) bool {
	if f.addrs == nil {
		return true
	}

	if len(prov.Addrs) == 0 {
		return f.addrs.includeUnknown
	}

	prov.Addrs = ma.FilterAddrs(prov.Addrs, func(addr ma.Multiaddr) bool {
		for _, component := range addr {
			// Any negative filter match excludes the address
			if slices.Contains(f.addrs.exclude, component.Protocol().Name) {
				return false
			}
		}

		if len(f.addrs.include) == 0 {
			return true
		}

		for _, component := range addr {
			// Any positive filter match includes the address
			if slices.Contains(f.addrs.include, component.Protocol().Name) {
				return true
			}
		}

		return false
	})

	return len(prov.Addrs) > 0
}

// applyProtocols implements protocol filtering according to
// https://specs.ipfs.tech/routing/http-routing-v1/#filter-protocols-providers-request-query-parameter
func (f *filters) applyProtocols(prov *drProvider) bool {
	if f.protocols == nil {
		return true
	}

	if len(prov.Protocols) == 0 {
		return f.protocols.includeUnknown
	}

	if len(f.protocols.include) == 0 {
		return true
	}

	for _, proto := range prov.Protocols {
		if slices.Contains(f.protocols.include, proto) {
			return true
		}
	}

	return false
}
