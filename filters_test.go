package main

import (
	"net/http"
	"net/url"
	"slices"
	"testing"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestGetFilters(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    *filters
		wantErr string
	}{
		{
			name:  "none",
			query: "",
			want:  &filters{},
		},
		{
			name:  "include addrs",
			query: "filter-addrs=ip4",
			want: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
				},
			},
		},
		{
			name:  "exclude addrs",
			query: "filter-addrs=!ip4",
			want: &filters{
				addrs: &singleFilter{
					exclude: []string{"ip4"},
				},
			},
		},
		{
			name:  "include unknown addrs",
			query: "filter-addrs=unknown",
			want: &filters{
				addrs: &singleFilter{
					includeUnknown: true,
				},
			},
		},
		{
			name:  "include protocols",
			query: "filter-protocols=dht",
			want: &filters{

				protocols: &singleFilter{
					include: []string{"dht"},
				},
			},
		},
		{
			name:  "exclude protocols is not supported",
			query: "filter-protocols=!dht",
			want: &filters{
				protocols: &singleFilter{
					include: []string{"!dht"},
				},
			},
		},
		{
			name:  "include unknown protocols",
			query: "filter-protocols=unknown",
			want: &filters{
				protocols: &singleFilter{
					includeUnknown: true,
				},
			},
		},
		{
			name:    "multiple filter-addrs parameters",
			query:   "filter-addrs=ip4&filter-addrs=ip6",
			wantErr: "more than one filter-addrs get parameter provided",
		},
		{
			name:    "multiple filter-protocols parameters",
			query:   "filter-protocols=dht&filter-protocols=bitswap",
			wantErr: "more than one filter-protocols get parameter provided",
		},
		{
			name:  "mixed addrs and protocols filters",
			query: "filter-addrs=ip4,!tcp,ip6&filter-protocols=dht,bitswap",
			want: &filters{
				addrs: &singleFilter{
					include: []string{"ip4", "ip6"},
					exclude: []string{"tcp"},
				},
				protocols: &singleFilter{
					include: []string{"dht", "bitswap"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &http.Request{
				URL: &url.URL{
					RawQuery: tt.query,
				},
			}
			got, err := getFilters(r)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestApplyFilters(t *testing.T) {
	ip4tcp, err := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/8080")
	require.NoError(t, err)
	ip6tcp, err := ma.NewMultiaddr("/ip6/::1/tcp/8080")
	require.NoError(t, err)
	ip4udp, err := ma.NewMultiaddr("/ip4/1.2.3.4/udp/8080")
	require.NoError(t, err)

	tests := []struct {
		name          string
		filters       *filters
		provider      *drProvider
		wantPass      bool
		expectedAddrs []ma.Multiaddr
	}{
		// No filters tests
		{
			name:    "no filters, provider with addrs and protocols",
			filters: &filters{},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp, ip6tcp},
				Protocols: []string{"dht", "kad"},
			},
			wantPass: true,
		},
		{
			name:    "no filters, provider with empty addrs",
			filters: &filters{},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{},
				Protocols: []string{"dht"},
			},
			wantPass: true,
		},
		{
			name:    "no filters, provider with empty protocols",
			filters: &filters{},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{},
			},
			wantPass: true,
		},

		// Address include filters tests
		{
			name: "include ip4 addrs, provider matches",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"dht"},
			},
			wantPass: true,
		},
		{
			name: "include ip4 addrs, provider doesn't match",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip6tcp},
				Protocols: []string{"dht"},
			},
			wantPass: false,
		},
		{
			name: "include multiple addrs, provider matches one",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4", "ip6"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip6tcp},
				Protocols: []string{"dht"},
			},
			wantPass: true,
		},

		// Address exclude filters tests
		{
			name: "exclude tcp addrs, provider matches",
			filters: &filters{
				addrs: &singleFilter{
					exclude: []string{"tcp"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4udp},
				Protocols: []string{"dht"},
			},
			wantPass: true,
		},
		{
			name: "exclude tcp addrs, provider doesn't match",
			filters: &filters{
				addrs: &singleFilter{
					exclude: []string{"tcp"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"dht"},
			},
			wantPass: false,
		},
		{
			name: "exclude multiple addrs, provider has excluded protocol",
			filters: &filters{
				addrs: &singleFilter{
					exclude: []string{"tcp", "udp"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4udp},
				Protocols: []string{"dht"},
			},
			wantPass: false,
		},

		// Address mixed include/exclude filters tests
		{
			name: "include ip4, exclude tcp, provider matches with one address",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
					exclude: []string{"tcp"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp, ip4udp},
				Protocols: []string{"dht"},
			},
			wantPass:      true,
			expectedAddrs: []ma.Multiaddr{ip4udp},
		},
		{
			name: "include ip4, exclude tcp, provider excluded",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
					exclude: []string{"tcp"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"dht"},
			},
			wantPass: false,
		},

		// Address unknown filters tests
		{
			name: "include unknown addrs, provider has empty addrs",
			filters: &filters{
				addrs: &singleFilter{
					includeUnknown: true,
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{},
				Protocols: []string{"dht"},
			},
			wantPass: true,
		},
		{
			name: "include unknown addrs, provider has addrs",
			filters: &filters{
				addrs: &singleFilter{
					includeUnknown: true,
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"dht"},
			},
			wantPass: true,
		},

		// Protocol include filters tests
		{
			name: "include dht protocols, provider matches",
			filters: &filters{
				protocols: &singleFilter{
					include: []string{"dht"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"dht", "kad"},
			},
			wantPass: true,
		},
		{
			name: "include dht protocols, provider doesn't match",
			filters: &filters{
				protocols: &singleFilter{
					include: []string{"dht"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"bitswap"},
			},
			wantPass: false,
		},
		{
			name: "include multiple protocols, provider matches one",
			filters: &filters{
				protocols: &singleFilter{
					include: []string{"dht", "bitswap"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"bitswap", "kad"},
			},
			wantPass: true,
		},

		// Protocol unknown filters tests
		{
			name: "include unknown protocols, provider has empty protocols",
			filters: &filters{
				protocols: &singleFilter{
					includeUnknown: true,
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{},
			},
			wantPass: true,
		},
		{
			name: "include unknown protocols, provider has protocols",
			filters: &filters{
				protocols: &singleFilter{
					includeUnknown: true,
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"dht"},
			},
			wantPass: true,
		},

		// Combined addr and protocol filters tests
		{
			name: "include ip4 addrs and dht protocols, provider matches both",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
				},
				protocols: &singleFilter{
					include: []string{"dht"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"dht", "kad"},
			},
			wantPass: true,
		},
		{
			name: "include ip4 addrs and dht protocols, addrs don't match",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
				},
				protocols: &singleFilter{
					include: []string{"dht"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip6tcp},
				Protocols: []string{"dht"},
			},
			wantPass: false,
		},
		{
			name: "include ip4 addrs and dht protocols, protocols don't match",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
				},
				protocols: &singleFilter{
					include: []string{"dht"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4tcp},
				Protocols: []string{"bitswap"},
			},
			wantPass: false,
		},
		{
			name: "mixed addr and protocol filters",
			filters: &filters{
				addrs: &singleFilter{
					include: []string{"ip4"},
					exclude: []string{"tcp"},
				},
				protocols: &singleFilter{
					include: []string{"dht"},
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{ip4udp},
				Protocols: []string{"dht", "kad"},
			},
			wantPass: true,
		},
		{
			name: "include unknown addrs and unknown protocols, provider has empty both",
			filters: &filters{
				addrs: &singleFilter{
					includeUnknown: true,
				},
				protocols: &singleFilter{
					includeUnknown: true,
				},
			},
			provider: &drProvider{
				Addrs:     []ma.Multiaddr{},
				Protocols: []string{},
			},
			wantPass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialAddrs := slices.Clone(tt.provider.Addrs)

			got := tt.filters.apply(tt.provider)
			require.Equal(t, tt.wantPass, got)

			if tt.wantPass {
				if tt.expectedAddrs != nil {
					require.ElementsMatch(t, tt.expectedAddrs, tt.provider.Addrs)
				} else {
					require.ElementsMatch(t, initialAddrs, tt.provider.Addrs)
				}
			}

		})
	}

}
