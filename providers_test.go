package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/pcache"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// fakeSource is a provider source whose FetchAll takes delay and fails for
// its first failures calls
type fakeSource struct {
	name     string
	delay    time.Duration
	failures int32
	fetches  atomic.Int32
}

func (f *fakeSource) Fetch(context.Context, peer.ID) (*model.ProviderInfo, error) {
	return nil, nil
}

func (f *fakeSource) FetchAll(context.Context) ([]*model.ProviderInfo, error) {
	time.Sleep(f.delay)
	if f.fetches.Add(1) <= f.failures {
		return nil, errors.New("backend unreachable")
	}
	return nil, nil
}

func (f *fakeSource) String() string { return f.name }

func TestCheckProviderBackendsRetriesFailedBackends(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const retry = 5 * time.Second
		up := &fakeSource{name: "up"}
		flaky := &fakeSource{name: "flaky", failures: 2}

		start := time.Now()
		_, err := checkProviderBackends(t.Context(), []pcache.ProviderSource{up, flaky}, 3, retry)
		require.NoError(t, err)
		require.Equal(t, 2*retry, time.Since(start))
		require.EqualValues(t, 1, up.fetches.Load())
		require.EqualValues(t, 3, flaky.fetches.Load())
	})
}

func TestCheckProviderBackendsFailsAfterAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const retry = 5 * time.Second
		up := &fakeSource{name: "up"}
		down := &fakeSource{name: "http://down/providers", failures: 100}

		start := time.Now()
		_, err := checkProviderBackends(t.Context(), []pcache.ProviderSource{up, down}, 3, retry)
		require.ErrorContains(t, err, "(attempts: 3)")
		require.ErrorContains(t, err, "http://down/providers: backend unreachable")
		require.NotContains(t, err.Error(), "up:")
		require.Equal(t, 2*retry, time.Since(start))
		require.EqualValues(t, 1, up.fetches.Load())
		require.EqualValues(t, 3, down.fetches.Load())
	})
}

func TestCheckProviderBackendsFetchesConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			retry   = 5 * time.Second
			timeout = 30 * time.Second
		)
		down1 := &fakeSource{name: "down1", delay: timeout, failures: 100}
		down2 := &fakeSource{name: "down2", delay: timeout, failures: 100}
		up := &fakeSource{name: "up", delay: timeout}

		// Each attempt takes as long as the slowest backend, not the sum of
		// all backends.
		start := time.Now()
		_, err := checkProviderBackends(t.Context(), []pcache.ProviderSource{down1, up, down2}, 3, retry)
		require.ErrorContains(t, err, "down1: backend unreachable")
		require.ErrorContains(t, err, "down2: backend unreachable")
		require.Equal(t, 3*timeout+2*retry, time.Since(start))
		require.EqualValues(t, 3, down1.fetches.Load())
		require.EqualValues(t, 3, down2.fetches.Load())
		require.EqualValues(t, 1, up.fetches.Load())
	})
}

func TestCheckProviderBackendsAtLeastOneAttempt(t *testing.T) {
	down := &fakeSource{name: "down", failures: 100}
	_, err := checkProviderBackends(t.Context(), []pcache.ProviderSource{down}, 0, time.Hour)
	require.ErrorContains(t, err, "(attempts: 1)")
	require.EqualValues(t, 1, down.fetches.Load())
}

func TestCheckProviderBackendsMinimumRetry(t *testing.T) {
	for _, retry := range []time.Duration{0, -time.Second, time.Millisecond} {
		synctest.Test(t, func(t *testing.T) {
			down := &fakeSource{name: "down", failures: 100}

			start := time.Now()
			_, err := checkProviderBackends(t.Context(), []pcache.ProviderSource{down}, 3, retry)
			require.ErrorContains(t, err, "(attempts: 3)")
			require.Equal(t, 2*minStartupRetry, time.Since(start))
			require.EqualValues(t, 3, down.fetches.Load())
		})
	}
}

func TestCheckProviderBackendsStopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		down := &fakeSource{name: "down", failures: 100}

		errCh := make(chan error, 1)
		go func() {
			_, err := checkProviderBackends(ctx, []pcache.ProviderSource{down}, 5, time.Hour)
			errCh <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-errCh, context.Canceled)
		require.EqualValues(t, 1, down.fetches.Load())
	})
}

func TestNewServerFailsWhenProvidersBackendIsDown(t *testing.T) {
	saved := config.Server
	t.Cleanup(func() { config.Server = saved })
	config.Server.PcacheStartupMaxAttempts = 2
	config.Server.PcacheStartupRetry = minStartupRetry

	// The providers backend is up but answers every request with an error
	var requests atomic.Int32
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)

	err := runApp(t.Context(), []string{"indexstar",
		"--listen", "127.0.0.1:0",
		"--metrics", "127.0.0.1:0",
		"--backends", "http://127.0.0.1:1/",
		"--providersBackends", down.URL,
	}, nil, 0, NewServer)
	require.ErrorContains(t, err, "cannot fetch providers (attempts: 2)")
	require.ErrorContains(t, err, down.URL)
	require.EqualValues(t, 2, requests.Load())
}

// An empty provider list is a successful answer; an error status or a body that
// is not a provider list is a failure
func TestCheckProviderBackendsHTTPResponses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{name: "empty list", status: http.StatusOK, body: `[]`},
		{name: "null list", status: http.StatusOK, body: `null`},
		{name: "providers", status: http.StatusOK, body: `[{"AddrInfo":{"ID":"12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC","Addrs":[]}}]`},
		{name: "not found", status: http.StatusNotFound, wantErr: true},
		{name: "server error", status: http.StatusInternalServerError, wantErr: true},
		{name: "invalid json", status: http.StatusOK, body: `{`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(backend.Close)
			src, err := pcache.NewHTTPSource(backend.URL, backend.Client())
			require.NoError(t, err)

			_, err = checkProviderBackends(t.Context(), []pcache.ProviderSource{src}, 1, minStartupRetry)
			if tc.wantErr {
				require.ErrorContains(t, err, backend.URL)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// The provider cache starts with the lists fetched by the check, without
// fetching them again
func TestCheckProviderBackendsPrefillsCache(t *testing.T) {
	const pid = "12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC"
	// The backend answers the first request and fails every later one
	var requests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) > 1 {
			http.Error(w, "", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`[{"AddrInfo":{"ID":"` + pid + `","Addrs":[]}}]`))
	}))
	t.Cleanup(backend.Close)
	src, err := pcache.NewHTTPSource(backend.URL, backend.Client())
	require.NoError(t, err)

	sources, err := checkProviderBackends(t.Context(), []pcache.ProviderSource{src}, 1, minStartupRetry)
	require.NoError(t, err)
	require.Len(t, sources, 1)
	require.Equal(t, src.String(), sources[0].String())

	pc, err := pcache.New(pcache.WithSource(sources...), pcache.WithRefreshInterval(0))
	require.NoError(t, err)
	infos := pc.List()
	require.Len(t, infos, 1)
	require.Equal(t, pid, infos[0].AddrInfo.ID.String())
	require.EqualValues(t, 1, requests.Load())

	// Later fetches go to the backend
	_, err = sources[0].FetchAll(t.Context())
	require.ErrorContains(t, err, "503")
	require.EqualValues(t, 2, requests.Load())
}

// The providers backends are checked before the finder listener is bound
func TestNewServerChecksProvidersBeforeListening(t *testing.T) {
	saved := config.Server
	t.Cleanup(func() { config.Server = saved })
	config.Server.PcacheStartupMaxAttempts = 1

	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)

	// Holding the listen address makes binding it fail, so the error shows
	// which step ran first
	held, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })

	err = runApp(t.Context(), []string{"indexstar",
		"--listen", held.Addr().String(),
		"--metrics", "127.0.0.1:0",
		"--backends", "http://127.0.0.1:1/",
		"--providersBackends", down.URL,
	}, nil, 0, NewServer)
	require.ErrorContains(t, err, "cannot fetch providers (attempts: 1)")
}
