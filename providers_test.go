package main

import (
	"context"
	"errors"
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

// fakeSource is a provider source whose FetchAll fails for its first failures
// calls
type fakeSource struct {
	name     string
	failures int32
	fetches  atomic.Int32
}

func (f *fakeSource) Fetch(context.Context, peer.ID) (*model.ProviderInfo, error) {
	return nil, nil
}

func (f *fakeSource) FetchAll(context.Context) ([]*model.ProviderInfo, error) {
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
		err := checkProviderBackends(t.Context(), []pcache.ProviderSource{up, flaky}, 3, retry)
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
		err := checkProviderBackends(t.Context(), []pcache.ProviderSource{up, down}, 3, retry)
		require.ErrorContains(t, err, "after 3 attempts")
		require.ErrorContains(t, err, "http://down/providers: backend unreachable")
		require.NotContains(t, err.Error(), "up:")
		require.Equal(t, 2*retry, time.Since(start))
		require.EqualValues(t, 1, up.fetches.Load())
		require.EqualValues(t, 3, down.fetches.Load())
	})
}

func TestCheckProviderBackendsAtLeastOneAttempt(t *testing.T) {
	down := &fakeSource{name: "down", failures: 100}
	err := checkProviderBackends(t.Context(), []pcache.ProviderSource{down}, 0, time.Hour)
	require.ErrorContains(t, err, "after 1 attempts")
	require.EqualValues(t, 1, down.fetches.Load())
}

func TestCheckProviderBackendsStopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		down := &fakeSource{name: "down", failures: 100}

		errCh := make(chan error, 1)
		go func() {
			errCh <- checkProviderBackends(ctx, []pcache.ProviderSource{down}, 5, time.Hour)
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
	config.Server.PcacheStartupRetry = time.Millisecond

	down := httptest.NewServer(nil)
	downURL := down.URL
	down.Close()

	err := runApp(t.Context(), []string{"indexstar",
		"--listen", "127.0.0.1:0",
		"--metrics", "127.0.0.1:0",
		"--backends", "http://127.0.0.1:1/",
		"--providersBackends", downURL,
	}, nil, 0, NewServer)
	require.ErrorContains(t, err, "cannot fetch providers after 2 attempts")
	require.ErrorContains(t, err, downURL)
}
