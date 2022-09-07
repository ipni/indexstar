package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_DefaultConfig(t *testing.T) {
	require.Equal(t, defaultReframeMaxIdleConns, config.Reframe.MaxIdleConns)
	require.Equal(t, defaultReframeMaxConnsPerHost, config.Reframe.MaxConnsPerHost)
	require.Equal(t, defaultReframeMaxIdleConnsPerHost, config.Reframe.MaxIdleConnsPerHost)
	require.Equal(t, defaultReframeHttpClientTimeout, config.Reframe.HttpClientTimeout)
	require.Equal(t, defaultServerMaxIdleConns, config.Server.MaxIdleConns)
	require.Equal(t, defaultServerMaxConnsPerHost, config.Server.MaxConnsPerHost)
	require.Equal(t, defaultServerMaxIdleConnsPerHost, config.Server.MaxIdleConnsPerHost)
	require.Equal(t, defaultServerHttpClientTimeout, config.Server.HttpClientTimeout)
	require.Equal(t, defaultServerMaxRequestBodySize, config.Server.MaxRequestBodySize)
}
