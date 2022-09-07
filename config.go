package main

import (
	"os"
	"strconv"
	"time"
)

const (
	defaultReframeMaxIdleConns              = 100
	defaultReframeMaxConnsPerHost           = 100
	defaultReframeMaxIdleConnsPerHost       = 100
	defaultReframeHttpClientTimeout         = 10 * time.Second
	defaultServerMaxIdleConns               = 100
	defaultServerMaxConnsPerHost            = 100
	defaultServerMaxIdleConnsPerHost        = 100
	defaultServerHttpClientTimeout          = 10 * time.Second
	defaultServerMaxRequestBodySize   int64 = 8 << 10 // 8KiB
)

var config struct {
	Reframe struct {
		MaxIdleConns        int
		MaxConnsPerHost     int
		MaxIdleConnsPerHost int
		HttpClientTimeout   time.Duration
	}
	Server struct {
		MaxIdleConns        int
		MaxConnsPerHost     int
		MaxIdleConnsPerHost int
		HttpClientTimeout   time.Duration
		MaxRequestBodySize  int64
	}
}

func init() {
	config.Reframe.MaxIdleConns = getEnvOrDefault[int]("REFRAME_MAX_IDLE_CONNS", defaultReframeMaxIdleConns)
	config.Reframe.MaxConnsPerHost = getEnvOrDefault[int]("REFRAME_MAX_CONNS_PER_HOST", defaultReframeMaxConnsPerHost)
	config.Reframe.MaxIdleConnsPerHost = getEnvOrDefault[int]("REFRAME_MAX_IDLE_CONNS_PER_HOST", defaultReframeMaxIdleConnsPerHost)
	config.Reframe.HttpClientTimeout = getEnvOrDefault[time.Duration]("REFRAME_HTTP_CLIENT_TIMEOUT", defaultReframeHttpClientTimeout)
	config.Server.MaxIdleConns = getEnvOrDefault[int]("SERVER_MAX_IDLE_CONNS", defaultServerMaxIdleConns)
	config.Server.MaxConnsPerHost = getEnvOrDefault[int]("SERVER_MAX_CONNS_PER_HOST", defaultServerMaxConnsPerHost)
	config.Server.MaxIdleConnsPerHost = getEnvOrDefault[int]("SERVER_MAX_IDLE_CONNS_PER_HOST", defaultServerMaxIdleConnsPerHost)
	config.Server.HttpClientTimeout = getEnvOrDefault[time.Duration]("SERVER_HTTP_CLIENT_TIMEOUT", defaultServerHttpClientTimeout)
	config.Server.MaxRequestBodySize = getEnvOrDefault[int64]("SERVER_MAX_REQUEST_BODY_SIZE", defaultServerMaxRequestBodySize)
}

func getEnvOrDefault[T any](key string, def T) T {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	switch any(def).(type) {
	case int:
		pv, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.Warnf("Failed to parse %s=%s environment variable as int. Falling back on default %v", key, v, def)
			return def
		}
		return any(int(pv)).(T)
	case int64:
		pv, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			log.Warnf("Failed to parse %s=%s environment variable as int64. Falling back on default %v", key, v, def)
			return def
		}
		return any(pv).(T)
	case time.Duration:
		pv, err := time.ParseDuration(v)
		log.Warnf("Failed to parse %s=%s environment variable as time.Duration. Falling back on default %v", key, v, def)
		if err != nil {
			return def
		}
		return any(pv).(T)
	default:
		log.Warnf("Unknown type for %s=%s environment variable. Falling back on default %v", key, v, def)
		return def
	}
}
