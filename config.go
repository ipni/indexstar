package main

import (
	"os"
	"strconv"
	"time"
)

var config struct {
	Reframe struct {
		MaxIdleConns        int
		MaxConnsPerHost     int
		MaxIdleConnsPerHost int
		DialerTimeout       time.Duration
		DialerKeepAlive     time.Duration
		HttpClientTimeout   time.Duration
		ResultMaxWait       time.Duration
	}
	Server struct {
		MaxIdleConns        int
		MaxConnsPerHost     int
		MaxIdleConnsPerHost int
		DialerTimeout       time.Duration
		DialerKeepAlive     time.Duration
		HttpClientTimeout   time.Duration
		ResultMaxWait       time.Duration
		MaxRequestBodySize  int64
	}
}

func init() {
	config.Reframe.MaxIdleConns = getEnvOrDefault("REFRAME_MAX_IDLE_CONNS", 100).(int)
	config.Reframe.MaxConnsPerHost = getEnvOrDefault("REFRAME_MAX_CONNS_PER_HOST", 100).(int)
	config.Reframe.MaxIdleConnsPerHost = getEnvOrDefault("REFRAME_MAX_IDLE_CONNS_PER_HOST", 100).(int)
	config.Reframe.DialerTimeout = getEnvOrDefault("REFRAME_DIALER_TIMEOUT", 10*time.Second).(time.Duration)
	config.Reframe.DialerKeepAlive = getEnvOrDefault("REFRAME_DIALER_KEEP_ALIVE", 15*time.Second).(time.Duration)
	config.Reframe.HttpClientTimeout = getEnvOrDefault("REFRAME_HTTP_CLIENT_TIMEOUT", 10*time.Second).(time.Duration)
	config.Reframe.ResultMaxWait = getEnvOrDefault("REFRAME_RESULT_MAX_WAIT", 5*time.Second).(time.Duration)

	config.Server.MaxIdleConns = getEnvOrDefault("SERVER_MAX_IDLE_CONNS", 100).(int)
	config.Server.MaxConnsPerHost = getEnvOrDefault("SERVER_MAX_CONNS_PER_HOST", 100).(int)
	config.Server.MaxIdleConnsPerHost = getEnvOrDefault("SERVER_MAX_IDLE_CONNS_PER_HOST", 100).(int)
	config.Server.DialerTimeout = getEnvOrDefault("SERVER_DIALER_TIMEOUT", 10*time.Second).(time.Duration)
	config.Server.DialerKeepAlive = getEnvOrDefault("SERVER_DIALER_KEEP_ALIVE", 15*time.Second).(time.Duration)
	config.Server.HttpClientTimeout = getEnvOrDefault("SERVER_HTTP_CLIENT_TIMEOUT", 10*time.Second).(time.Duration)
	config.Server.ResultMaxWait = getEnvOrDefault("SERVER_RESULT_MAX_WAIT", 5*time.Second).(time.Duration)
	config.Server.MaxRequestBodySize = getEnvOrDefault("SERVER_MAX_REQUEST_BODY_SIZE", int64(8<<10)).(int64) // 8KiB
}

func getEnvOrDefault(key string, def interface{}) interface{} {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	switch def.(type) {
	case int:
		pv, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.Warnf("Failed to parse %s=%s environment variable as int. Falling back on default %v", key, v, def)
			return def
		}
		return int(pv)
	case int64:
		pv, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			log.Warnf("Failed to parse %s=%s environment variable as int64. Falling back on default %v", key, v, def)
			return def
		}
		return pv
	case time.Duration:
		pv, err := time.ParseDuration(v)
		log.Warnf("Failed to parse %s=%s environment variable as time.Duration. Falling back on default %v", key, v, def)
		if err != nil {
			return def
		}
		return pv
	default:
		log.Warnf("Unknown type for %s=%s environment variable. Falling back on default %v", key, v, def)
		return def
	}
}
