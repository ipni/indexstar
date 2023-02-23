package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/mitchellh/go-homedir"
)

const (
	defaultReframeMaxIdleConns        = 100
	defaultReframeMaxConnsPerHost     = 100
	defaultReframeMaxIdleConnsPerHost = 100
	defaultReframeDialerTimeout       = 10 * time.Second
	defaultReframeDialerKeepAlive     = 15 * time.Second
	defaultReframeHttpClientTimeout   = 10 * time.Second
	defaultReframeResultMaxWait       = 5 * time.Second

	defaultServerMaxIdleConns               = 100
	defaultServerMaxConnsPerHost            = 100
	defaultServerMaxIdleConnsPerHost        = 100
	defaultServerDialerTimeout              = 10 * time.Second
	defaultServerDialerKeepAlive            = 15 * time.Second
	defaultServerHttpClientTimeout          = 30 * time.Second
	defaultServerResultMaxWait              = 5 * time.Second
	defaultServerResultStreamMaxWait        = 20 * time.Second
	defaultServerMaxRequestBodySize  int64  = 8 << 10 // 8KiB
	defaultServerCascadeLabels       string = ""      // 8KiB

	defaultCircuitHalfOpenSuccesses = 10
	defaultCircuitOpenTimeout       = 0
	defaultCircuitCounterReset      = 1 * time.Second

	defaultCascadeCircuitHalfOpenSuccesses = 10
	defaultCascadeCircuitOpenTimeout       = 0
	defaultCascadeCircuitCounterReset      = 1 * time.Second

	// DefaultPathName is the default config dir name.
	DefaultPathName = ".indexstar"
	// DefaultPathRoot is the path to the default config dir location.
	DefaultPathRoot = "~/" + DefaultPathName
	// DefaultConfigFile is the filename of the configuration file.
	DefaultConfigFile = "config"
	// EnvDir is the environment variable used to change the path root.
	EnvDir = "INDEXSTAR_PATH"
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
		ResultStreamMaxWait time.Duration
		MaxRequestBodySize  int64
		CascadeLabels       string
	}
	Circuit struct {
		HalfOpenSuccesses int
		OpenTimeout       time.Duration
		CounterReset      time.Duration
	}
	CascadeCircuit struct {
		HalfOpenSuccesses int
		OpenTimeout       time.Duration
		CounterReset      time.Duration
	}
}

func init() {
	config.Reframe.MaxIdleConns = getEnvOrDefault[int]("REFRAME_MAX_IDLE_CONNS", defaultReframeMaxIdleConns)
	config.Reframe.MaxConnsPerHost = getEnvOrDefault[int]("REFRAME_MAX_CONNS_PER_HOST", defaultReframeMaxConnsPerHost)
	config.Reframe.MaxIdleConnsPerHost = getEnvOrDefault[int]("REFRAME_MAX_IDLE_CONNS_PER_HOST", defaultReframeMaxIdleConnsPerHost)
	config.Reframe.DialerTimeout = getEnvOrDefault[time.Duration]("REFRAME_DIALER_TIMEOUT", defaultReframeDialerTimeout)
	config.Reframe.DialerKeepAlive = getEnvOrDefault[time.Duration]("REFRAME_DIALER_KEEP_ALIVE", defaultReframeDialerKeepAlive)
	config.Reframe.HttpClientTimeout = getEnvOrDefault[time.Duration]("REFRAME_HTTP_CLIENT_TIMEOUT", defaultReframeHttpClientTimeout)
	config.Reframe.ResultMaxWait = getEnvOrDefault[time.Duration]("REFRAME_RESULT_MAX_WAIT", defaultReframeResultMaxWait)

	config.Server.MaxIdleConns = getEnvOrDefault[int]("SERVER_MAX_IDLE_CONNS", defaultServerMaxIdleConns)
	config.Server.MaxConnsPerHost = getEnvOrDefault[int]("SERVER_MAX_CONNS_PER_HOST", defaultServerMaxConnsPerHost)
	config.Server.MaxIdleConnsPerHost = getEnvOrDefault[int]("SERVER_MAX_IDLE_CONNS_PER_HOST", defaultServerMaxIdleConnsPerHost)
	config.Server.DialerTimeout = getEnvOrDefault[time.Duration]("SERVER_DIALER_TIMEOUT", defaultServerDialerTimeout)
	config.Server.DialerKeepAlive = getEnvOrDefault[time.Duration]("SERVER_DIALER_KEEP_ALIVE", defaultServerDialerKeepAlive)
	config.Server.HttpClientTimeout = getEnvOrDefault[time.Duration]("SERVER_HTTP_CLIENT_TIMEOUT", defaultServerHttpClientTimeout)
	config.Server.ResultMaxWait = getEnvOrDefault[time.Duration]("SERVER_RESULT_MAX_WAIT", defaultServerResultMaxWait)
	config.Server.ResultStreamMaxWait = getEnvOrDefault[time.Duration]("SERVER_RESULT_STREAM_MAX_WAIT", defaultServerResultStreamMaxWait)
	config.Server.MaxRequestBodySize = getEnvOrDefault[int64]("SERVER_MAX_REQUEST_BODY_SIZE", defaultServerMaxRequestBodySize)
	config.Server.CascadeLabels = getEnvOrDefault[string]("SERVER_CASCADE_LABELS", defaultServerCascadeLabels)

	config.Circuit.HalfOpenSuccesses = getEnvOrDefault[int]("CIRCUIT_HALF_OPEN_SUCCESSES", defaultCircuitHalfOpenSuccesses)
	config.Circuit.OpenTimeout = getEnvOrDefault[time.Duration]("CIRCUIT_OPEN_TIMEOUT", defaultCircuitOpenTimeout)
	config.Circuit.CounterReset = getEnvOrDefault[time.Duration]("CIRCUIT_COUNTER_RESET", defaultCircuitCounterReset)

	config.CascadeCircuit.HalfOpenSuccesses = getEnvOrDefault[int]("CASCADE_CIRCUIT_HALF_OPEN_SUCCESSES", defaultCascadeCircuitHalfOpenSuccesses)
	config.CascadeCircuit.OpenTimeout = getEnvOrDefault[time.Duration]("CASCADE_CIRCUIT_OPEN_TIMEOUT", defaultCascadeCircuitOpenTimeout)
	config.CascadeCircuit.CounterReset = getEnvOrDefault[time.Duration]("CASCADE_CIRCUIT_COUNTER_RESET", defaultCascadeCircuitCounterReset)
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
		if err != nil {
			log.Warnf("Failed to parse %s=%s environment variable as time.Duration. Falling back on default %v", key, v, def)
			return def
		}
		return any(pv).(T)
	case string:
		if v == "" {
			return def
		}
		return any(v).(T)
	default:
		log.Warnf("Unknown type for %s=%s environment variable. Falling back on default %v", key, v, def)
		return def
	}
}

var (
	ErrInitialized    = errors.New("configuration file already exists")
	ErrNotInitialized = errors.New("not initialized")
)

// Filename returns the configuration file path given a configuration root
// directory. If the configuration root directory is empty, use the default.
func Filename(configRoot string) (string, error) {
	return Path(configRoot, DefaultConfigFile)
}

// Path returns the config file path relative to the configuration root. If an
// empty string is provided for `configRoot`, the default root is used. If
// configFile is an absolute path, then configRoot is ignored.
func Path(configRoot, configFile string) (string, error) {
	if filepath.IsAbs(configFile) {
		return filepath.Clean(configFile), nil
	}
	if configRoot == "" {
		var err error
		configRoot, err = PathRoot()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(configRoot, configFile), nil
}

// PathRoot returns the default configuration root directory.
func PathRoot() (string, error) {
	dir := os.Getenv(EnvDir)
	if dir != "" {
		return dir, nil
	}
	return homedir.Expand(DefaultPathRoot)
}

func Load(filePath string) ([]string, error) {
	var err error
	if filePath == "" {
		filePath, err = Filename("")
		if err != nil {
			return nil, err
		}
	}

	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotInitialized
		}
		return nil, err
	}
	defer f.Close()

	var urls []string
	if err = json.NewDecoder(f).Decode(&urls); err != nil {
		return nil, err
	}
	return urls, nil
}
