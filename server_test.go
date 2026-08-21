package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/nettest"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

type serverTestSuite struct {
	suite.Suite

	backendHandler http.HandlerFunc

	testBackendServer *httptest.Server

	srvListener     net.Listener
	metricsListener net.Listener

	srv        *server
	srvCancel  context.CancelFunc
	srvErrChan <-chan error
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(serverTestSuite))
}

func (s *serverTestSuite) SetupTest() {
	t := s.T()

	logging.SetDebugLogging()

	s.testBackendServer = httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if s.backendHandler == nil {
				panic("backend handler not set")
			}

			s.backendHandler(w, r)
		}),
	)

	listener, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err)
	s.srvListener = listener

	metricsListener, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err)
	s.metricsListener = metricsListener

	s.backendHandler = nil

	be, err := NewBackend(s.testBackendServer.URL, nil, Matchers.Any)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	s.srvCancel = cancel

	s.srv = &server{
		ctx:                   ctx,
		httpClient:            *http.DefaultClient,
		cfgBase:               "",
		listener:              s.srvListener,
		metricsListener:       s.metricsListener,
		backends:              []Backend{be},
		translateNonStreaming: false,
	}

	s.srvErrChan = s.srv.Serve()
}

func (s *serverTestSuite) TearDownTest() {
	s.srvCancel()

	for err := range s.srvErrChan {
		require.NoError(s.T(), err)
	}

	s.srvListener.Close()
	s.metricsListener.Close()
	s.testBackendServer.Close()
	logging.SetAllLoggers(logging.LevelError)
}

func writeOneLineJSON(t *testing.T, w io.Writer, format string, args ...any) {
	t.Helper()

	var data any
	err := json.Unmarshal([]byte(fmt.Sprintf(format, args...)), &data)
	require.NoError(t, err)

	err = json.NewEncoder(w).Encode(data)
	require.NoError(t, err)

	_, err = w.Write([]byte("\n"))
	require.NoError(t, err)
}

func (s *serverTestSuite) TestStreamingFind() {
	t := s.T()

	const cidStr = "bafybeigdyrzt5m6h6g5y2l3n4j5s7q4z6w7x8y9z0a1b2c3d4e5f6g7h8i9j0"

	s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, `/cid/`+cidStr, r.URL.Path)
		require.Equal(t, r.Header.Get("Accept"), "application/x-ndjson")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)

		writeOneLineJSON(t, w, `
			{
				"ContextID":"ctx1",
				"Metadata":"gBI=",
				"Provider":{
					"ID":"12D3KooWAGjvuFgSMiSdivCnxifF23ovdqb8j8nzYiEcdy6quL6a",
					"Addrs":[
						"/ip4/1.2.3.4/tcp/4001",
						"/ip4/1.2.3.4/udp/4001/quic-v1"
					]
				}
			}
		`)

		time.Sleep(10 * time.Millisecond)

		writeOneLineJSON(t, w, `
			{
				"ContextID":"ctx2",
				"Metadata":"kBKjaFBpZWNlQ0lE2CpYKAABgeIDkiAgRptffrqqNDd7gUDc3O0yFrSFUNuVqr/JLbTAvzSUCBRsVmVyaWZpZWREZWFs9W1GYXN0UmV0cmlldmFs9Q==",
				"Provider":{
					"ID":"12D3KooWLYDhmYYUnPzqu5nhj7kEuuDKWTdwHdPKUSF41TLXoqsi",
					"Addrs":[
						"/ip4/2.3.4.5/tcp/30003"
					]
				}
			}
		`)
	}

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("http://%s/routing/v1/providers/%s", s.srvListener.Addr(), cidStr),
		nil,
	)
	require.NoError(t, err)

	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	dataSplit := strings.Split(string(data), "\n")
	require.Len(t, dataSplit, 3)

	require.JSONEq(t, `
		{
			"ID": "12D3KooWAGjvuFgSMiSdivCnxifF23ovdqb8j8nzYiEcdy6quL6a",
			"Addrs": [
				"/ip4/1.2.3.4/tcp/4001",
				"/ip4/1.2.3.4/udp/4001/quic-v1"
			],
			"Protocols": [
				"transport-bitswap"
			],
			"Schema": "peer",
			"transport-bitswap": "gBI="
		}
	`, dataSplit[0])

	require.JSONEq(t, `
		{
			"ID": "12D3KooWLYDhmYYUnPzqu5nhj7kEuuDKWTdwHdPKUSF41TLXoqsi",
			"Addrs": [
				"/ip4/2.3.4.5/tcp/30003"
			],
			"Protocols": [
				"transport-graphsync-filecoinv1"
			],
			"Schema": "peer",
			"transport-graphsync-filecoinv1": "kBKjaFBpZWNlQ0lE2CpYKAABgeIDkiAgRptffrqqNDd7gUDc3O0yFrSFUNuVqr/JLbTAvzSUCBRsVmVyaWZpZWREZWFs9W1GYXN0UmV0cmlldmFs9Q=="
		}
	`, dataSplit[1])

	require.Empty(t, dataSplit[2])
}

func (s *serverTestSuite) TestFindCIDConvertsBackendJSONToNDJSON() {
	t := s.T()

	decoded, err := cid.Decode(testFindCID)
	require.NoError(t, err)

	s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, `/cid/`+testFindCID, r.URL.Path)
		require.Equal(t, r.Header.Get("Accept"), "application/x-ndjson")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		writeOneLineJSON(t, w, `
			{
				"MultihashResults": [{
					"Multihash": %[1]q,
					"ProviderResults": [{
						"ContextID": "Y3R4MQ==",
						"Metadata": "gBI=",
						"Provider": {
							"ID": %[2]q,
							"Addrs": [%[3]q]
						}
					}]
				}]
			}`,
			base64.StdEncoding.EncodeToString(decoded.Hash()), // %[1]
			testProviderID,   // %[2]
			testProviderAddr, // %[3]
		)
	}

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("http://%s/cid/%s", s.srvListener.Addr(), testFindCID),
		nil,
	)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/x-ndjson")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/x-ndjson", resp.Header.Get("Content-Type"))

	var result encryptedOrPlainResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Equal(t, testProviderID, result.Provider.ID.String())
	require.Equal(t, testProviderAddr, result.Provider.Addrs[0].String())
}

func (s *serverTestSuite) TestFindCIDDropsBackendJSONForUnexpectedMultihash() {
	t := s.T()

	other, err := cid.Decode("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")
	require.NoError(t, err)

	s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, `/cid/`+testFindCID, r.URL.Path)
		require.Equal(t, r.Header.Get("Accept"), "application/x-ndjson")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		writeOneLineJSON(t, w, `
			{
				"MultihashResults": [{
					"Multihash": %[1]q,
					"ProviderResults": [{
						"ContextID": "Y3R4MQ==",
						"Metadata": "gBI=",
						"Provider": {
							"ID": %[2]q,
							"Addrs": [%[3]q]
						}
					}]
				}]
			}`,
			base64.StdEncoding.EncodeToString(other.Hash()), // %[1]
			testProviderID,   // %[2]
			testProviderAddr, // %[3]
		)
	}

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("http://%s/cid/%s", s.srvListener.Addr(), testFindCID),
		nil,
	)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/x-ndjson")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func (s *serverTestSuite) TestFindCIDConvertsBackendNDJSONToJSON() {
	t := s.T()

	s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, `/cid/`+testFindCID, r.URL.Path)
		require.Equal(t, r.Header.Get("Accept"), "application/json")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		writeOneLineJSON(t, w, `
			{
				"ContextID":"Y3R4MQ==",
				"Metadata":"gBI=",
				"Provider":{
					"ID": %[1]q,
					"Addrs": [%[2]q]
				}
			}`,
			testProviderID,   // %[1]
			testProviderAddr, // %[2]
		)
	}

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("http://%s/cid/%s", s.srvListener.Addr(), testFindCID),
		nil,
	)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "application/json")

	var parsed struct {
		MultihashResults []struct {
			ProviderResults []struct {
				Provider struct {
					ID    string
					Addrs []string
				}
			}
		}
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&parsed))
	require.Len(t, parsed.MultihashResults, 1)
	require.Len(t, parsed.MultihashResults[0].ProviderResults, 1)
	require.Equal(t, testProviderID, parsed.MultihashResults[0].ProviderResults[0].Provider.ID)
	require.Equal(t, []string{testProviderAddr}, parsed.MultihashResults[0].ProviderResults[0].Provider.Addrs)
}

func (s *serverTestSuite) TestFindCIDBackendContentTypeIsPerResponse() {
	t := s.T()

	decoded, err := cid.Decode(testFindCID)
	require.NoError(t, err)

	var calls atomic.Int32
	s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, r.Header.Get("Accept"), "application/x-ndjson")
		call := calls.Add(1)
		if call == 1 {
			w.Header().Set("Content-Type", "application/json")
			writeOneLineJSON(t, w, `
				{
					"MultihashResults": [{
						"Multihash": %[1]q,
						"ProviderResults": [{
							"ContextID": "Y3R4MQ==",
							"Metadata": "gBI=",
							"Provider": {
								"ID": %[2]q,
								"Addrs": [%[3]q]
							}
						}]
					}]
				}`,
				base64.StdEncoding.EncodeToString(decoded.Hash()), // %[1]
				testProviderID,   // %[2]
				testProviderAddr, // %[3]
			)
			return
		}

		w.Header().Set("Content-Type", "application/x-ndjson")
		writeOneLineJSON(t, w, `
			{
				"ContextID":"Y3R4Mg==",
				"Metadata":"gBI=",
				"Provider":{
					"ID": %[1]q,
					"Addrs": [%[2]q]
				}
			}`,
			testProviderID,   // %[1]
			testProviderAddr, // %[2]
		)

	}

	doNDJSONFind := func() encryptedOrPlainResult {
		req, err := http.NewRequest(
			http.MethodGet,
			fmt.Sprintf("http://%s/cid/%s", s.srvListener.Addr(), testFindCID),
			nil,
		)
		require.NoError(t, err)
		req.Header.Set("Accept", "application/x-ndjson")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result encryptedOrPlainResult
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		return result
	}

	first := doNDJSONFind()
	require.Equal(t, []byte("ctx1"), first.ContextID)

	second := doNDJSONFind()
	require.Equal(t, []byte("ctx2"), second.ContextID)
	require.Equal(t, int32(2), calls.Load())
}

func (s *serverTestSuite) TestStreamingFindMalformedBackend() {
	t := s.T()

	const cidStr = "bafybeigdyrzt5m6h6g5y2l3n4j5s7q4z6w7x8y9z0a1b2c3d4e5f6g7h8i9j0"

	for _, data := range []string{
		`{"ContextID":"ctx1", "Metadata":"gBI="}`,
		`NOT-A-JSON_STRING`,
	} {
		s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, `/cid/`+cidStr, r.URL.Path)
			require.Equal(t, r.Header.Get("Accept"), "application/x-ndjson")
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(data))
		}

		req, err := http.NewRequest(
			http.MethodGet,
			fmt.Sprintf("http://%s/routing/v1/providers/%s", s.srvListener.Addr(), cidStr),
			nil,
		)
		require.NoError(t, err)

		req.Header.Set("Accept", "application/x-ndjson")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)

		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Empty(t, bytes.TrimSpace(data))
	}
}

func decodeNDJSONResults(t *testing.T, body io.Reader) []encryptedOrPlainResult {
	t.Helper()

	var results []encryptedOrPlainResult
	decoder := json.NewDecoder(body)
	for {
		var result encryptedOrPlainResult
		err := decoder.Decode(&result)
		if errors.Is(err, io.EOF) {
			return results
		}
		require.NoError(t, err)
		results = append(results, result)
	}
}

func (s *serverTestSuite) findCIDNDJSON(t *testing.T) *http.Response {
	t.Helper()

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("http://%s/cid/%s", s.srvListener.Addr(), testFindCID),
		nil,
	)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/x-ndjson")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func (s *serverTestSuite) backendHost(t *testing.T) string {
	t.Helper()

	u, err := url.Parse(s.testBackendServer.URL)
	require.NoError(t, err)
	return u.Host
}

func (s *serverTestSuite) TestFindCIDCountsValidAndMalformedNDJSONBackendEntries() {
	t := s.T()

	s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, `/cid/`+testFindCID, r.URL.Path)
		require.Equal(t, r.Header.Get("Accept"), "application/x-ndjson")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)

		writeOneLineJSON(t, w, `
			{
				"ContextID":"Y3R4MQ==",
				"Metadata":"gBI=",
				"Provider":{
					"ID": %[1]q,
					"Addrs": [%[2]q]
				}
			}`,
			testProviderID,
			testProviderAddr,
		)
		_, err := w.Write([]byte("\n"))
		require.NoError(t, err)
		_, err = w.Write([]byte(`{"ContextID":"Y3R4eA==","Metadata":"gBI="}` + "\n"))
		require.NoError(t, err)
		writeOneLineJSON(t, w, `
			{
				"ContextID":"Y3R4Mg==",
				"Metadata":"gBI=",
				"Provider":{
					"ID": %[1]q,
					"Addrs": [%[2]q]
				}
			}`,
			testProviderID,
			testProviderAddr,
		)
	}

	resp := s.findCIDNDJSON(t)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	results := decodeNDJSONResults(t, resp.Body)
	require.Len(t, results, 2)
	require.Equal(t, []byte("ctx1"), results[0].ContextID)
	require.Equal(t, []byte("ctx2"), results[1].ContextID)

	valid, malformed, unexpected := readFindBackendEntriesFetched(t, s.backendHost(t))
	require.Equal(t, 2.0, valid)
	require.Equal(t, 1.0, malformed)
	require.Zero(t, unexpected)
}

func (s *serverTestSuite) TestFindCIDCountsValidAndMalformedJSONBackendEntries() {
	t := s.T()

	decoded, err := cid.Decode(testFindCID)
	require.NoError(t, err)

	s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, `/cid/`+testFindCID, r.URL.Path)
		require.Equal(t, r.Header.Get("Accept"), "application/x-ndjson")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		writeOneLineJSON(t, w, `
			{
				"MultihashResults": [{
					"Multihash": %[1]q,
					"ProviderResults": [
						{
							"ContextID": "Y3R4MQ==",
							"Metadata": "gBI=",
							"Provider": {
								"ID": %[2]q,
								"Addrs": [%[3]q]
							}
						},
						{
							"ContextID": "Y3R4eA==",
							"Metadata": "gBI="
						},
						{
							"ContextID": "Y3R4Mg==",
							"Metadata": "gBI=",
							"Provider": {
								"ID": %[2]q,
								"Addrs": [%[3]q]
							}
						}
					]
				}]
			}`,
			base64.StdEncoding.EncodeToString(decoded.Hash()),
			testProviderID,
			testProviderAddr,
		)
	}

	resp := s.findCIDNDJSON(t)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	results := decodeNDJSONResults(t, resp.Body)
	require.Len(t, results, 2)
	require.Equal(t, []byte("ctx1"), results[0].ContextID)
	require.Equal(t, []byte("ctx2"), results[1].ContextID)

	valid, malformed, unexpected := readFindBackendEntriesFetched(t, s.backendHost(t))
	require.Equal(t, 2.0, valid)
	require.Equal(t, 1.0, malformed)
	require.Zero(t, unexpected)
}

func randomPeerID(t *testing.T) peer.ID {
	_, pub, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)

	id, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	return id
}

func (s *serverTestSuite) TestLargeJSONResponse() {
	t := s.T()

	type (
		list = []any
		dict = map[string]any
	)

	const cidStr = "QmeLvFK9dBLhC3kbfc58mLntUei6s7fZUGWsm1xJhczm1S"

	decodedCid, err := cid.Decode(cidStr)
	require.NoError(t, err)

	s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, `/cid/`+cidStr, r.URL.Path)
		require.Equal(t, r.Header.Get("Accept"), "application/json")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		data := list{}
		for i := range 200 {
			data = append(data, dict{
				"ContextID": "AXESIFBXwfY5v1krna9B2bzjlxEoRTG4avb/uIGFHJbGjtL4",
				"Metadata":  "oBIA",
				"Provider": dict{
					"ID": randomPeerID(t),
					"Addrs": list{
						fmt.Sprintf("/ip4/1.2.3.4/tcp/%d", 30000+i),
					},
				},
			})
		}
		err = json.NewEncoder(w).Encode(dict{
			"MultihashResults": list{dict{
				"Multihash":       decodedCid.Hash(),
				"ProviderResults": data,
			}},
		})
		require.NoError(t, err)
	}

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("http://%s/routing/v1/providers/%s", s.srvListener.Addr(), cidStr),
		nil,
	)
	require.NoError(t, err)

	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var response struct {
		Providers []struct{}
	}
	err = json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)
	require.Len(t, response.Providers, 100)
}

func (s *serverTestSuite) TestDelegatedRoutingResponseHeaders() {
	t := s.T()

	const cidStr = "QmeLvFK9dBLhC3kbfc58mLntUei6s7fZUGWsm1xJhczm1S"

	for _, dd := range []struct {
		Name string

		RequestNDJson bool
		RequestMethod string

		EmptyResponse  bool
		NoCacheControl bool

		ExpectedContentType    string
		ExpectedStatusCode     int
		ExpectedAllowedMethods []string
	}{
		{
			Name:                "JSON response",
			RequestNDJson:       false,
			RequestMethod:       http.MethodGet,
			EmptyResponse:       false,
			ExpectedContentType: "application/json",
			ExpectedStatusCode:  http.StatusOK,
		},
		{
			Name:                "NDJSON response",
			RequestNDJson:       true,
			RequestMethod:       http.MethodGet,
			EmptyResponse:       false,
			ExpectedContentType: "application/x-ndjson",
			ExpectedStatusCode:  http.StatusOK,
		},
		{
			Name:                "Empty JSON response",
			RequestNDJson:       false,
			RequestMethod:       http.MethodGet,
			EmptyResponse:       true,
			ExpectedContentType: "text/plain",
			ExpectedStatusCode:  http.StatusNotFound,
		},
		{
			Name:                "Empty NDJSON response",
			RequestNDJson:       true,
			RequestMethod:       http.MethodGet,
			EmptyResponse:       true,
			ExpectedContentType: "text/plain",
			ExpectedStatusCode:  http.StatusNotFound,
		},
		{
			Name:                   "Bad method for JSON",
			RequestNDJson:          false,
			RequestMethod:          http.MethodPost,
			EmptyResponse:          false,
			NoCacheControl:         true,
			ExpectedContentType:    "text/plain",
			ExpectedStatusCode:     http.StatusMethodNotAllowed,
			ExpectedAllowedMethods: []string{"GET", "OPTIONS"},
		},
		{
			Name:                   "Bad method for NDJSON",
			RequestNDJson:          true,
			RequestMethod:          http.MethodPost,
			EmptyResponse:          false,
			NoCacheControl:         true,
			ExpectedContentType:    "text/plain",
			ExpectedStatusCode:     http.StatusMethodNotAllowed,
			ExpectedAllowedMethods: []string{"GET", "OPTIONS"},
		},
	} {
		t.Run(dd.Name, func(t *testing.T) {
			s.backendHandler = func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, `/cid/`+cidStr, r.URL.Path)

				if dd.EmptyResponse {
					http.Error(w, "", http.StatusNotFound)
					return
				}

				if dd.RequestNDJson {
					require.Equal(t, r.Header.Get("Accept"), "application/x-ndjson")
					w.Header().Set("Content-Type", "application/x-ndjson")
					writeOneLineJSON(t, w, `
						{
							"ContextID":"ctx1",
							"Metadata":"gBI=",
							"Provider":{
								"ID":"12D3KooWAGjvuFgSMiSdivCnxifF23ovdqb8j8nzYiEcdy6quL6a",
								"Addrs":[
									"/ip4/1.2.3.4/tcp/30000"
								]
							}
						}
					`)
				} else {
					require.Equal(t, r.Header.Get("Accept"), "application/json")
					w.Header().Set("Content-Type", "application/json")
					writeOneLineJSON(t, w, `
						{
							"MultihashResults": [
								{
									"Multihash": "EiDtzI9MECNeznPpXjjXnrCpZ/Te+679GWm43DnGecaDIQ==",
									"ProviderResults": [
										{
											"ContextID": "AXESIFBXwfY5v1krna9B2bzjlxEoRTG4avb/uIGFHJbGjtL4",
											"Metadata":  "oBIA",
											"Provider": {
												"ID": "12D3KooWAGjvuFgSMiSdivCnxifF23ovdqb8j8nzYiEcdy6quL6a",
												"Addrs": [
													"/ip4/1.2.3.4/tcp/30000"
												]
											}
										}
									]
								}
							]
						}
					`)
				}
			}

			req, err := http.NewRequest(
				dd.RequestMethod,
				fmt.Sprintf("http://%s/routing/v1/providers/%s", s.srvListener.Addr(), cidStr),
				nil,
			)
			require.NoError(t, err)

			if dd.RequestNDJson {
				req.Header.Set("Accept", "application/x-ndjson")
			} else {
				req.Header.Set("Accept", "application/json")
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, resp.Header.Get("Access-Control-Allow-Origin"), "*")
			require.Equal(t, resp.Header.Get("Access-Control-Allow-Methods"), "GET, OPTIONS")
			require.Equal(t, resp.Header.Get("X-Content-Type-Options"), "nosniff")
			require.Equal(t, resp.Header.Get("Vary"), "Accept")

			if !dd.NoCacheControl {
				cc := resp.Header.Get("Cache-Control")
				require.Contains(t, cc, "public")
				require.Contains(t, cc, "max-age")
				require.Contains(t, cc, "s-maxage")
				require.Contains(t, cc, "stale-while-revalidate")
				require.Contains(t, cc, "stale-if-error")
			}

			require.Equal(t, dd.ExpectedStatusCode, resp.StatusCode)
			require.Contains(t, resp.Header.Get("Content-Type"), dd.ExpectedContentType)

			allowedMethods := []string{}
			for method := range strings.SplitSeq(resp.Header.Get("Allow"), ",") {
				if method = strings.TrimSpace(method); method != "" {
					allowedMethods = append(allowedMethods, method)
				}
			}

			require.ElementsMatch(t, dd.ExpectedAllowedMethods, allowedMethods)
		})
	}
}
