package main

import (
	"encoding/json"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const (
	testFindCID      = "QmeLvFK9dBLhC3kbfc58mLntUei6s7fZUGWsm1xJhczm1S"
	testProviderID   = "12D3KooWAGjvuFgSMiSdivCnxifF23ovdqb8j8nzYiEcdy6quL6a"
	testProviderAddr = "/ip4/1.2.3.4/tcp/4001"
)

func testProviderResult(t *testing.T) model.ProviderResult {
	t.Helper()

	pid, err := peer.Decode(testProviderID)
	require.NoError(t, err)
	addr, err := multiaddr.NewMultiaddr(testProviderAddr)
	require.NoError(t, err)

	return model.ProviderResult{
		ContextID: []byte("ctx1"),
		Metadata:  []byte{0x80, 0x12},
		Provider: &peer.AddrInfo{
			ID:    pid,
			Addrs: []multiaddr.Multiaddr{addr},
		},
	}
}

func TestResultsFromFindResponseAndBack(t *testing.T) {
	decoded, err := cid.Decode(testFindCID)
	require.NoError(t, err)

	pr := testProviderResult(t)
	encKey := []byte{0x01, 0x02, 0x03}
	resp := &model.FindResponse{
		MultihashResults: []model.MultihashResult{
			{
				Multihash:       decoded.Hash(),
				ProviderResults: []model.ProviderResult{pr},
			},
		},
		EncryptedMultihashResults: []model.EncryptedMultihashResult{
			{
				Multihash:          decoded.Hash(),
				EncryptedValueKeys: [][]byte{encKey},
			},
		},
	}

	results, skipped := resultsFromFindResponse(resp, decoded.Hash())
	require.Zero(t, skipped)
	require.Len(t, results, 2)
	require.True(t, usableResult(results[0]))
	require.True(t, usableResult(results[1]))
	require.Equal(t, pr.Provider.ID, results[0].Provider.ID)
	require.Equal(t, encKey, results[1].EncryptedValueKey)

	var ndjson []byte
	for _, result := range results {
		line, err := json.Marshal(result)
		require.NoError(t, err)
		ndjson = append(ndjson, line...)
		ndjson = append(ndjson, '\n')
	}

	got, err := findResponseFromNDJSON(ndjson, decoded.Hash())
	require.NoError(t, err)
	require.Len(t, got.MultihashResults, 1)
	require.Equal(t, decoded.Hash(), got.MultihashResults[0].Multihash)
	require.Equal(t, pr.Provider.ID, got.MultihashResults[0].ProviderResults[0].Provider.ID)
	require.Len(t, got.EncryptedMultihashResults, 1)
	require.Equal(t, encKey, got.EncryptedMultihashResults[0].EncryptedValueKeys[0])
}

func TestResultsFromFindResponseSkipsUnexpectedMultihash(t *testing.T) {
	decoded, err := cid.Decode(testFindCID)
	require.NoError(t, err)
	other, err := cid.Decode("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")
	require.NoError(t, err)

	matching := testProviderResult(t)
	otherPR := matching
	otherPR.ContextID = []byte("other")

	resp := &model.FindResponse{
		MultihashResults: []model.MultihashResult{
			{
				Multihash:       other.Hash(),
				ProviderResults: []model.ProviderResult{otherPR},
			},
			{
				Multihash:       decoded.Hash(),
				ProviderResults: []model.ProviderResult{matching},
			},
			{
				ProviderResults: []model.ProviderResult{matching},
			},
		},
		EncryptedMultihashResults: []model.EncryptedMultihashResult{
			{
				Multihash:          other.Hash(),
				EncryptedValueKeys: [][]byte{{0xaa}},
			},
			{
				Multihash:          decoded.Hash(),
				EncryptedValueKeys: [][]byte{{0xbb}},
			},
		},
	}

	results, skipped := resultsFromFindResponse(resp, decoded.Hash())
	require.Equal(t, 3, skipped)
	require.Len(t, results, 2)
	require.Equal(t, matching.ContextID, results[0].ContextID)
	require.Equal(t, []byte{0xbb}, results[1].EncryptedValueKey)
}

func TestDecodeBackendFindResponseJSONWithCharset(t *testing.T) {
	decoded, err := cid.Decode(testFindCID)
	require.NoError(t, err)
	pr := testProviderResult(t)

	body, err := model.MarshalFindResponse(&model.FindResponse{
		MultihashResults: []model.MultihashResult{
			{
				Multihash:       decoded.Hash(),
				ProviderResults: []model.ProviderResult{pr},
			},
		},
	})
	require.NoError(t, err)

	got, err := decodeBackendFindResponse(body, "application/json; charset=utf-8", "/cid/"+testFindCID)
	require.NoError(t, err)
	require.Len(t, got.MultihashResults, 1)
	require.Equal(t, pr.Provider.ID, got.MultihashResults[0].ProviderResults[0].Provider.ID)
}

func TestDecodeBackendFindResponseNDJSON(t *testing.T) {
	decoded, err := cid.Decode(testFindCID)
	require.NoError(t, err)
	pr := testProviderResult(t)

	line, err := json.Marshal(encryptedOrPlainResult{ProviderResult: pr})
	require.NoError(t, err)

	got, err := decodeBackendFindResponse(append(line, '\n'), mediaTypeNDJson, "/cid/"+testFindCID)
	require.NoError(t, err)
	require.Equal(t, decoded.Hash(), got.MultihashResults[0].Multihash)
	require.Equal(t, pr.Provider.ID, got.MultihashResults[0].ProviderResults[0].Provider.ID)
}

func TestDecodeBackendFindResponseUnsupportedContentType(t *testing.T) {
	_, err := decodeBackendFindResponse([]byte(`{}`), "text/plain", "/cid/"+testFindCID)
	require.ErrorIs(t, err, errUnsupportedContentType)
}

func TestMultihashFromFindPath(t *testing.T) {
	decoded, err := cid.Decode(testFindCID)
	require.NoError(t, err)

	got, err := multihashFromFindPath("/cid/" + testFindCID)
	require.NoError(t, err)
	require.Equal(t, decoded.Hash(), got)

	got, err = multihashFromFindPath("/encrypted/cid/" + testFindCID)
	require.NoError(t, err)
	require.Equal(t, decoded.Hash(), got)
}

func TestUsableResult(t *testing.T) {
	require.False(t, usableResult(&encryptedOrPlainResult{}))
	require.True(t, usableResult(&encryptedOrPlainResult{EncryptedValueKey: []byte{1}}))

	pr := testProviderResult(t)
	require.True(t, usableResult(&encryptedOrPlainResult{ProviderResult: pr}))

	pr.Provider.Addrs = nil
	require.False(t, usableResult(&encryptedOrPlainResult{ProviderResult: pr}))
}
