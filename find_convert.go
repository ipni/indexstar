package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

var errUnsupportedContentType = errors.New("unsupported backend content type")

func usableResult(result *encryptedOrPlainResult) bool {
	if len(result.EncryptedValueKey) > 0 {
		return true
	}
	return result.Provider != nil && result.Provider.ID != "" && len(result.Provider.Addrs) > 0
}

func resultsFromFindResponse(resp *model.FindResponse, expected multihash.Multihash) (results []*encryptedOrPlainResult, skippedUnexpected int) {
	if resp == nil {
		return nil, 0
	}
	for _, mhr := range resp.MultihashResults {
		if !bytes.Equal(mhr.Multihash, expected) {
			skippedUnexpected += len(mhr.ProviderResults)
			log.Debugw(
				"skipping find results for unexpected multihash",
				"expected", expected,
				"got", mhr.Multihash,
				"count", len(mhr.ProviderResults),
			)
			continue
		}
		for i := range mhr.ProviderResults {
			pr := mhr.ProviderResults[i]
			results = append(results, &encryptedOrPlainResult{ProviderResult: pr})
		}
	}
	for _, emr := range resp.EncryptedMultihashResults {
		if !bytes.Equal(emr.Multihash, expected) {
			skippedUnexpected += len(emr.EncryptedValueKeys)
			log.Debugw(
				"skipping encrypted find results for unexpected multihash",
				"expected", expected,
				"got", emr.Multihash,
				"count", len(emr.EncryptedValueKeys),
			)
			continue
		}
		for _, key := range emr.EncryptedValueKeys {
			results = append(results, &encryptedOrPlainResult{EncryptedValueKey: key})
		}
	}
	if skippedUnexpected > 0 {
		log.Warnw(
			"skipped find results for unexpected multihash",
			"count", skippedUnexpected,
		)
	}
	return results, skippedUnexpected
}

func findResponseFromNDJSON(data []byte, mh multihash.Multihash) (*model.FindResponse, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var provResults []model.ProviderResult
	var encValKeys [][]byte
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var result encryptedOrPlainResult
		if err := json.Unmarshal(line, &result); err != nil {
			return nil, err
		}
		if !usableResult(&result) {
			continue
		}
		if len(result.EncryptedValueKey) > 0 {
			encValKeys = append(encValKeys, result.EncryptedValueKey)
			continue
		}
		provResults = append(provResults, result.ProviderResult)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	resp := &model.FindResponse{}
	if len(provResults) > 0 {
		resp.MultihashResults = []model.MultihashResult{
			{
				Multihash:       mh,
				ProviderResults: provResults,
			},
		}
	}
	if len(encValKeys) > 0 {
		resp.EncryptedMultihashResults = []model.EncryptedMultihashResult{
			{
				Multihash:          mh,
				EncryptedValueKeys: encValKeys,
			},
		}
	}
	return resp, nil
}

func decodeBackendFindResponse(data []byte, contentType string, reqPath string) (*model.FindResponse, error) {
	switch mediaType := mediaTypeFromContentType(contentType, mediaTypeJson); mediaType {
	case mediaTypeJson:
		return model.UnmarshalFindResponse(data)
	case mediaTypeNDJson:
		mh, err := multihashFromFindPath(reqPath)
		if err != nil {
			return nil, err
		}
		return findResponseFromNDJSON(data, mh)
	default:
		return nil, fmt.Errorf("%w: %s", errUnsupportedContentType, mediaType)
	}
}

func multihashFromFindPath(p string) (multihash.Multihash, error) {
	resource := path.Base(p)
	switch path.Base(path.Dir(p)) {
	case "cid":
		c, err := cid.Decode(resource)
		if err != nil {
			return nil, err
		}
		return c.Hash(), nil
	case "multihash":
		mh, err := multihash.FromB58String(resource)
		if err != nil {
			return multihash.FromHexString(resource)
		}
		return mh, nil
	default:
		return nil, fmt.Errorf("unsupported find path %s", p)
	}
}
