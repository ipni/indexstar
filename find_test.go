package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractShardingKey(t *testing.T) {
	require.Equal(t, "bafybeidbjeqjovk2zdwh2dngy7tckid7l7qab5wivw2v5es4gphqxvsqqu", extractShardingKey("https://cid.contact/cid/bafybeidbjeqjovk2zdwh2dngy7tckid7l7qab5wivw2v5es4gphqxvsqqu"))
	require.Equal(t, "QmZ7nrfFMcrnroRWkZCAiALDEYK5Z5gkEFsSMAaoFfQmAw", extractShardingKey("https://cid.contact/multihash/QmZ7nrfFMcrnroRWkZCAiALDEYK5Z5gkEFsSMAaoFfQmAw"))
	require.Equal(t, "ABCDEFQmZ7nrfFM", extractShardingKey("https://cid.contact/metadata/ABCDEFQmZ7nrfFM"))
}
