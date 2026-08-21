package main

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_getAccepts(t *testing.T) {

	tests := []struct {
		name    string
		given   string
		want    accepts
		wantErr bool
	}{
		{
			name:  "browser",
			given: "ext/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
			want: accepts{
				any:               true,
				acceptHeaderFound: true,
			},
		},
		{
			name:  "extra space",
			given: "ext/html,application/xhtml+xml   ,   application/xml;q=0.9",
			want: accepts{
				acceptHeaderFound: true,
			},
		},
		{
			name: "none",
		},
		{
			name:  "invalid",
			given: `;;;;`,
			want: accepts{
				acceptHeaderFound: true,
			},
			wantErr: true,
		},
		{
			name:  "extra space",
			given: "ext/html,application/xhtml+xml   ,   application/xml;q=0.9",
			want: accepts{
				acceptHeaderFound: true,
			},
		},
		{
			name:  "json",
			given: "application/json",
			want: accepts{
				json:              true,
				acceptHeaderFound: true,
			},
		},
		{
			name:  "ndjson",
			given: "application/x-ndjson",
			want: accepts{
				ndjson:            true,
				acceptHeaderFound: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "fish.invalid", nil)
			require.NoError(t, err)
			if tt.given != "" {
				r.Header.Set("Accept", tt.given)
			}
			got, err := getAccepts(r)
			if (err != nil) != tt.wantErr {
				t.Errorf("getAccepts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getAccepts() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mediaTypeFromContentType(t *testing.T) {
	const fallback = "application/fallback"
	tests := []struct {
		name  string
		given string
		want  string
	}{
		{name: "empty", want: fallback},
		{name: "json", given: "application/json", want: mediaTypeJson},
		{name: "json charset", given: "application/json; charset=utf-8", want: mediaTypeJson},
		{name: "json charset no space", given: "application/json;charset=utf-8", want: mediaTypeJson},
		{name: "ndjson", given: "application/x-ndjson", want: mediaTypeNDJson},
		{name: "invalid", given: ";;;;", want: fallback},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, mediaTypeFromContentType(tt.given, fallback))
		})
	}
}
