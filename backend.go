package main

import (
	"net/http"
	"net/url"

	"github.com/mercari/go-circuitbreaker"
)

var Matchers struct {
	Any        HttpRequestMatcher
	AnyOf      func(...HttpRequestMatcher) HttpRequestMatcher
	QueryParam func(key, value string) HttpRequestMatcher
}

type (
	HttpRequestMatcher func(r *http.Request) bool
	Backend            interface {
		URL() *url.URL
		CB() *circuitbreaker.CircuitBreaker
		Matches(r *http.Request) bool
	}
	SimpleBackend struct {
		url     *url.URL
		cb      *circuitbreaker.CircuitBreaker
		matcher HttpRequestMatcher
	}
)

func (b *SimpleBackend) URL() *url.URL {
	return b.url
}

func (b *SimpleBackend) CB() *circuitbreaker.CircuitBreaker {
	return b.cb
}

func init() {
	Matchers.Any = func(*http.Request) bool { return true }
	Matchers.AnyOf = func(ms ...HttpRequestMatcher) HttpRequestMatcher {
		return func(r *http.Request) bool {
			for _, m := range ms {
				if m(r) {
					return true
				}
			}
			return false
		}
	}
	Matchers.QueryParam = func(key, value string) HttpRequestMatcher {
		return func(r *http.Request) bool {
			if r == nil {
				return false
			}
			values, ok := r.URL.Query()[key]
			if !ok {
				return false
			}
			for _, got := range values {
				if value == got {
					return true
				}
			}
			return false
		}
	}
}

func NewBackend(u string, cb *circuitbreaker.CircuitBreaker, matcher HttpRequestMatcher) (Backend, error) {
	burl, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	return &SimpleBackend{
		url:     burl,
		cb:      cb,
		matcher: matcher,
	}, nil
}

func (b *SimpleBackend) Matches(r *http.Request) bool {
	return b.matcher(r)
}
