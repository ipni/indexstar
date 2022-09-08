package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestScatterGather_GathersExpectedResults(t *testing.T) {
	subject := scatterGather[int, string]{
		targets: []int{1, 2, 3, 4, 5},
		maxWait: 2 * time.Second,
	}

	ctx := context.Background()
	err := subject.scatter(ctx, func(i int) (<-chan string, error) {
		r := make(chan string, 1)
		r <- fmt.Sprintf("%d fish", i)
		return r, nil
	})
	require.NoError(t, err)

	var gotResults []string
	for got := range subject.gather(ctx) {
		gotResults = append(gotResults, got)
	}
	require.Len(t, gotResults, 5)
	require.Contains(t, gotResults, "1 fish")
	require.Contains(t, gotResults, "2 fish")
	require.Contains(t, gotResults, "3 fish")
	require.Contains(t, gotResults, "4 fish")
	require.Contains(t, gotResults, "5 fish")
}

func TestScatterGather_ExcludesScatterErrors(t *testing.T) {
	subject := scatterGather[int, string]{
		targets: []int{1, 2, 3},
		maxWait: 2 * time.Second,
	}
	ctx := context.Background()
	err := subject.scatter(ctx, func(i int) (<-chan string, error) {
		if i == 2 {
			return nil, errors.New("fish says no")
		}
		r := make(chan string, 1)
		r <- fmt.Sprintf("%d fish", i)
		return r, nil
	})
	require.NoError(t, err)

	var gotResults []string
	for got := range subject.gather(ctx) {
		gotResults = append(gotResults, got)
	}
	require.Len(t, gotResults, 2)
	require.Contains(t, gotResults, "1 fish")
	require.Contains(t, gotResults, "3 fish")
	require.NotContains(t, gotResults, "2 fish")
}

func TestScatterGather_DoesNotWaitLongerThanExpected(t *testing.T) {
	subject := scatterGather[int, string]{
		targets: []int{1},
		maxWait: 100 * time.Millisecond,
	}
	ctx := context.Background()
	err := subject.scatter(ctx, func(i int) (<-chan string, error) {
		time.Sleep(2 * time.Second)
		r := make(chan string, 1)
		r <- fmt.Sprintf("%d fish", i)
		return r, nil
	})
	require.NoError(t, err)

	var gotResults []string
	for got := range subject.gather(ctx) {
		gotResults = append(gotResults, got)
	}
	require.Len(t, gotResults, 0)
}

func TestScatterGather_GathersNothingWhenContextIsCancelled(t *testing.T) {
	subject := scatterGather[int, string]{
		targets: []int{1, 2, 3},
		maxWait: 2 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	cancel()

	err := subject.scatter(ctx, func(i int) (<-chan string, error) {
		r := make(chan string, 1)
		r <- fmt.Sprintf("%d fish", i)
		return r, nil
	})
	require.NoError(t, err)

	var gotResults []string
	for got := range subject.gather(ctx) {
		gotResults = append(gotResults, got)
	}
	require.Len(t, gotResults, 0)
}
