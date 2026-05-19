package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	cli "github.com/urfave/cli/v2"
)

type mockServer struct {
	ctx context.Context

	cfgBase                  string
	errorsToEmitBeforeCancel []error
	errorsToEmitAfterCancel  []error

	reloadCallCnt atomic.Int64
}

func (s *mockServer) Serve() <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		for _, err := range s.errorsToEmitBeforeCancel {
			errChan <- err
		}

		<-s.ctx.Done()

		for _, err := range s.errorsToEmitAfterCancel {
			errChan <- err
		}

	}()

	return errChan
}

func (s *mockServer) Reload(c *cli.Context) error {
	s.reloadCallCnt.Add(1)
	return nil
}

func (s *mockServer) GetCfgBase() string {
	return s.cfgBase
}

type runAppTestSuite struct {
	suite.Suite
	errCnt int
}

func (s *runAppTestSuite) genError() error {
	s.errCnt++
	return fmt.Errorf("test error %d at %s", s.errCnt, time.Now())
}

func TestRunAppTestSuite(t *testing.T) {
	suite.Run(t, &runAppTestSuite{})
}

func (s *runAppTestSuite) TestFailureInNewServer() {
	synctest.Test(s.T(), func(t *testing.T) {
		errToReturn := s.genError()

		err := runApp(
			context.Background(),
			[]string{},
			nil,
			configCheckInterval,
			func(c *cli.Context) (serverInterface, error) {
				return nil, errToReturn
			},
		)

		require.ErrorIs(t, err, errToReturn)
	})
}

func (s *runAppTestSuite) TestSuccess() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		start := time.Now()
		runAppFinished := false

		go func() {
			err := runApp(
				ctx,
				[]string{},
				nil,
				time.Minute, // More than the duration of this test
				func(c *cli.Context) (serverInterface, error) {
					return &mockServer{ctx: c.Context}, nil
				},
			)
			require.NoError(t, err)
			require.Equal(t, time.Second, time.Since(start))
			runAppFinished = true
		}()

		// Advance teh execution to a state where the server has started
		time.Sleep(time.Second)
		synctest.Wait()
		require.False(t, runAppFinished)

		// Simulate terminating signal
		cancel()
		synctest.Wait()
		require.True(t, runAppFinished)
	})
}

func (s *runAppTestSuite) TestErrorBeforeCancel() {
	synctest.Test(s.T(), func(t *testing.T) {
		errToEmit1 := s.genError()
		errToEmit2 := s.genError()

		err := runApp(
			t.Context(),
			[]string{},
			nil,
			0,
			func(c *cli.Context) (serverInterface, error) {
				return &mockServer{
					errorsToEmitBeforeCancel: []error{
						errToEmit1, errToEmit2,
					},
					ctx: c.Context,
				}, nil
			},
		)

		require.ErrorIs(t, err, errToEmit1)
		require.ErrorIs(t, err, errToEmit2)
	})
}

func (s *runAppTestSuite) TestErrorAfterCancel() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		errToEmit1 := s.genError()
		errToEmit2 := s.genError()

		runAppFinished := false

		wg := sync.WaitGroup{}

		wg.Go(func() {
			err := runApp(
				ctx,
				[]string{},
				nil,
				0,
				func(c *cli.Context) (serverInterface, error) {
					return &mockServer{
						errorsToEmitAfterCancel: []error{
							errToEmit1, errToEmit2,
						},
						ctx: c.Context,
					}, nil
				},
			)
			require.ErrorIs(t, err, errToEmit1)
			require.ErrorIs(t, err, errToEmit2)

			runAppFinished = true
		})

		synctest.Wait()
		require.False(t, runAppFinished)

		cancel()

		synctest.Wait()
		require.True(t, runAppFinished)

		wg.Wait()
	})
}

func (s *runAppTestSuite) TestErrorBeforeAndAfterCancel() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		errToEmitBefore1 := s.genError()
		errToEmitBefore2 := s.genError()
		errToEmitAfter1 := s.genError()
		errToEmitAfter2 := s.genError()

		runAppFinished := false

		wg := sync.WaitGroup{}

		wg.Go(func() {
			err := runApp(
				ctx,
				[]string{},
				nil,
				0,
				func(c *cli.Context) (serverInterface, error) {
					return &mockServer{
						errorsToEmitBeforeCancel: []error{
							errToEmitBefore1, errToEmitBefore2,
						},
						errorsToEmitAfterCancel: []error{
							errToEmitAfter1, errToEmitAfter2,
						},
						ctx: c.Context,
					}, nil
				},
			)
			require.ErrorIs(t, err, errToEmitBefore1)
			require.ErrorIs(t, err, errToEmitBefore2)
			require.ErrorIs(t, err, errToEmitAfter1)
			require.ErrorIs(t, err, errToEmitAfter2)

			runAppFinished = true
		})

		synctest.Wait()

		cancel()

		synctest.Wait()
		require.True(t, runAppFinished)

		wg.Wait()
	})
}

func (s *runAppTestSuite) TestReloadConfigFromSignal() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		cfgPath := filepath.Join(t.TempDir(), "config.txt")

		reloadSignal := make(chan os.Signal, 1)

		var srv *mockServer

		wg := sync.WaitGroup{}
		wg.Go(func() {
			err := runApp(
				ctx,
				[]string{},
				reloadSignal,
				0,
				func(c *cli.Context) (serverInterface, error) {
					srv = &mockServer{
						ctx:     c.Context,
						cfgBase: cfgPath,
					}
					return srv, nil
				},
			)
			require.NoError(t, err)
		})

		synctest.Wait()
		require.Zero(t, srv.reloadCallCnt.Load())

		reloadSignal <- syscall.SIGHUP
		synctest.Wait()
		require.EqualValues(t, 1, srv.reloadCallCnt.Load())

		cancel()
		wg.Wait()
	})
}

func (s *runAppTestSuite) TestReloadConfigFromFileChange() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		wg := sync.WaitGroup{}

		cfgPath := filepath.Join(t.TempDir(), "config.txt")
		require.NoError(t, os.WriteFile(cfgPath, []byte("test1"), 0600))

		reloadSignal := make(chan os.Signal, 1)

		var srv *mockServer

		wg.Go(func() {
			err := runApp(
				ctx,
				[]string{},
				reloadSignal,
				configCheckInterval,
				func(c *cli.Context) (serverInterface, error) {
					srv = &mockServer{
						ctx:     c.Context,
						cfgBase: cfgPath,
					}
					return srv, nil
				},
			)
			require.NoError(t, err)
		})

		// must not reload config initially
		synctest.Wait()
		require.Zero(t, srv.reloadCallCnt.Load())

		// must not reload config if config file has not changed
		time.Sleep(2 * configCheckInterval)
		synctest.Wait()
		require.Zero(t, srv.reloadCallCnt.Load())

		// must reload config if config file has changed
		require.NoError(t, os.WriteFile(cfgPath, []byte("updated file"), 0600))

		time.Sleep(configCheckInterval)
		synctest.Wait()
		require.EqualValues(t, 1, srv.reloadCallCnt.Load())

		// no changes afterwards, must not reload
		time.Sleep(2 * configCheckInterval)
		synctest.Wait()
		require.EqualValues(t, 1, srv.reloadCallCnt.Load())

		// stop reloading if the file is removed
		plr := logging.NewPipeReader()
		defer plr.Close()

		require.NoError(t, os.Remove(cfgPath))
		time.Sleep(2 * configCheckInterval)
		synctest.Wait()
		require.EqualValues(t, 1, srv.reloadCallCnt.Load())

		buf := make([]byte, 1000)
		n, err := plr.Read(buf)
		require.NoError(t, err)
		buf = buf[:n]

		require.Contains(t, string(buf), "Cannot stat config file")

		cancel()
		wg.Wait()
	})
}
