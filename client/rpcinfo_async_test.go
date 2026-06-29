/*
 * Copyright 2026 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package client

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/cloudwego/kitex/internal/mocks"
	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/retry"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

type asyncRPCInfoContexts struct {
	mu   sync.Mutex
	ctxs []context.Context
}

func (c *asyncRPCInfoContexts) append(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ctxs = append(c.ctxs, ctx)
}

func (c *asyncRPCInfoContexts) snapshot() []context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctxs := make([]context.Context, len(c.ctxs))
	copy(ctxs, c.ctxs)
	return ctxs
}

func mustReadAllRPCInfoAsync(t *testing.T, captured *asyncRPCInfoContexts, want int) {
	t.Helper()
	ctxs := captured.snapshot()
	test.Assert(t, len(ctxs) == want, len(ctxs))
	for _, ctx := range ctxs {
		mustReadRPCInfoAsync(t, ctx)
	}
}

func mustReadRPCInfoAsync(t *testing.T, ctx context.Context) {
	t.Helper()
	if panicInfo := readRPCInfoAsyncPanic(ctx); panicInfo != nil {
		t.Fatalf("async RPCInfo read panicked: %v", panicInfo)
	}
}

func mustReadRPCInfoAsyncPanic(t *testing.T, ctx context.Context) {
	t.Helper()
	if panicInfo := readRPCInfoAsyncPanic(ctx); panicInfo == nil {
		t.Fatalf("expected async RPCInfo read panic")
	}
}

func readRPCInfoAsyncPanic(ctx context.Context) interface{} {
	done := make(chan interface{}, 1)
	go func() {
		defer func() {
			done <- recover()
		}()
		readRPCInfoForAsyncTest(ctx)
	}()
	return <-done
}

func readRPCInfoForAsyncTest(ctx context.Context) {
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil {
		panic("nil RPCInfo")
	}
	from := ri.From()
	if from == nil {
		panic("nil From endpoint")
	}
	_ = from.ServiceName()
	_ = from.Method()
	_ = from.Address()
	_, _ = from.Tag("cluster")
	_ = from.DefaultTag("cluster", "")

	to := ri.To()
	if to == nil {
		panic("nil To endpoint")
	}
	_ = to.ServiceName()
	_ = to.Method()
	_ = to.Address()
	_, _ = to.Tag("cluster")
	_ = to.DefaultTag("cluster", "")

	inv := ri.Invocation()
	if inv == nil {
		panic("nil Invocation")
	}
	_ = inv.ServiceName()
	_ = inv.MethodName()
	_ = inv.PackageName()
	_ = inv.SeqID()
	_ = inv.StreamingMode()

	cfg := ri.Config()
	if cfg == nil {
		panic("nil RPCConfig")
	}
	_ = cfg.RPCTimeout()
	_ = cfg.ConnectTimeout()
	_ = cfg.ReadWriteTimeout()

	st := ri.Stats()
	if st == nil {
		panic("nil RPCStats")
	}
	_ = st.Level()
	_ = st.Error()
	_, _ = st.Panicked()
	_ = st.SendSize()
	_ = st.RecvSize()
}

func readRPCInfoUntilStopped(ctx context.Context, stop <-chan struct{}, started chan<- struct{}, done chan<- interface{}) {
	close(started)
	defer func() {
		done <- recover()
	}()
	for {
		select {
		case <-stop:
			return
		default:
			readRPCInfoForAsyncTest(ctx)
			runtime.Gosched()
		}
	}
}

func TestCallRPCInfoAsyncReadAfterReturn(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var captured context.Context
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			captured = ctx
			return next(ctx, req, resp)
		}
	}
	cli := newMockClient(t, ctrl, WithMiddleware(md))

	err := cli.Call(context.Background(), mocks.MockMethod, mocks.NewMockArgs(), mocks.NewMockResult())
	test.Assert(t, err == nil, err)
	test.Assert(t, captured != nil)
	mustReadRPCInfoAsync(t, captured)
}

func TestCallRPCInfoNoRaceWithAsyncReadDuringFinish(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stop := make(chan struct{})
	started := make(chan struct{})
	done := make(chan interface{}, 1)
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			go readRPCInfoUntilStopped(ctx, stop, started, done)
			<-started
			return next(ctx, req, resp)
		}
	}
	cli := newMockClient(t, ctrl, WithMiddleware(md))

	err := cli.Call(context.Background(), mocks.MockMethod, mocks.NewMockArgs(), mocks.NewMockResult())
	test.Assert(t, err == nil, err)
	close(stop)
	if panicInfo := <-done; panicInfo != nil {
		t.Fatalf("async RPCInfo read panicked: %v", panicInfo)
	}
}

func TestCallRPCInfoAsyncReadAfterReturnWithPoolPanics(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(true)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var captured context.Context
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			captured = ctx
			return next(ctx, req, resp)
		}
	}
	cli := newMockClient(t, ctrl, WithMiddleware(md))

	err := cli.Call(context.Background(), mocks.MockMethod, mocks.NewMockArgs(), mocks.NewMockResult())
	test.Assert(t, err == nil, err)
	test.Assert(t, captured != nil)
	mustReadRPCInfoAsyncPanic(t, captured)
}

func TestCallFailureRetryRPCInfoAsyncReadAfterReturn(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var callTimes int32
	var captured asyncRPCInfoContexts
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			captured.append(ctx)
			if atomic.AddInt32(&callTimes, 1) == 1 {
				return kerrors.ErrRPCTimeout
			}
			return next(ctx, req, resp)
		}
	}
	cli := newMockClient(t, ctrl, WithMiddleware(md), WithFailureRetry(retry.NewFailurePolicy()))

	err := cli.Call(context.Background(), mocks.MockMethod, mocks.NewMockArgs(), mocks.NewMockResult())
	test.Assert(t, err == nil, err)
	test.Assert(t, atomic.LoadInt32(&callTimes) == 2, callTimes)
	mustReadAllRPCInfoAsync(t, &captured, 2)
}

func TestCallFailureRetryRPCInfoNoRaceWithAsyncReadDuringFinish(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var callTimes int32
	var readersMu sync.Mutex
	var stops []chan struct{}
	var dones []chan interface{}
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			stop := make(chan struct{})
			started := make(chan struct{})
			done := make(chan interface{}, 1)
			readersMu.Lock()
			stops = append(stops, stop)
			dones = append(dones, done)
			readersMu.Unlock()

			go readRPCInfoUntilStopped(ctx, stop, started, done)
			<-started

			if atomic.AddInt32(&callTimes, 1) == 1 {
				return kerrors.ErrRPCTimeout
			}
			return next(ctx, req, resp)
		}
	}
	cli := newMockClient(t, ctrl, WithMiddleware(md), WithFailureRetry(retry.NewFailurePolicy()))

	err := cli.Call(context.Background(), mocks.MockMethod, mocks.NewMockArgs(), mocks.NewMockResult())
	test.Assert(t, err == nil, err)
	test.Assert(t, atomic.LoadInt32(&callTimes) == 2, callTimes)

	readersMu.Lock()
	localStops := append([]chan struct{}(nil), stops...)
	localDones := append([]chan interface{}(nil), dones...)
	readersMu.Unlock()
	test.Assert(t, len(localStops) == 2, len(localStops))
	for _, stop := range localStops {
		close(stop)
	}
	for _, done := range localDones {
		if panicInfo := <-done; panicInfo != nil {
			t.Fatalf("async RPCInfo read panicked: %v", panicInfo)
		}
	}
}

func TestCallBackupRetryRPCInfoAsyncReadAfterReturn(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var callTimes int32
	var wg sync.WaitGroup
	var captured asyncRPCInfoContexts
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			wg.Add(1)
			defer wg.Done()

			captured.append(ctx)
			if atomic.AddInt32(&callTimes, 1) == 1 {
				time.Sleep(80 * time.Millisecond)
			}
			return next(ctx, req, resp)
		}
	}
	cli := newMockClient(t, ctrl, WithMiddleware(md), WithBackupRequest(retry.NewBackupPolicy(10)))

	err := cli.Call(context.Background(), mocks.MockMethod, mocks.NewMockArgs(), mocks.NewMockResult())
	test.Assert(t, err == nil, err)
	wg.Wait()
	test.Assert(t, atomic.LoadInt32(&callTimes) == 2, callTimes)
	mustReadAllRPCInfoAsync(t, &captured, 2)
}

func TestCallMixedRetryRPCInfoAsyncReadAfterReturn(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var callTimes int32
	var wg sync.WaitGroup
	var captured asyncRPCInfoContexts
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			wg.Add(1)
			defer wg.Done()

			captured.append(ctx)
			if atomic.AddInt32(&callTimes, 1) == 1 {
				time.Sleep(80 * time.Millisecond)
			}
			return next(ctx, req, resp)
		}
	}
	cli := newMockClient(t, ctrl, WithMiddleware(md), WithMixedRetry(retry.NewMixedPolicy(10)))

	err := cli.Call(context.Background(), mocks.MockMethod, mocks.NewMockArgs(), mocks.NewMockResult())
	test.Assert(t, err == nil, err)
	wg.Wait()
	test.Assert(t, atomic.LoadInt32(&callTimes) == 2, callTimes)
	mustReadAllRPCInfoAsync(t, &captured, 2)
}

func TestServiceInlineCallRPCInfoAsyncReadAfterReturn(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var captured context.Context
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			captured = ctx
			return next(ctx, req, resp)
		}
	}
	cli := newMockServiceInlineClient(t, ctrl, WithMiddleware(md))

	err := cli.Call(context.Background(), mocks.MockMethod, new(MockTStruct), new(MockTStruct))
	test.Assert(t, err == nil, err)
	test.Assert(t, captured != nil)
	mustReadRPCInfoAsync(t, captured)
}

func TestServiceInlineCallRPCInfoNoRaceWithAsyncReadDuringFinish(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stop := make(chan struct{})
	started := make(chan struct{})
	done := make(chan interface{}, 1)
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			go readRPCInfoUntilStopped(ctx, stop, started, done)
			<-started
			return next(ctx, req, resp)
		}
	}
	cli := newMockServiceInlineClient(t, ctrl, WithMiddleware(md))

	err := cli.Call(context.Background(), mocks.MockMethod, new(MockTStruct), new(MockTStruct))
	test.Assert(t, err == nil, err)
	close(stop)
	if panicInfo := <-done; panicInfo != nil {
		t.Fatalf("async RPCInfo read panicked: %v", panicInfo)
	}
}

func TestServiceInlineCallRPCInfoAsyncReadAfterReturnWithPoolPanics(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(true)
	defer rpcinfo.EnablePool(originState)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var captured context.Context
	md := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			captured = ctx
			return next(ctx, req, resp)
		}
	}
	cli := newMockServiceInlineClient(t, ctrl, WithMiddleware(md))

	err := cli.Call(context.Background(), mocks.MockMethod, new(MockTStruct), new(MockTStruct))
	test.Assert(t, err == nil, err)
	test.Assert(t, captured != nil)
	mustReadRPCInfoAsyncPanic(t, captured)
}
