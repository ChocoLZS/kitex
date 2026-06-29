/*
 * Copyright 2021 CloudWeGo Authors
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

package trans

import (
	"context"
	"errors"
	"net"
	"runtime"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/cloudwego/kitex/internal/mocks"
	mockmessage "github.com/cloudwego/kitex/internal/mocks/message"
	remotemocks "github.com/cloudwego/kitex/internal/mocks/remote"
	"github.com/cloudwego/kitex/internal/mocks/stats"
	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

var (
	svcInfo     = mocks.ServiceInfo()
	svcSearcher = remotemocks.NewDefaultSvcSearcher()
)

func mustReadRPCInfoAsync(t *testing.T, ctx context.Context) {
	t.Helper()
	done := make(chan interface{}, 1)
	go func() {
		defer func() {
			done <- recover()
		}()
		readRPCInfoForAsyncTest(ctx)
	}()
	if panicInfo := <-done; panicInfo != nil {
		t.Fatalf("async RPCInfo read panicked: %v", panicInfo)
	}
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

func initOrResetReadableRPCInfo(ri rpcinfo.RPCInfo, addr net.Addr) rpcinfo.RPCInfo {
	if ri != nil {
		return ri
	}
	return rpcinfo.NewRPCInfo(
		rpcinfo.NewEndpointInfo("client", "from_method", addr, map[string]string{"cluster": "test"}),
		rpcinfo.FromBasicInfo(&rpcinfo.EndpointBasicInfo{
			ServiceName: mocks.MockServiceName,
			Method:      mocks.MockMethod,
			Tags:        map[string]string{"cluster": "test"},
		}),
		rpcinfo.NewInvocation(mocks.MockServiceName, mocks.MockMethod),
		rpcinfo.NewRPCConfig(),
		rpcinfo.NewRPCStats(),
	)
}

func initOrResetMockRPCInfo(ri rpcinfo.RPCInfo, addr net.Addr) rpcinfo.RPCInfo {
	if ri == nil {
		ri = newMockRPCInfo()
	}
	rpcinfo.AsMutableEndpointInfo(ri.From()).SetAddress(addr)
	return ri
}

func TestDefaultSvrTransHandler(t *testing.T) {
	buf := remote.NewReaderWriterBuffer(1024)
	ext := &MockExtension{
		NewWriteByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
		NewReadByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
	}

	tagEncode, tagDecode := 0, 0
	opt := &remote.ServerOption{
		Codec: &MockCodec{
			EncodeFunc: func(ctx context.Context, msg remote.Message, out remote.ByteBuffer) error {
				tagEncode++
				test.Assert(t, out == buf)
				return nil
			},
			DecodeFunc: func(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
				tagDecode++
				test.Assert(t, in == buf)
				return nil
			},
		},
		SvcSearcher: svcSearcher,
	}

	handler, err := NewDefaultSvrTransHandler(opt, ext)
	test.Assert(t, err == nil)

	ctx := context.Background()
	conn := &mocks.Conn{}
	msg := &mockmessage.MockMessage{
		RPCInfoFunc: func() rpcinfo.RPCInfo {
			return newMockRPCInfo()
		},
	}
	ctx, err = handler.Write(ctx, conn, msg)
	test.Assert(t, ctx != nil, ctx)
	test.Assert(t, err == nil, err)
	test.Assert(t, tagEncode == 1, tagEncode)
	test.Assert(t, tagDecode == 0, tagDecode)

	ctx, err = handler.Read(ctx, conn, msg)
	test.Assert(t, ctx != nil, ctx)
	test.Assert(t, err == nil, err)
	test.Assert(t, tagEncode == 1, tagEncode)
	test.Assert(t, tagDecode == 1, tagDecode)
}

func TestSvrTransHandlerBizError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracer := stats.NewMockTracer(ctrl)
	mockTracer.EXPECT().Start(gomock.Any()).DoAndReturn(func(ctx context.Context) context.Context { return ctx }).AnyTimes()
	mockTracer.EXPECT().Finish(gomock.Any()).DoAndReturn(func(ctx context.Context) {
		err := rpcinfo.GetRPCInfo(ctx).Stats().Error()
		test.Assert(t, err != nil)
	}).AnyTimes()

	buf := remote.NewReaderWriterBuffer(1024)
	ext := &MockExtension{
		NewWriteByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
		NewReadByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
	}

	tracerCtl := &rpcinfo.TraceController{}
	tracerCtl.Append(mockTracer)
	opt := &remote.ServerOption{
		Codec: &MockCodec{
			EncodeFunc: func(ctx context.Context, msg remote.Message, out remote.ByteBuffer) error {
				return nil
			},
			DecodeFunc: func(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
				mink := msg.RPCInfo().Invocation().(rpcinfo.InvocationSetter)
				mink.SetServiceName(mocks.MockServiceName)
				mink.SetMethodName(mocks.MockMethod)
				mink.SetMethodInfo(svcInfo.MethodInfo(context.Background(), mocks.MockMethod))
				return nil
			},
		},
		SvcSearcher:            svcSearcher,
		TracerCtl:              tracerCtl,
		InitOrResetRPCInfoFunc: initOrResetMockRPCInfo,
	}
	ri := rpcinfo.NewRPCInfo(rpcinfo.EmptyEndpointInfo(), rpcinfo.FromBasicInfo(&rpcinfo.EndpointBasicInfo{}),
		rpcinfo.NewInvocation("", mocks.MockMethod), nil, rpcinfo.NewRPCStats())
	ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)

	svrHandler, err := NewDefaultSvrTransHandler(opt, ext)
	pl := remote.NewTransPipeline(svrHandler)
	svrHandler.SetPipeline(pl)
	if setter, ok := svrHandler.(remote.InvokeHandleFuncSetter); ok {
		setter.SetInvokeHandleFunc(func(ctx context.Context, req, resp interface{}) (err error) {
			return kerrors.ErrBiz.WithCause(errors.New("mock"))
		})
	}
	test.Assert(t, err == nil)
	err = svrHandler.OnRead(ctx, &mocks.Conn{})
	test.Assert(t, err == nil)
}

func TestSvrTransHandlerRPCInfoAsyncReadAfterOnRead(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	buf := remote.NewReaderWriterBuffer(1024)
	ext := &MockExtension{
		NewWriteByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
		NewReadByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
	}
	opt := &remote.ServerOption{
		Codec: &MockCodec{
			EncodeFunc: func(ctx context.Context, msg remote.Message, out remote.ByteBuffer) error {
				return nil
			},
			DecodeFunc: func(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
				mink := msg.RPCInfo().Invocation().(rpcinfo.InvocationSetter)
				mink.SetServiceName(mocks.MockServiceName)
				mink.SetMethodName(mocks.MockMethod)
				mink.SetMethodInfo(svcInfo.MethodInfo(context.Background(), mocks.MockMethod))
				return nil
			},
		},
		SvcSearcher:            svcSearcher,
		TracerCtl:              &rpcinfo.TraceController{},
		InitOrResetRPCInfoFunc: initOrResetReadableRPCInfo,
	}
	svrHandler, err := NewDefaultSvrTransHandler(opt, ext)
	test.Assert(t, err == nil, err)
	pl := remote.NewTransPipeline(svrHandler)
	svrHandler.SetPipeline(pl)

	var captured context.Context
	svrHandler.(remote.InvokeHandleFuncSetter).SetInvokeHandleFunc(func(ctx context.Context, req, resp interface{}) error {
		captured = ctx
		return nil
	})

	ctx, err := svrHandler.OnActive(context.Background(), &mocks.Conn{})
	test.Assert(t, err == nil, err)
	err = svrHandler.OnRead(ctx, &mocks.Conn{})
	test.Assert(t, err == nil, err)
	test.Assert(t, captured != nil)
	mustReadRPCInfoAsync(t, captured)
}

func TestSvrTransHandlerRPCInfoNoRaceWithAsyncReadDuringFinish(t *testing.T) {
	originState := rpcinfo.PoolEnabled()
	rpcinfo.EnablePool(false)
	defer rpcinfo.EnablePool(originState)

	buf := remote.NewReaderWriterBuffer(1024)
	ext := &MockExtension{
		NewWriteByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
		NewReadByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
	}
	opt := &remote.ServerOption{
		Codec: &MockCodec{
			EncodeFunc: func(ctx context.Context, msg remote.Message, out remote.ByteBuffer) error {
				return nil
			},
			DecodeFunc: func(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
				mink := msg.RPCInfo().Invocation().(rpcinfo.InvocationSetter)
				mink.SetServiceName(mocks.MockServiceName)
				mink.SetMethodName(mocks.MockMethod)
				mink.SetMethodInfo(svcInfo.MethodInfo(context.Background(), mocks.MockMethod))
				return nil
			},
		},
		SvcSearcher:            svcSearcher,
		TracerCtl:              &rpcinfo.TraceController{},
		InitOrResetRPCInfoFunc: initOrResetReadableRPCInfo,
	}
	svrHandler, err := NewDefaultSvrTransHandler(opt, ext)
	test.Assert(t, err == nil, err)
	pl := remote.NewTransPipeline(svrHandler)
	svrHandler.SetPipeline(pl)

	stop := make(chan struct{})
	started := make(chan struct{})
	done := make(chan interface{}, 1)
	svrHandler.(remote.InvokeHandleFuncSetter).SetInvokeHandleFunc(func(ctx context.Context, req, resp interface{}) error {
		go readRPCInfoUntilStopped(ctx, stop, started, done)
		<-started
		return nil
	})

	ctx, err := svrHandler.OnActive(context.Background(), &mocks.Conn{})
	test.Assert(t, err == nil, err)
	err = svrHandler.OnRead(ctx, &mocks.Conn{})
	test.Assert(t, err == nil, err)
	close(stop)
	if panicInfo := <-done; panicInfo != nil {
		t.Fatalf("async RPCInfo read panicked: %v", panicInfo)
	}
}

func TestSvrTransHandlerReadErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracer := stats.NewMockTracer(ctrl)
	mockTracer.EXPECT().Start(gomock.Any()).DoAndReturn(func(ctx context.Context) context.Context { return ctx }).AnyTimes()
	mockTracer.EXPECT().Finish(gomock.Any()).DoAndReturn(func(ctx context.Context) {
		err := rpcinfo.GetRPCInfo(ctx).Stats().Error()
		test.Assert(t, err != nil)
	}).AnyTimes()

	buf := remote.NewReaderWriterBuffer(1024)
	ext := &MockExtension{
		NewWriteByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
		NewReadByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
	}

	mockErr := errors.New("mock")
	tracerCtl := &rpcinfo.TraceController{}
	tracerCtl.Append(mockTracer)
	opt := &remote.ServerOption{
		Codec: &MockCodec{
			EncodeFunc: func(ctx context.Context, msg remote.Message, out remote.ByteBuffer) error {
				return nil
			},
			DecodeFunc: func(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
				return mockErr
			},
		},
		SvcSearcher:            svcSearcher,
		TracerCtl:              tracerCtl,
		InitOrResetRPCInfoFunc: initOrResetMockRPCInfo,
	}
	ri := rpcinfo.NewRPCInfo(rpcinfo.EmptyEndpointInfo(), rpcinfo.FromBasicInfo(&rpcinfo.EndpointBasicInfo{}),
		rpcinfo.NewInvocation("", mocks.MockMethod), nil, rpcinfo.NewRPCStats())
	ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)

	svrHandler, err := NewDefaultSvrTransHandler(opt, ext)
	test.Assert(t, err == nil)
	pl := remote.NewTransPipeline(svrHandler)
	svrHandler.SetPipeline(pl)
	err = svrHandler.OnRead(ctx, &mocks.Conn{})
	test.Assert(t, err != nil)
	test.Assert(t, errors.Is(err, mockErr))
}

func TestSvrTransHandlerReadPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracer := stats.NewMockTracer(ctrl)
	mockTracer.EXPECT().Start(gomock.Any()).DoAndReturn(func(ctx context.Context) context.Context { return ctx }).AnyTimes()
	mockTracer.EXPECT().Finish(gomock.Any()).DoAndReturn(func(ctx context.Context) {
		err := rpcinfo.GetRPCInfo(ctx).Stats().Error()
		test.Assert(t, err != nil)
	}).AnyTimes()

	buf := remote.NewReaderWriterBuffer(1024)
	ext := &MockExtension{
		NewWriteByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
		NewReadByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
	}

	tracerCtl := &rpcinfo.TraceController{}
	tracerCtl.Append(mockTracer)
	opt := &remote.ServerOption{
		Codec: &MockCodec{
			EncodeFunc: func(ctx context.Context, msg remote.Message, out remote.ByteBuffer) error {
				return nil
			},
			DecodeFunc: func(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
				panic("mock")
			},
		},
		SvcSearcher:            svcSearcher,
		TracerCtl:              tracerCtl,
		InitOrResetRPCInfoFunc: initOrResetMockRPCInfo,
	}
	ri := rpcinfo.NewRPCInfo(rpcinfo.EmptyEndpointInfo(), rpcinfo.FromBasicInfo(&rpcinfo.EndpointBasicInfo{}),
		rpcinfo.NewInvocation("", ""), nil, rpcinfo.NewRPCStats())
	ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)

	svrHandler, err := NewDefaultSvrTransHandler(opt, ext)
	test.Assert(t, err == nil)
	pl := remote.NewTransPipeline(svrHandler)
	svrHandler.SetPipeline(pl)
	err = svrHandler.OnRead(ctx, &mocks.Conn{})
	test.Assert(t, err != nil)
	test.Assert(t, strings.Contains(err.Error(), "panic"))
}

func TestSvrTransHandlerOnReadHeartbeat(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracer := stats.NewMockTracer(ctrl)
	mockTracer.EXPECT().Start(gomock.Any()).DoAndReturn(func(ctx context.Context) context.Context { return ctx }).AnyTimes()
	mockTracer.EXPECT().Finish(gomock.Any()).DoAndReturn(func(ctx context.Context) {
		err := rpcinfo.GetRPCInfo(ctx).Stats().Error()
		test.Assert(t, err == nil)
	}).AnyTimes()

	buf := remote.NewReaderWriterBuffer(1024)
	ext := &MockExtension{
		NewWriteByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
		NewReadByteBufferFunc: func(ctx context.Context, conn net.Conn, msg remote.Message) remote.ByteBuffer {
			return buf
		},
	}

	tracerCtl := &rpcinfo.TraceController{}
	tracerCtl.Append(mockTracer)
	opt := &remote.ServerOption{
		Codec: &MockCodec{
			EncodeFunc: func(ctx context.Context, msg remote.Message, out remote.ByteBuffer) error {
				if msg.MessageType() != remote.Heartbeat {
					return errors.New("response is not of MessageType Heartbeat")
				}
				return nil
			},
			DecodeFunc: func(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
				msg.SetMessageType(remote.Heartbeat)
				return nil
			},
		},
		SvcSearcher:            svcSearcher,
		TracerCtl:              tracerCtl,
		InitOrResetRPCInfoFunc: initOrResetMockRPCInfo,
	}
	ri := rpcinfo.NewRPCInfo(rpcinfo.EmptyEndpointInfo(), rpcinfo.FromBasicInfo(&rpcinfo.EndpointBasicInfo{}),
		rpcinfo.NewInvocation("", mocks.MockMethod), nil, rpcinfo.NewRPCStats())
	ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)

	svrHandler, err := NewDefaultSvrTransHandler(opt, ext)
	test.Assert(t, err == nil)
	pl := remote.NewTransPipeline(svrHandler)
	svrHandler.SetPipeline(pl)
	err = svrHandler.OnRead(ctx, &mocks.Conn{})
	test.Assert(t, err == nil)
}
