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

package rpcinfo

import (
	"github.com/cloudwego/kitex/pkg/stats"
)

type inlineRPCInfo struct {
	from       endpointInfo
	to         endpointInfo
	invocation invocation
	config     rpcConfig
	stats      rpcStats
}

// From implements the RPCInfo interface.
func (r *inlineRPCInfo) From() EndpointInfo { return &r.from }

// To implements the RPCInfo interface.
func (r *inlineRPCInfo) To() EndpointInfo { return &r.to }

// Invocation implements the RPCInfo interface.
func (r *inlineRPCInfo) Invocation() Invocation { return &r.invocation }

// Config implements the RPCInfo interface.
func (r *inlineRPCInfo) Config() RPCConfig { return &r.config }

// Stats implements the RPCInfo interface.
func (r *inlineRPCInfo) Stats() RPCStats { return &r.stats }

// Recycle is kept for compatibility. RPCInfo is no longer reused.
func (r *inlineRPCInfo) Recycle() {}

// NewRPCInfoWithInlineFields creates an RPCInfo using inlined concrete fields,
// avoiding separate pool allocations for from, to, invocation, config, and stats.
// The returned RPCInfo's From(), To(), Invocation(), Config(), and Stats() return
// pointers to the inlined fields. Use AsMutable* to modify them after creation.
func NewRPCInfoWithInlineFields() RPCInfo {
	return newInlineRPCInfo()
}

func newInlineRPCInfo() *inlineRPCInfo {
	once.Do(func() {
		stats.FinishInitialization()
		maxEventNum = stats.MaxEventNum()
	})
	r := &inlineRPCInfo{}
	r.config.initialize()
	r.stats.eventMap = make([]event, maxEventNum)
	return r
}
