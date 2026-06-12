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

// EnablePool is kept for API compatibility. RPCInfo pooling has been removed.
func EnablePool(enable bool) {}

// PoolEnabled returns false because RPCInfo pooling has been removed.
func PoolEnabled() bool {
	return false
}

type rpcInfo struct {
	from       EndpointInfo
	to         EndpointInfo
	invocation Invocation
	config     RPCConfig
	stats      RPCStats
}

// From implements the RPCInfo interface.
func (r *rpcInfo) From() EndpointInfo { return r.from }

// To implements the RPCInfo interface.
func (r *rpcInfo) To() EndpointInfo { return r.to }

// Config implements the RPCInfo interface.
func (r *rpcInfo) Config() RPCConfig { return r.config }

// Invocation implements the RPCInfo interface.
func (r *rpcInfo) Invocation() Invocation { return r.invocation }

// Stats implements the RPCInfo interface.
func (r *rpcInfo) Stats() RPCStats { return r.stats }

func (r *rpcInfo) zero() {
	r.from = nil
	r.to = nil
	r.invocation = nil
	r.config = nil
	r.stats = nil
}

// Recycle is kept for compatibility. RPCInfo is no longer reused.
func (r *rpcInfo) Recycle() {}

// NewRPCInfo creates a new RPCInfo using the given information.
func NewRPCInfo(from, to EndpointInfo, ink Invocation, config RPCConfig, stats RPCStats) RPCInfo {
	return &rpcInfo{
		from:       from,
		to:         to,
		invocation: ink,
		config:     config,
		stats:      stats,
	}
}
