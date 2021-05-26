// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package edits

import (
	"sync"

	"github.com/dolthub/dolt/go/store/types"
)

var aseBufferPool = &sync.Pool{New: newASEBuff}
var aseBufferSize = 16 * 1024

func newASEBuff() interface{} {
	return make(types.KVPSlice, 0, aseBufferSize)
}

type BuffPool struct{}

func (p BuffPool) Get() types.KVPSlice {
	return aseBufferPool.Get().(types.KVPSlice)
}

func (p BuffPool) Put(s types.KVPSlice) {
	// todo: reset len?
	aseBufferPool.Put(s)
}
