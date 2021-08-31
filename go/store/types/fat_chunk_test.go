// Copyright 2019 Dolthub, Inc.
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

package types

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFatChunk(t *testing.T) {
	require.True(t, true)

	ctx := context.Background()
	nbf := Format_Default
	vrw := NewMemoryValueStore()

	const o_w_id_tag = 2807
	const o_w_id_bound = 9

	const o_d_id_tag = 6633
	const o_d_id_bound = 10

	const o_id_tag = 13743
	const o_id_bound = 3200

	m, err := NewMap(ctx, vrw)
	require.NoError(t, err)
	me := m.Edit()

	empty, _ := NewTuple(nbf)

	o_w := 1
	for o_w <= o_w_id_bound {

		o_d := 1
		for o_d <= o_d_id_bound {

			o_id := 1
			for o_id <= o_id_bound {

				tup, err := NewTuple(
					nbf,
					Uint(o_w_id_tag),
					Uint(o_w),
					Uint(o_d_id_tag),
					Uint(o_d),
					Uint(o_id_tag),
					Uint(o_id),
				)
				require.NoError(t, err)
				me.set(tup, empty)

				o_id++
			}
			o_d++
		}
		o_w++
	}

	m, err = me.Map(ctx)
	require.NoError(t, err)
	r, err := vrw.WriteValue(ctx, m)
	require.NoError(t, err)
	h, err := r.Hash(nbf)
	require.NoError(t, err)

	// flush
	cs := vrw.ChunkStore()
	root, err := cs.Root(ctx)
	require.NoError(t, err)
	ok, err := cs.Commit(ctx, h, root)
	require.NoError(t, err)
	require.True(t, ok)

	fmt.Println(cs.StatsSummary())
}