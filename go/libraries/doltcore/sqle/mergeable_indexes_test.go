// Copyright 2020 Dolthub, Inc.
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

package sqle

import (
	"context"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
)

// This tests mergeable indexes by using the SQL engine and intercepting specific calls. This way, we can verify that
// the engine is intersecting and combining the proper number of lookups, and we can also examine the ranges before
// they're converted into a format that Noms understands to verify that they were handled correctly. Lastly, we ensure
// that the final output is as expected.
func TestMergeableIndexes(t *testing.T) {
	engine, denv, db, indexTuples, initialRoot := setupIndexes(t, "test", `INSERT INTO test VALUES
		(-3, NULL, NULL),
		(-2, NULL, NULL),
		(-1, NULL, NULL),
		(0, 10, 20),
		(1, 11, 21),
		(2, 12, 22),
		(3, 13, 23),
		(4, 14, 24),
		(5, 15, 25),
		(6, 16, 26),
		(7, 17, 27),
		(8, 18, 28),
		(9, 19, 29);`)
	idxv1, idxv2v1, idxv2v1Gen := indexTuples[0], indexTuples[1], indexTuples[2]

	tests := []struct {
		whereStmt   string
		finalRanges []*noms.ReadRange
		pks         []int64
	}{
		{
			"v1 = 11",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 = 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
				closedRange(idxv1.tuple(19), idxv1.tuple(19)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 = 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 = 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 != 11",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 OR v1 != 15",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(15)),
				lessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 != 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 != 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(19)),
				greaterThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 != 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 != 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(19)),
				greaterThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 != 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 > 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{1, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 > 15",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 > 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
				greaterThanRange(idxv1.tuple(19)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 > 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 > 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(19)),
			},
			[]int64{},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 > 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 >= 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				greaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{1, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 >= 15",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 >= 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
				greaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 >= 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 >= 19",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 >= 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 < 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 = 11 AND v1 < 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 < 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 < 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 < 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 < 19",
			nil,
			[]int64{},
		},
		{
			"v1 = 11 OR v1 <= 15",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 = 11 AND v1 <= 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
			},
			[]int64{1},
		},
		{
			"v1 = 11 OR v1 = 15 OR v1 <= 19",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 OR v1 = 15 AND v1 <= 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{1, 5},
		},
		{
			"v1 = 11 AND v1 = 15 OR v1 <= 19",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 = 11 AND v1 = 15 AND v1 <= 19",
			nil,
			[]int64{},
		},
		{
			"v1 != 11",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <> 11",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <> 11 OR v1 <> 15",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 <> 11 AND v1 <> 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 != 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <> 11 OR v1 <> 15 OR v1 <> 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 != 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <> 11 OR v1 <> 15 AND v1 <> 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 != 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				openRange(idxv1.tuple(15), idxv1.tuple(19)),
				greaterThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8},
		},
		{
			"v1 <> 11 AND v1 <> 15 AND v1 <> 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				openRange(idxv1.tuple(15), idxv1.tuple(19)),
				greaterThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8},
		},
		{
			"v1 != 11 OR v1 > 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 > 15",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 > 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 > 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 OR v1 > 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 > 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(19)),
			},
			[]int64{},
		},
		{
			"v1 != 11 OR v1 >= 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 >= 15",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 >= 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 >= 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{0, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 OR v1 >= 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 >= 19",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 != 11 OR v1 < 15",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 < 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{0, 2, 3, 4},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 < 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 < 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 OR v1 < 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 < 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				openRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8},
		},
		{
			"v1 != 11 OR v1 <= 15",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 <= 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				customRange(idxv1.tuple(11), idxv1.tuple(15), sql.Open, sql.Closed),
			},
			[]int64{0, 2, 3, 4, 5},
		},
		{
			"v1 != 11 OR v1 != 15 OR v1 <= 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 OR v1 != 15 AND v1 <= 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 OR v1 <= 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 != 11 AND v1 != 15 AND v1 <= 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				customRange(idxv1.tuple(15), idxv1.tuple(19), sql.Open, sql.Closed),
			},
			[]int64{0, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 > 11",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 > 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 > 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 > 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(19)),
			},
			[]int64{},
		},
		{
			"v1 > 11 OR v1 >= 15",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 >= 15",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 >= 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 >= 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 >= 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 >= 19",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 > 11 OR v1 < 15",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 < 15",
			[]*noms.ReadRange{
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{2, 3, 4},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 < 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 < 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 < 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 < 19",
			[]*noms.ReadRange{
				openRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{6, 7, 8},
		},
		{
			"v1 > 11 OR v1 <= 15",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 <= 15",
			[]*noms.ReadRange{
				customRange(idxv1.tuple(11), idxv1.tuple(15), sql.Open, sql.Closed),
			},
			[]int64{2, 3, 4, 5},
		},
		{
			"v1 > 11 OR v1 > 15 OR v1 <= 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 OR v1 > 15 AND v1 <= 19",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(11)),
			},
			[]int64{2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 OR v1 <= 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 <= 19",
			[]*noms.ReadRange{
				customRange(idxv1.tuple(15), idxv1.tuple(19), sql.Open, sql.Closed),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 > 15 AND v1 <= 19",
			[]*noms.ReadRange{
				customRange(idxv1.tuple(15), idxv1.tuple(19), sql.Open, sql.Closed),
			},
			[]int64{6, 7, 8, 9},
		},
		{
			"v1 > 11 AND v1 < 15 OR v1 > 15 AND v1 < 19",
			[]*noms.ReadRange{
				openRange(idxv1.tuple(11), idxv1.tuple(15)),
				openRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{2, 3, 4, 6, 7, 8},
		},
		{
			"v1 >= 11",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 >= 19",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 >= 19",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 >= 19",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{9},
		},
		{
			"v1 >= 11 OR v1 < 15",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 < 15",
			[]*noms.ReadRange{
				customRange(idxv1.tuple(11), idxv1.tuple(15), sql.Closed, sql.Open),
			},
			[]int64{1, 2, 3, 4},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 < 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 < 19",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 OR v1 < 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 < 19",
			[]*noms.ReadRange{
				customRange(idxv1.tuple(15), idxv1.tuple(19), sql.Closed, sql.Open),
			},
			[]int64{5, 6, 7, 8},
		},
		{
			"v1 >= 11 OR v1 <= 15",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 <= 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 >= 11 OR v1 >= 15 OR v1 <= 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 OR v1 >= 15 AND v1 <= 19",
			[]*noms.ReadRange{
				greaterOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 OR v1 <= 19",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 >= 15 AND v1 <= 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(15), idxv1.tuple(19)),
			},
			[]int64{5, 6, 7, 8, 9},
		},
		{
			"v1 >= 11 AND v1 <= 14 OR v1 >= 16 AND v1 <= 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(14)),
				closedRange(idxv1.tuple(16), idxv1.tuple(19)),
			},
			[]int64{1, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v1 < 11",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15 OR v1 < 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v1 < 11 OR v1 < 15 AND v1 < 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15 AND v1 < 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 > 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 > 15",
			nil,
			[]int64{},
		},
		{
			"v1 < 11 OR v1 <= 15",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 < 11 AND v1 <= 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 < 15 OR v1 <= 19",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 OR v1 < 15 AND v1 <= 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4},
		},
		{
			"v1 < 11 AND v1 < 15 OR v1 <= 19",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 < 15 AND v1 <= 19",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
			},
			[]int64{0},
		},
		{
			"v1 < 11 OR v1 >= 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(11)),
				greaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 5, 6, 7, 8, 9},
		},
		{
			"v1 < 11 AND v1 >= 15",
			nil,
			[]int64{},
		},
		{
			"(v1 < 13 OR v1 > 16) AND (v1 > 10 OR v1 < 19)",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(13)),
				greaterThanRange(idxv1.tuple(16)),
			},
			[]int64{0, 1, 2, 7, 8, 9},
		},
		{
			"v1 <= 11",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 <= 15",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 <= 11 AND v1 <= 15",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 <= 15 OR v1 <= 19",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(19)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 OR v1 <= 15 AND v1 <= 19",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 2, 3, 4, 5},
		},
		{
			"v1 <= 11 AND v1 <= 15 AND v1 <= 19",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(11)),
			},
			[]int64{0, 1},
		},
		{
			"v1 <= 11 OR v1 > 15",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(11)),
				greaterThanRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 AND v1 > 15",
			nil,
			[]int64{},
		},
		{
			"v1 <= 11 OR v1 >= 15",
			[]*noms.ReadRange{
				lessOrEqualRange(idxv1.tuple(11)),
				greaterOrEqualRange(idxv1.tuple(15)),
			},
			[]int64{0, 1, 5, 6, 7, 8, 9},
		},
		{
			"v1 <= 11 AND v1 >= 15",
			nil,
			[]int64{},
		},
		{
			"v1 BETWEEN 11 AND 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 BETWEEN 15 AND 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(19)),
			},
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 BETWEEN 15 AND 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 = 13",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 OR v1 != 13",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 != 13",
			[]*noms.ReadRange{
				customRange(idxv1.tuple(11), idxv1.tuple(13), sql.Closed, sql.Open),
				customRange(idxv1.tuple(13), idxv1.tuple(15), sql.Open, sql.Closed),
			},
			[]int64{1, 2, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 <= 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 BETWEEN 11 AND 15 AND v1 <= 19",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(15)),
			},
			[]int64{1, 2, 3, 4, 5},
		},
		{
			"v1 IN (11, 12, 13)",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(12), idxv1.tuple(12)),
				closedRange(idxv1.tuple(13), idxv1.tuple(13)),
			},
			[]int64{1, 2, 3},
		},
		{
			"v1 IN (11, 12, 13) OR v1 BETWEEN 11 and 13",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(13)),
			},
			[]int64{1, 2, 3},
		},
		{
			"v1 IN (11, 12, 13) AND v1 > 11",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(12), idxv1.tuple(12)),
				closedRange(idxv1.tuple(13), idxv1.tuple(13)),
			},
			[]int64{2, 3},
		},
		{
			"v1 IN (11, 12, 13) OR v1 != 12",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 IN (11, 12, 13) AND v1 != 12",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(13), idxv1.tuple(13)),
			},
			[]int64{1, 3},
		},
		{
			"v1 IN (11, 12, 13) OR v1 >= 13 AND v1 < 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.tuple(12), idxv1.tuple(12)),
				customRange(idxv1.tuple(13), idxv1.tuple(15), sql.Closed, sql.Open),
			},
			[]int64{1, 2, 3, 4},
		},
		{
			"v2 = 21 AND v1 = 11 OR v2 > 25 AND v1 > 11",
			[]*noms.ReadRange{
				closedRange(idxv2v1.tuple(21, 11), idxv2v1.tuple(21, 11)),
				greaterThanRange(idxv2v1.tuple(25, 11)),
			},
			[]int64{1, 6, 7, 8, 9},
		},
		{
			"v2 > 21 AND v1 > 11 AND v2 < 25 AND v1 < 15",
			[]*noms.ReadRange{
				openRange(idxv2v1.tuple(21, 11), idxv2v1.tuple(25, 15)),
			},
			[]int64{2, 3, 4},
		},
		{
			"v2 = 21",
			[]*noms.ReadRange{
				closedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
			},
			[]int64{1},
		},
		{
			"v2 = 21 OR v2 = 25",
			[]*noms.ReadRange{
				closedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
				closedRange(idxv2v1Gen.tuple(25), idxv2v1Gen.tuple(25)),
			},
			[]int64{1, 5},
		},
		{
			"v2 = 21 AND v2 = 25",
			nil,
			[]int64{},
		},
		{
			"v2 = 21 OR v2 = 25 OR v2 = 29",
			[]*noms.ReadRange{
				closedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
				closedRange(idxv2v1Gen.tuple(25), idxv2v1Gen.tuple(25)),
				closedRange(idxv2v1Gen.tuple(29), idxv2v1Gen.tuple(29)),
			},
			[]int64{1, 5, 9},
		},
		{
			"v2 = 21 OR v2 = 25 AND v2 = 29",
			[]*noms.ReadRange{
				closedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
			},
			[]int64{1},
		},
		{
			"v2 = 21 AND v2 = 25 AND v2 = 29",
			nil,
			[]int64{},
		},
		{
			"v2 = 21 OR v2 != 21",
			[]*noms.ReadRange{
				allRange(idxv2v1Gen.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v2 = 21 OR v2 != 25",
			[]*noms.ReadRange{
				lessThanRange(idxv2v1Gen.tuple(25)),
				greaterThanRange(idxv2v1Gen.tuple(25)),
			},
			[]int64{0, 1, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			"v2 = 21 AND v2 != 25",
			[]*noms.ReadRange{
				closedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
			},
			[]int64{1},
		},
		{
			"v2 = 21 OR v2 = 25 OR v2 != 29",
			[]*noms.ReadRange{
				lessThanRange(idxv2v1Gen.tuple(29)),
				greaterThanRange(idxv2v1Gen.tuple(29)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v2 = 21 OR v2 = 25 AND v2 != 29",
			[]*noms.ReadRange{
				closedRange(idxv2v1Gen.tuple(21), idxv2v1Gen.tuple(21)),
				closedRange(idxv2v1Gen.tuple(25), idxv2v1Gen.tuple(25)),
			},
			[]int64{1, 5},
		},
		{
			"v2 = 21 AND v2 = 25 OR v2 != 29",
			[]*noms.ReadRange{
				lessThanRange(idxv2v1Gen.tuple(29)),
				greaterThanRange(idxv2v1Gen.tuple(29)),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			"v2 = 21 AND v2 = 25 AND v2 != 29",
			nil,
			[]int64{},
		},
	}

	for _, test := range tests {
		t.Run(test.whereStmt, func(t *testing.T) {
			var finalRanges []*noms.ReadRange
			db.t = t
			db.finalRanges = func(ranges []*noms.ReadRange) {
				finalRanges = ranges
			}

			ctx := context.Background()
			sqlCtx := NewTestSQLCtx(ctx)
			session := dsess.DSessFromSess(sqlCtx.Session)
			dbState := getDbState(t, db, denv)
			err := session.AddDB(sqlCtx, dbState)

			require.NoError(t, err)
			sqlCtx.SetCurrentDatabase(db.Name())
			err = session.SetRoot(sqlCtx, db.Name(), initialRoot)
			require.NoError(t, err)

			_, iter, err := engine.Query(sqlCtx, fmt.Sprintf(`SELECT pk FROM test WHERE %s ORDER BY 1`, test.whereStmt))
			require.NoError(t, err)
			res, err := sql.RowIterToRows(sqlCtx, iter)
			require.NoError(t, err)
			if assert.Equal(t, len(test.pks), len(res)) {
				for i, pk := range test.pks {
					if assert.Equal(t, 1, len(res[i])) {
						assert.Equal(t, pk, res[i][0])
					}
				}
			}

			if assert.Equal(t, len(test.finalRanges), len(finalRanges)) {
				finalRangeMatches := make([]bool, len(finalRanges))
				for _, finalRange := range finalRanges {
					for i, testFinalRange := range test.finalRanges {
						if readRangesEqual(finalRange, testFinalRange) {
							if finalRangeMatches[i] {
								require.FailNow(t, fmt.Sprintf("Duplicate ReadRange: `%v`", finalRange))
							} else {
								finalRangeMatches[i] = true
							}
						}
					}
				}
				for _, finalRangeMatch := range finalRangeMatches {
					if !finalRangeMatch {
						require.FailNow(t, fmt.Sprintf("Expected: `%v`\nActual:   `%v`", test.finalRanges, finalRanges))
					}
				}
			}
		})
	}
}

// TestMergeableIndexesNulls is based on TestMergeableIndexes, but specifically handles IS NULL and IS NOT NULL.
// For now, some of these tests are broken, but they return the correct end result. As NULL is encoded as being a value
// larger than all integers, == NULL becomes a subset of > x and >= x, thus the intersection returns == NULL.
// The correct behavior would be to return the empty range in that example. However, as the SQL engine still filters the
// returned results, we end up with zero values actually being returned, just like we'd expect from the empty range.
// As a consequence, I'm leaving these tests in to verify that the overall result is correct, but the intermediate
// ranges may be incorrect.
// TODO: disassociate NULL ranges from value ranges and fix the intermediate ranges (finalRanges).
func TestMergeableIndexesNulls(t *testing.T) {
	engine, denv, db, indexTuples, initialRoot := setupIndexes(t, "test", `INSERT INTO test VALUES
		(0, 10, 20),
		(1, 11, 21),
		(2, NULL, NULL),
		(3, 13, 23),
		(4, NULL, NULL),
		(5, 15, 25),
		(6, NULL, NULL),
		(7, 17, 27),
		(8, 18, 28),
		(9, 19, 29);`)
	idxv1 := indexTuples[0]

	tests := []struct {
		whereStmt   string
		finalRanges []*noms.ReadRange
		pks         []int64
	}{
		{
			"v1 IS NULL",
			[]*noms.ReadRange{
				closedRange(idxv1.nilTuple(), idxv1.nilTuple()),
			},
			[]int64{2, 4, 6},
		},
		{
			"v1 IS NULL OR v1 IS NULL",
			[]*noms.ReadRange{
				closedRange(idxv1.nilTuple(), idxv1.nilTuple()),
			},
			[]int64{2, 4, 6},
		},
		{
			"v1 IS NULL AND v1 IS NULL",
			[]*noms.ReadRange{
				closedRange(idxv1.nilTuple(), idxv1.nilTuple()),
			},
			[]int64{2, 4, 6},
		},
		{
			"v1 IS NULL OR v1 = 11",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(11), idxv1.tuple(11)),
				closedRange(idxv1.nilTuple(), idxv1.nilTuple()),
			},
			[]int64{1, 2, 4, 6},
		},
		{
			"v1 IS NULL OR v1 < 16",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(16)),
				closedRange(idxv1.nilTuple(), idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6},
		},
		{
			"v1 IS NULL OR v1 > 16",
			[]*noms.ReadRange{
				greaterThanRange(idxv1.tuple(16)),
			},
			[]int64{2, 4, 6, 7, 8, 9},
		},
		{
			"v1 IS NULL AND v1 < 16",
			nil,
			[]int64{},
		},
		{
			"v1 IS NULL AND v1 > 16",
			[]*noms.ReadRange{
				closedRange(idxv1.nilTuple(), idxv1.nilTuple()),
			},
			[]int64{},
		},
		{
			"v1 IS NULL OR v1 IS NOT NULL",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 IS NULL AND v1 IS NOT NULL",
			nil,
			[]int64{},
		},
		{
			"v1 IS NOT NULL",
			[]*noms.ReadRange{
				lessThanRange(idxv1.nilTuple()),
				greaterThanRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 3, 5, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL OR v1 IS NULL",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL AND v1 IS NULL",
			nil,
			[]int64{},
		},
		{
			"v1 IS NOT NULL OR v1 = 15",
			[]*noms.ReadRange{
				lessThanRange(idxv1.nilTuple()),
				greaterThanRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 3, 5, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL OR v1 > 16",
			[]*noms.ReadRange{
				allRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 3, 5, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL OR v1 < 16",
			[]*noms.ReadRange{
				lessThanRange(idxv1.nilTuple()),
				greaterThanRange(idxv1.nilTuple()),
			},
			[]int64{0, 1, 3, 5, 7, 8, 9},
		},
		{
			"v1 IS NOT NULL AND v1 = 15",
			[]*noms.ReadRange{
				closedRange(idxv1.tuple(15), idxv1.tuple(15)),
			},
			[]int64{5},
		},
		{
			"v1 IS NOT NULL AND v1 > 16",
			[]*noms.ReadRange{
				openRange(idxv1.tuple(16), idxv1.nilTuple()),
				greaterThanRange(idxv1.nilTuple()),
			},
			[]int64{7, 8, 9},
		},
		{
			"v1 IS NOT NULL AND v1 < 16",
			[]*noms.ReadRange{
				lessThanRange(idxv1.tuple(16)),
			},
			[]int64{0, 1, 3, 5},
		},
	}

	for _, test := range tests {
		t.Run(test.whereStmt, func(t *testing.T) {
			var finalRanges []*noms.ReadRange
			db.t = t
			db.finalRanges = func(ranges []*noms.ReadRange) {
				finalRanges = ranges
			}

			ctx := context.Background()
			sqlCtx := NewTestSQLCtx(ctx)
			session := dsess.DSessFromSess(sqlCtx.Session)
			dbState := getDbState(t, db, denv)
			err := session.AddDB(sqlCtx, dbState)
			require.NoError(t, err)
			sqlCtx.SetCurrentDatabase(db.Name())
			err = session.SetRoot(sqlCtx, db.Name(), initialRoot)
			require.NoError(t, err)

			_, iter, err := engine.Query(sqlCtx, fmt.Sprintf(`SELECT pk FROM test WHERE %s ORDER BY 1`, test.whereStmt))
			require.NoError(t, err)
			res, err := sql.RowIterToRows(sqlCtx, iter)
			require.NoError(t, err)
			if assert.Equal(t, len(test.pks), len(res)) {
				for i, pk := range test.pks {
					if assert.Equal(t, 1, len(res[i])) {
						assert.Equal(t, pk, res[i][0])
					}
				}
			}

			if assert.Equal(t, len(test.finalRanges), len(finalRanges)) {
				finalRangeMatches := make([]bool, len(finalRanges))
				for _, finalRange := range finalRanges {
					for i, testFinalRange := range test.finalRanges {
						if readRangesEqual(finalRange, testFinalRange) {
							if finalRangeMatches[i] {
								require.FailNow(t, fmt.Sprintf("Duplicate ReadRange: `%v`", finalRange))
							} else {
								finalRangeMatches[i] = true
							}
						}
					}
				}
				for _, finalRangeMatch := range finalRangeMatches {
					if !finalRangeMatch {
						require.FailNow(t, fmt.Sprintf("Expected: `%v`\nActual:   `%v`", test.finalRanges, finalRanges))
					}
				}
			}
		})
	}
}
