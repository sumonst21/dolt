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

package datas

import (
	"context"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"math/bits"

	"github.com/dolthub/dolt/go/store/types"
)

func getParentsSkipListHeight(ctx context.Context, parents types.List) (int, error) {
	h := sha512.New()
	err := parents.IterAll(ctx, func(v types.Value, i uint64) error {
		r, ok := v.(types.Ref)
		if !ok {
			return errors.New("unexpected parents list entry; not a Ref")
		}
		targetHash := r.TargetHash()
		h.Write(targetHash[:])
		return nil
	})
	if err != nil {
		return 0, err
	}
	b := h.Sum(nil)
	ui32 := binary.BigEndian.Uint32(b[:])
	ret := bits.TrailingZeros32(ui32)
	if ret == 32 {
		ret -= 1
	}
	return ret, nil
}

func getSkipListNodeFromCommitRef(ctx context.Context, vrw types.ValueReadWriter, r types.Ref) (types.Tuple, error) {
	tv, err := r.TargetValue(ctx, vrw)
	if err != nil {
		return types.EmptyTuple(vrw.Format()), err
	}
	commit := tv.(types.Struct)
	slnVal, found, err := commit.MaybeGet(ParentsSkipListField)
	if !found {
		return types.EmptyTuple(vrw.Format()), nil
	}
	slnTuple, ok := slnVal.(types.Tuple)
	if !ok {
		return types.EmptyTuple(vrw.Format()), errors.New("unexpected parents skip list entry; not a Tuple")
	}
	if slnTuple.Len() == 0 {
		return types.EmptyTuple(vrw.Format()), nil
	}
	cVal, err := slnTuple.Get(0)
	if err != nil {
		return types.EmptyTuple(vrw.Format()), err
	}
	ret, ok := cVal.(types.Tuple)
	if !ok {
		return types.EmptyTuple(vrw.Format()), errors.New("unexpected first element in skip list entry Tuple; not a Tuple")
	}
	return ret, nil
}

func getParentSkipListNodeOfHeight(ctx context.Context, vrw types.ValueReadWriter, parentRef types.Ref, height int) (types.Tuple, error) {
	// Return a tuple of length (height + 1). The i-th entry in the Tuple
	// will point to a RefOfValue for the first commit found, starting at
	// parentRef and walking it's first parents_skip_list entries, where
	// the ref'd Commit's parents_skip_list entry itself has at least level
	// i. The 0-th entry always points to parentRef. If sufficiently high
	// skip list nodes are not found, the returned Tuple has
	// |types.NullValue| value entries.

	var err error
	currRef, err := types.ToRefOfValue(parentRef, vrw.Format())
	if err != nil {
		return types.EmptyTuple(vrw.Format()), err
	}

	if height == 0 {
		return types.NewTuple(vrw.Format(), currRef)
	}

	entries := make([]types.Value, height+1)
	for i := range entries {
		if i == 0 {
			entries[i] = currRef
		} else {
			entries[i] = types.NullValue
		}
	}

	currSkipListNode, err := getSkipListNodeFromCommitRef(ctx, vrw, currRef)
ENTRIES:
	for i := 1; i <= height; i++ {
		for i+1 > int(currSkipListNode.Len()) {
			if currSkipListNode.Len() == 0 {
				break ENTRIES
			}
			crVal, err := currSkipListNode.Get(currSkipListNode.Len() - 1)
			if err != nil {
				return types.EmptyTuple(vrw.Format()), err
			}
			if types.IsNull(crVal) {
				break ENTRIES
			}
			var ok bool
			currRef, ok = crVal.(types.Ref)
			if !ok {
				return types.EmptyTuple(vrw.Format()), errors.New("unexpected last element in skip list node; not Null or Tuple")
			}

			currSkipListNode, err = getSkipListNodeFromCommitRef(ctx, vrw, currRef)
			if err != nil {
				return types.EmptyTuple(vrw.Format()), err
			}
		}
		entries[i] = currRef
	}

	return types.NewTuple(vrw.Format(), entries...)
}

func getParentsSkipList(ctx context.Context, vrw types.ValueReadWriter, parents types.List) (types.Tuple, error) {
	if parents.Len() == 0 {
		return types.EmptyTuple(vrw.Format()), nil
	}

	parentHeight, err := getParentsSkipListHeight(ctx, parents)
	if err != nil {
		return types.EmptyTuple(vrw.Format()), nil
	}

	entries := make([]types.Value, parents.Len())
	err = parents.IterAll(ctx, func(v types.Value, i uint64) error {
		r, ok := v.(types.Ref)
		if !ok {
			return errors.New("unexpected parents list entry; not a Ref")
		}
		entry, err := getParentSkipListNodeOfHeight(ctx, vrw, r, parentHeight)
		if err != nil {
			return err
		}
		entries[i] = entry
		return nil
	})
	if err != nil {
		return types.EmptyTuple(vrw.Format()), err
	}
	return types.NewTuple(vrw.Format(), entries...)
}

