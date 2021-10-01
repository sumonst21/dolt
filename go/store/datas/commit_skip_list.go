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

type ParentsSkipList struct {
	types.Tuple
}

func (p ParentsSkipList) Len() uint64 {
	return p.Tuple.Len()
}

func (p ParentsSkipList) Get(n uint64) (SkipListNode, error) {
	v, err := p.Tuple.Get(n)
	if err != nil {
		return SkipListNode{}, err
	}
	t, ok := v.(types.Tuple)
	if !ok {
		return SkipListNode{}, errors.New("unexpected element in skip list entry Tuple; not a Tuple")
	}
	return SkipListNode{t}, nil
}

type SkipListNode struct {
	types.Tuple
}

func (n SkipListNode) Height() uint64 {
	return n.Tuple.Len()
}

func (n SkipListNode) GetRef(i uint64) (types.Ref, bool, error) {
	v, err := n.Get(i)
	if err != nil {
		return types.Ref{}, false, err
	}
	if types.IsNull(v) {
		return types.Ref{}, false, nil
	}
	r, ok := v.(types.Ref)
	if !ok {
		return types.Ref{}, false, errors.New("unexpected element in skip list entry Tuple; not Null or Ref")
	}
	return r, true, nil
}

func LoadParentsSkipListFromCommitRef(ctx context.Context, vrw types.ValueReadWriter, ref types.Ref) (ParentsSkipList, bool, error) {
	val, err := ref.TargetValue(ctx, vrw)
	if err != nil {
		return ParentsSkipList{}, false, err
	}
	commit, ok := val.(types.Struct)
	if !ok {
		return ParentsSkipList{}, false, errors.New("expected ref to be a Commit Struct; was not")
	}
	return LoadParentsSkipListFromCommit(commit)
}

func LoadParentsSkipListFromCommit(commit types.Struct) (ParentsSkipList, bool, error) {
	val, found, err := commit.MaybeGet(ParentsSkipListField)
	if err != nil {
		return ParentsSkipList{}, false, err
	}
	if !found {
		return ParentsSkipList{}, false, nil
	}
	tuple, ok := val.(types.Tuple)
	if !ok {
		return ParentsSkipList{}, false, errors.New("unexpected parents skip list entry; not a Tuple")
	}
	return ParentsSkipList{tuple}, true, nil
}

var SkipListNodeHeightForParents = func(ctx context.Context, parents types.List) (int, error) {
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
	return ret + 1, nil
}

func getSkipListNodeFromCommitRef(ctx context.Context, vrw types.ValueReadWriter, r types.Ref) (SkipListNode, error) {
	sl, found, err := LoadParentsSkipListFromCommitRef(ctx, vrw, r)
	if err != nil {
		return SkipListNode{}, err
	}
	if !found || sl.Len() == 0 {
		return SkipListNode{types.EmptyTuple(vrw.Format())}, nil
	}
	return sl.Get(0)
}

func getParentSkipListNodeOfHeight(ctx context.Context, vrw types.ValueReadWriter, parentRef types.Ref, height int) (types.Tuple, error) {
	// Return a tuple of length height. The i-th entry in the Tuple
	// will point to a RefOfValue for the first commit found, starting at
	// parentRef and walking it's first parents_skip_list entries, where
	// the ref'd Commit's parents_skip_list entry itself has at least level
	// i. The 0-th entry always points to parentRef. If sufficiently high
	// skip list nodes are not found, the returned Tuple has
	// |types.NullValue| value entries.

	var err error
	currRef, err := types.ToRefOfValue(parentRef, vrw.Format())
	if err != nil {
		return types.Tuple{}, err
	}

	if height == 1 {
		return types.NewTuple(vrw.Format(), currRef)
	}

	entries := make([]types.Value, height)
	for i := range entries {
		if i == 0 {
			entries[i] = currRef
		} else {
			entries[i] = types.NullValue
		}
	}

	currNode, err := getSkipListNodeFromCommitRef(ctx, vrw, currRef)
	if err != nil {
		return types.Tuple{}, err
	}
ENTRIES:
	for i := 1; i < height; i++ {
		for int(currNode.Height()) < i+1 {
			if currNode.Height() == 0 {
				break ENTRIES
			}
			var found bool
			currRef, found, err = currNode.GetRef(currNode.Height() - 1)
			if err != nil {
				return types.Tuple{}, err
			}
			if !found {
				break ENTRIES
			}
			currNode, err = getSkipListNodeFromCommitRef(ctx, vrw, currRef)
			if err != nil {
				return types.Tuple{}, err
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

	parentHeight, err := SkipListNodeHeightForParents(ctx, parents)
	if err != nil {
		return types.Tuple{}, err
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
		return types.Tuple{}, err
	}
	return types.NewTuple(vrw.Format(), entries...)
}
