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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/nomdl"
	"github.com/dolthub/dolt/go/store/types"
)

func mustHead(ds Dataset) types.Struct {
	s, ok := ds.MaybeHead()
	if !ok {
		panic("no head")
	}

	return s
}

func mustHeadRef(ds Dataset) types.Ref {
	hr, ok, err := ds.MaybeHeadRef()

	if err != nil {
		panic("error getting head")
	}

	if !ok {
		panic("no head")
	}

	return hr
}

func mustHeadValue(ds Dataset) types.Value {
	val, ok, err := ds.MaybeHeadValue()

	if err != nil {
		panic("error getting head")
	}

	if !ok {
		panic("no head")
	}

	return val
}

func mustString(str string, err error) string {
	d.PanicIfError(err)
	return str
}

func mustStruct(st types.Struct, err error) types.Struct {
	d.PanicIfError(err)
	return st
}

func mustSet(s types.Set, err error) types.Set {
	d.PanicIfError(err)
	return s
}

func mustList(l types.List, err error) types.List {
	d.PanicIfError(err)
	return l
}

func mustType(t *types.Type, err error) *types.Type {
	d.PanicIfError(err)
	return t
}

func mustRef(ref types.Ref, err error) types.Ref {
	d.PanicIfError(err)
	return ref
}

func mustValue(val types.Value, err error) types.Value {
	d.PanicIfError(err)
	return val
}

func mustTuple(val types.Tuple, err error) types.Tuple {
	d.PanicIfError(err)
	return val
}

func TestNewCommit(t *testing.T) {
	assert := assert.New(t)

	assertTypeEquals := func(e, a *types.Type) {
		t.Helper()
		assert.True(a.Equals(e), "Actual: %s\nExpected %s", mustString(a.Describe(context.Background())), mustString(e.Describe(context.Background())))
	}

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	parents := mustList(types.NewList(context.Background(), db))
	parentsSkipList := mustTuple(getParentsSkipList(context.Background(), db, parents))
	commit, err := newCommit(context.Background(), types.Float(1), parents, parentsSkipList, types.EmptyStruct(types.Format_7_18))
	assert.NoError(err)
	at, err := types.TypeOf(commit)
	assert.NoError(err)
	et, err := makeCommitStructType(
		types.EmptyStructType,
		mustType(types.MakeSetType(mustType(types.MakeUnionType()))),
		mustType(types.MakeListType(mustType(types.MakeUnionType()))),
		mustType(types.TypeOf(types.EmptyTuple(types.Format_7_18))),
		types.PrimitiveTypeMap[types.FloatKind],
	)
	assert.NoError(err)
	assertTypeEquals(et, at)

	_, err = db.WriteValue(context.Background(), commit)
	assert.NoError(err)

	// Committing another Float
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit, types.Format_7_18))))
	parentsSkipList = mustTuple(getParentsSkipList(context.Background(), db, parents))
	commit2, err := newCommit(context.Background(), types.Float(2), parents, parentsSkipList, types.EmptyStruct(types.Format_7_18))
	assert.NoError(err)
	at2, err := types.TypeOf(commit2)
	assert.NoError(err)
	et2 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_list: List<Ref<Cycle<Commit>>>,
                parents_skip_list: Tuple,
                value: Float,
        }`)
	assertTypeEquals(et2, at2)

	_, err = db.WriteValue(context.Background(), commit2)
	assert.NoError(err)

	// Now commit a String
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit2, types.Format_7_18))))
	parentsSkipList = mustTuple(getParentsSkipList(context.Background(), db, parents))
	commit3, err := newCommit(context.Background(), types.String("Hi"), parents, parentsSkipList, types.EmptyStruct(types.Format_7_18))
	assert.NoError(err)
	at3, err := types.TypeOf(commit3)
	assert.NoError(err)
	et3 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_list: List<Ref<Cycle<Commit>>>,
                parents_skip_list: Tuple,
                value: Float | String,
        }`)
	assertTypeEquals(et3, at3)

	// Now commit a String with MetaInfo
	meta, err := types.NewStruct(types.Format_7_18, "Meta", types.StructData{"date": types.String("some date"), "number": types.Float(9)})
	assert.NoError(err)
	metaType := nomdl.MustParseType(`Struct Meta {
                date: String,
                number: Float,
	}`)
	assertTypeEquals(metaType, mustType(types.TypeOf(meta)))
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit2, types.Format_7_18))))
	parentsSkipList = mustTuple(getParentsSkipList(context.Background(), db, parents))
	commit4, err := newCommit(context.Background(), types.String("Hi"), parents, parentsSkipList, meta)
	assert.NoError(err)
	at4, err := types.TypeOf(commit4)
	assert.NoError(err)
	et4 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {} | Struct Meta {
                        date: String,
                        number: Float,
        	},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_list: List<Ref<Cycle<Commit>>>,
                parents_skip_list: Tuple,
                value: Float | String,
        }`)
	assertTypeEquals(et4, at4)

	_, err = db.WriteValue(context.Background(), commit3)
	assert.NoError(err)

	// Merge-commit with different parent types
	parents = mustList(types.NewList(context.Background(), db,
		mustRef(types.NewRef(commit2, types.Format_7_18)),
		mustRef(types.NewRef(commit3, types.Format_7_18))))
	parentsSkipList = mustTuple(getParentsSkipList(context.Background(), db, parents))
	commit5, err := newCommit(
		context.Background(),
		types.String("Hi"),
		parents,
		parentsSkipList,
		types.EmptyStruct(types.Format_7_18))
	assert.NoError(err)
	at5, err := types.TypeOf(commit5)
	assert.NoError(err)
	et5 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_list: List<Ref<Cycle<Commit>>>,
                parents_skip_list: Tuple,
                value: Float | String,
        }`)
	assertTypeEquals(et5, at5)
}

func TestCommitWithoutMetaField(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	metaCommit, err := types.NewStruct(types.Format_7_18, "Commit", types.StructData{
		"value":   types.Float(9),
		"parents": mustSet(types.NewSet(context.Background(), db)),
		"meta":    types.EmptyStruct(types.Format_7_18),
	})
	assert.NoError(err)
	assert.True(IsCommit(metaCommit))

	noMetaCommit, err := types.NewStruct(types.Format_7_18, "Commit", types.StructData{
		"value":   types.Float(9),
		"parents": mustSet(types.NewSet(context.Background(), db)),
	})
	assert.NoError(err)
	assert.False(IsCommit(noMetaCommit))
}

// Convert list of Struct's to List<Ref>
func toRefList(vrw types.ValueReadWriter, commits ...types.Struct) (types.List, error) {
	l, err := types.NewList(context.Background(), vrw)
	if err != nil {
		return types.EmptyList, err
	}

	le := l.Edit()
	for _, p := range commits {
		le = le.Append(mustRef(types.NewRef(p, types.Format_7_18)))
	}
	return le.List(context.Background())
}

func commonAncWithSetClosure(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (a types.Ref, ok bool, err error) {
	closure, err := NewSetRefClosure(ctx, vr1, c1)
	if err != nil {
		return types.Ref{}, false, err
	}
	return FindClosureCommonAncestor(ctx, closure, c2, vr2)
}

func commonAncWithLazyClosure(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (a types.Ref, ok bool, err error) {
	closure := NewLazyRefClosure(c1, vr1)
	return FindClosureCommonAncestor(ctx, closure, c2, vr2)
}

// Assert that c is the common ancestor of a and b, using multiple common ancestor methods.
func assertCommonAncestor(t *testing.T, expected, a, b types.Struct, ldb, rdb Database) {
	assert := assert.New(t)

	type caFinder func(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (a types.Ref, ok bool, err error)

	methods := map[string]caFinder{
		"FindCommonAncestor": FindCommonAncestor,
		"SetClosure":         commonAncWithSetClosure,
		"LazyClosure":        commonAncWithLazyClosure,
	}

	for name, method := range methods {
		tn := fmt.Sprintf("find common ancestor using %s", name)
		t.Run(tn, func(t *testing.T) {
			found, ok, err := method(context.Background(), mustRef(types.NewRef(a, types.Format_7_18)), mustRef(types.NewRef(b, types.Format_7_18)), ldb, rdb)
			assert.NoError(err)

			if assert.True(ok) {
				tv, err := found.TargetValue(context.Background(), ldb)
				assert.NoError(err)
				ancestor := tv.(types.Struct)
				expV, _, _ := expected.MaybeGet(ValueField)
				aV, _, _ := a.MaybeGet(ValueField)
				bV, _, _ := b.MaybeGet(ValueField)
				ancV, _, _ := ancestor.MaybeGet(ValueField)
				assert.True(
					expected.Equals(ancestor),
					"%s should be common ancestor of %s, %s. Got %s",
					expV,
					aV,
					bV,
					ancV,
				)
			}
		})
	}
}

// Add a commit and return it
func addCommit(t *testing.T, db Database, datasetID string, val string, parents ...types.Struct) types.Struct {
	ds, err := db.GetDataset(context.Background(), datasetID)
	assert.NoError(t, err)
	ds, err = db.Commit(context.Background(), ds, types.String(val), CommitOptions{ParentsList: mustList(toRefList(db, parents...))})
	assert.NoError(t, err)
	return mustHead(ds)
}

func TestFindCommonAncestor(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())

	// Build commit DAG
	//
	// ds-a: a1<-a2<-a3<-a4<-a5<-a6
	//       ^    ^   ^          |
	//       |     \   \----\  /-/
	//       |      \        \V
	// ds-b:  \      b3<-b4<-b5
	//         \
	//          \
	// ds-c:     c2<-c3
	//              /
	//             /
	//            V
	// ds-d: d1<-d2
	//
	a, b, c, d := "ds-a", "ds-b", "ds-c", "ds-d"
	a1 := addCommit(t, db, a, "a1")
	d1 := addCommit(t, db, d, "d1")
	a2 := addCommit(t, db, a, "a2", a1)
	c2 := addCommit(t, db, c, "c2", a1)
	d2 := addCommit(t, db, d, "d2", d1)
	a3 := addCommit(t, db, a, "a3", a2)
	b3 := addCommit(t, db, b, "b3", a2)
	c3 := addCommit(t, db, c, "c3", c2, d2)
	a4 := addCommit(t, db, a, "a4", a3)
	b4 := addCommit(t, db, b, "b4", b3)
	a5 := addCommit(t, db, a, "a5", a4)
	b5 := addCommit(t, db, b, "b5", b4, a3)
	a6 := addCommit(t, db, a, "a6", a5, b5)

	assertCommonAncestor(t, a1, a1, a1, db, db) // All self
	assertCommonAncestor(t, a1, a1, a2, db, db) // One side self
	assertCommonAncestor(t, a2, a3, b3, db, db) // Common parent
	assertCommonAncestor(t, a2, a4, b4, db, db) // Common grandparent
	assertCommonAncestor(t, a1, a6, c3, db, db) // Traversing multiple parents on both sides

	// No common ancestor
	found, ok, err := FindCommonAncestor(context.Background(), mustRef(types.NewRef(d2, types.Format_7_18)), mustRef(types.NewRef(a6, types.Format_7_18)), db, db)
	assert.NoError(err)

	if !assert.False(ok) {
		d2V, _, _ := d2.MaybeGet(ValueField)
		a6V, _, _ := a6.MaybeGet(ValueField)
		fTV, _ := found.TargetValue(context.Background(), db)
		fV, _, _ := fTV.(types.Struct).MaybeGet(ValueField)

		assert.Fail(
			"Unexpected common ancestor!",
			"Should be no common ancestor of %s, %s. Got %s",
			d2V,
			a6V,
			fV,
		)
	}

	assert.NoError(db.Close())

	storage = &chunks.TestStorage{}
	db = NewDatabase(storage.NewView())
	defer db.Close()

	rstorage := &chunks.TestStorage{}
	rdb := NewDatabase(rstorage.NewView())
	defer rdb.Close()

	// Rerun the tests when using two difference Databases for left and
	// right commits. Both databases have all the previous commits.
	a, b, c, d = "ds-a", "ds-b", "ds-c", "ds-d"
	a1 = addCommit(t, db, a, "a1")
	d1 = addCommit(t, db, d, "d1")
	a2 = addCommit(t, db, a, "a2", a1)
	c2 = addCommit(t, db, c, "c2", a1)
	d2 = addCommit(t, db, d, "d2", d1)
	a3 = addCommit(t, db, a, "a3", a2)
	b3 = addCommit(t, db, b, "b3", a2)
	c3 = addCommit(t, db, c, "c3", c2, d2)
	a4 = addCommit(t, db, a, "a4", a3)
	b4 = addCommit(t, db, b, "b4", b3)
	a5 = addCommit(t, db, a, "a5", a4)
	b5 = addCommit(t, db, b, "b5", b4, a3)
	a6 = addCommit(t, db, a, "a6", a5, b5)

	addCommit(t, rdb, a, "a1")
	addCommit(t, rdb, d, "d1")
	addCommit(t, rdb, a, "a2", a1)
	addCommit(t, rdb, c, "c2", a1)
	addCommit(t, rdb, d, "d2", d1)
	addCommit(t, rdb, a, "a3", a2)
	addCommit(t, rdb, b, "b3", a2)
	addCommit(t, rdb, c, "c3", c2, d2)
	addCommit(t, rdb, a, "a4", a3)
	addCommit(t, rdb, b, "b4", b3)
	addCommit(t, rdb, a, "a5", a4)
	addCommit(t, rdb, b, "b5", b4, a3)
	addCommit(t, rdb, a, "a6", a5, b5)

	// Additionally, |db| has a6<-a7<-a8<-a9.
	// |rdb| has a6<-ra7<-ra8<-ra9.
	a7 := addCommit(t, db, a, "a7", a6)
	a8 := addCommit(t, db, a, "a8", a7)
	a9 := addCommit(t, db, a, "a9", a8)

	ra7 := addCommit(t, rdb, a, "ra7", a6)
	ra8 := addCommit(t, rdb, a, "ra8", ra7)
	ra9 := addCommit(t, rdb, a, "ra9", ra8)

	assertCommonAncestor(t, a1, a1, a1, db, rdb) // All self
	assertCommonAncestor(t, a1, a1, a2, db, rdb) // One side self
	assertCommonAncestor(t, a2, a3, b3, db, rdb) // Common parent
	assertCommonAncestor(t, a2, a4, b4, db, rdb) // Common grandparent
	assertCommonAncestor(t, a1, a6, c3, db, rdb) // Traversing multiple parents on both sides

	assertCommonAncestor(t, a6, a9, ra9, db, rdb) // Common third parent

	_, _, err = FindCommonAncestor(context.Background(), mustRef(types.NewRef(a9, types.Format_7_18)), mustRef(types.NewRef(ra9, types.Format_7_18)), rdb, db)
	assert.Error(err)
}

func TestNewCommitRegressionTest(t *testing.T) {
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	parents := mustList(types.NewList(context.Background(), db))
	parentsSkipList := mustTuple(getParentsSkipList(context.Background(), db, parents))
	c1, err := newCommit(context.Background(), types.String("one"), parents, parentsSkipList, types.EmptyStruct(types.Format_7_18))
	assert.NoError(t, err)
	cx, err := newCommit(context.Background(), types.Bool(true), parents, parentsSkipList, types.EmptyStruct(types.Format_7_18))
	assert.NoError(t, err)
	value := types.String("two")
	_, err = db.WriteValue(context.Background(), c1)
	assert.NoError(t, err)
	parents, err = types.NewList(context.Background(), db, mustRef(types.NewRef(c1, types.Format_7_18)))
	assert.NoError(t, err)
	parentsSkipList = mustTuple(getParentsSkipList(context.Background(), db, parents))
	meta, err := types.NewStruct(types.Format_7_18, "", types.StructData{
		"basis": cx,
	})
	assert.NoError(t, err)

	// Used to fail
	_, err = newCommit(context.Background(), value, parents, parentsSkipList, meta)
	assert.NoError(t, err)
}

func TestPersistedCommitConsts(t *testing.T) {
	// changing constants that are persisted requires a migration strategy
	assert.Equal(t, "parents", ParentsField)
	assert.Equal(t, "parents_list", ParentsListField)
	assert.Equal(t, "value", ValueField)
	assert.Equal(t, "meta", CommitMetaField)
	assert.Equal(t, "Commit", CommitName)
}

func TestCommitParentsSkipList(t *testing.T) {
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	height := 1
	oldHeightFunc := SkipListNodeHeightForParents
	defer func() {
		SkipListNodeHeightForParents = oldHeightFunc
	}()
	SkipListNodeHeightForParents = func(ctx context.Context, parents types.List) (int, error) {
		return height, nil
	}

	type node []*types.Struct
	type expected []node

	assertExpected := func(e expected) func(got ParentsSkipList, found bool, err error) {
		return func(got ParentsSkipList, found bool, err error) {
			require.NoError(t, err)
			require.True(t, found)
			if !assert.Equal(t, uint64(len(e)), got.Len()) {
				return
			}
			for i := uint64(0); i < got.Len(); i++ {
				gotn := e[int(i)]
				n, err := got.Get(i)
				require.NoError(t, err)
				if !assert.Equal(t, uint64(len(gotn)), n.Height()) {
					return
				}
				for j := 0; j < len(gotn); j++ {
					r, found, err := n.GetRef(uint64(j))
					require.NoError(t, err)
					if gotn[j] != nil {
						require.True(t, found)
						assert.Equal(t, mustRef(types.NewRef(*gotn[j], types.Format_7_18)).TargetHash(), r.TargetHash())
					} else {
						assert.False(t, found)
					}
				}
			}
		}
	}

	a, b, c, d := "ds-a", "ds-b", "ds-c", "ds-d"
	fmt.Printf("%s %s %s %s\n", a, b, c, d)
	a1 := addCommit(t, db, a, "a1")
	assertExpected(expected{})(LoadParentsSkipListFromCommit(a1))

	a2 := addCommit(t, db, a, "a2", a1)
	assertExpected(expected{
		node{&a1},
	})(LoadParentsSkipListFromCommit(a2))

	height = 2
	a3 := addCommit(t, db, a, "a3", a2)
	assertExpected(expected{
		node{&a2, nil},
	})(LoadParentsSkipListFromCommit(a3))

	height = 1
	a4 := addCommit(t, db, a, "a4", a3)
	assertExpected(expected{
		node{&a3},
	})(LoadParentsSkipListFromCommit(a4))

	height = 4
	a5 := addCommit(t, db, a, "a5", a4)
	assertExpected(expected{
		node{&a4, &a3, nil, nil},
	})(LoadParentsSkipListFromCommit(a5))

	height = 2
	a6 := addCommit(t, db, a, "a6", a5)
	assertExpected(expected{
		node{&a5, &a5},
	})(LoadParentsSkipListFromCommit(a6))

	height = 1
	a7 := addCommit(t, db, a, "a7", a6)
	assertExpected(expected{
		node{&a6},
	})(LoadParentsSkipListFromCommit(a7))

	height = 1
	a8 := addCommit(t, db, a, "a8", a7)
	assertExpected(expected{
		node{&a7},
	})(LoadParentsSkipListFromCommit(a8))

	height = 4
	a9 := addCommit(t, db, a, "a9", a8)
	assertExpected(expected{
		node{&a8, &a6, &a5, &a5},
	})(LoadParentsSkipListFromCommit(a9))
}
