package diff_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	cmd "github.com/dolthub/dolt/go/cmd/dolt/commands"
	ddiff "github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/store/diff"
)

type testCommand struct {
	cmd  cli.Command
	args args
}

func (tc testCommand) exec(t *testing.T, ctx context.Context, dEnv *env.DoltEnv) {
	exitCode := tc.cmd.Exec(ctx, tc.cmd.Name(), tc.args, dEnv)
	require.Equal(t, 0, exitCode)
}

type args []string

func TestAsyncDifferKeyless(t *testing.T) {

	setupCommon := []testCommand{
		{cmd.SqlCmd{}, args{"-q", "CREATE TABLE keyless (c0 int, c1 int);"}},
		{cmd.AddCmd{}, args{"."}},
		{cmd.CommitCmd{}, args{"-am", "setup"}},
	}

	tests := []struct {
		name     string
		setup    []testCommand
		expected []*diff.Difference
	}{
		{
			name: "smoke test",
			setup: []testCommand{
				{cmd.CommitCmd{}, args{"--allow-empty", "-m", "empty commit"}},
			},
			expected: []*diff.Difference{},
		},
		//{
		//	name: "ld test replication",
		//	setup: []testCommand{
		//		{cmd.SqlCmd{}, args{"-q", "INSERT INTO test VALUES (1,1),(2,2);"}},
		//		{cmd.CommitCmd{}, args{"-am", "made changes"}},
		//	},
		//	expected: []*diff.Difference{},
		//},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtu.CreateTestEnv()

			for _, tc := range setupCommon {
				tc.exec(t, ctx, dEnv)
			}
			for _, tc := range test.setup {
				tc.exec(t, ctx, dEnv)
			}

			head, parent := headAndParent(t, ctx, dEnv)

			batchSizes := []int{1, 2, 10}
			for _, bs := range batchSizes {
				name := fmt.Sprintf("%s batch size %d", test.name, bs)
				t.Run(name, func(t *testing.T) {

					differ := rowDifferForRoots(t, ctx, head, parent)

					var actual []*diff.Difference
					ok := true
					for ok {
						var d []*diff.Difference
						var err error
						d, ok, err = differ.GetDiffs(1, -1)
						require.NoError(t, err)
						actual = append(actual, d...)
					}

					require.Equal(t, len(test.expected), len(actual))
					for i := range test.expected {
						assert.Equal(t, test.expected[i], actual[i])
					}
				})
			}
		})
	}
}

func headAndParent(t *testing.T, ctx context.Context, dEnv *env.DoltEnv) (head, parent *doltdb.RootValue) {
	ddb := dEnv.DoltDB

	r := dEnv.RepoStateReader().CWBHeadRef()
	h, err := ddb.ResolveCommitRef(ctx, r)
	require.NoError(t, err)

	head, err = h.GetRootValue()
	require.NoError(t, err)

	pc, err := ddb.ResolveParent(ctx, h, 0)
	require.NoError(t, err)

	parent, err = pc.GetRootValue()
	require.NoError(t, err)
	return
}

func rowDifferForRoots(t *testing.T, ctx context.Context, head, parent *doltdb.RootValue) ddiff.RowDiffer {
	toTable, _, err := head.GetTable(ctx, "keyless")
	require.NoError(t, err)
	toSchema, err := toTable.GetSchema(ctx)
	require.NoError(t, err)

	fromTable, _, err := parent.GetTable(ctx, "keyless")
	require.NoError(t, err)
	fromSchema, err := fromTable.GetSchema(ctx)
	require.NoError(t, err)

	toMap, err := toTable.GetRowData(ctx)
	require.NoError(t, err)
	fromMap, err := fromTable.GetRowData(ctx)
	require.NoError(t, err)

	rd := ddiff.NewRowDiffer(ctx, fromSchema, toSchema, 10)
	rd.Start(ctx, fromMap, toMap)
	return rd
}
