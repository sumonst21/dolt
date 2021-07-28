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

package commands

import (
	"context"
	"io"
	"fmt"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var jankyFetchDocs = cli.CommandDocumentationContent{
	ShortDesc: "Fetch remotes faster, but it is currently janky.",
	LongDesc: `Fetch some remotes. Default remote is "origin".

Acts like "fetch" in many ways. Adds all missing chunks in the remote to the local database. Updates refs/remotes/.../ with new commits.

Currently very adhoc. May fetch more chunks than necessary. Every remote tracking branch is force updated.
`,

	Synopsis: []string{`[{{.LessThan}}remote{{.GreaterThan}}]`},
}

type JankyFetchCmd struct{}

func (cmd JankyFetchCmd) Name() string {
	return "jankyfetch"
}

func (cmd JankyFetchCmd) Description() string {
	return "fetch the chunks of a remote, update remote tracking branches"
}

func (cmd JankyFetchCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TYPE_UNSPECIFIED
}

func (cmd JankyFetchCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, jankyFetchDocs, ap))
}

func (cmd JankyFetchCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

func (cmd JankyFetchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, jankyFetchDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	remotes, _ := dEnv.GetRemotes()

	remotename := "origin"
	if len(apr.Args()) > 0 {
		remotename = apr.Args()[0]
	}
	remote, ok := remotes[remotename]
	if !ok {
		panic("could not find remote " + remotename)
	}

	verr := walkRemote(ctx, dEnv, remotename, remote)

	return HandleVErrAndExitCode(verr, usage)
}

func FetchAll(ctx context.Context, hs hash.HashSet) (hash.HashSet, error) {
	return hs, nil
}

func walkRemote(ctx context.Context, dEnv *env.DoltEnv, remotename string, m env.Remote) errhand.VerboseError {
	refspecs, verr := dEnv.GetRefSpecs(remotename)
	if verr != nil {
		return verr
	}

	mdb, err := m.GetRemoteDBWithoutCaching(ctx, dEnv.DoltDB.Format())
	if err != nil {
		panic(err)
	}
	root, err := mdb.Root(ctx)
	if err != nil {
		panic(err)
	}

	remotebranches, err := mdb.GetBranchesWithHashes(ctx)
	if err != nil {
		panic(err)
	}

	newheads := make(map[string]hash.Hash)
        for _, rs := range refspecs {
		for _, rb := range remotebranches {
			remoteTrackRef := rs.DestRef(rb.Ref)
			newheads[remoteTrackRef.String()] = rb.Hash
		}
	}

	fmt.Fprintf(color.Output, "%v\n", root)
	writer, err := nbs.NewCmpChunkTableWriter("")
	if err != nil {
		panic(err)
	}
	f := func(ctx context.Context, hs hash.HashSet, f func(*chunks.Chunk)) error {
		return mdb.ChunkStore().(datas.NBSCompressedChunkStore).GetManyCompressed(ctx, hs, func(cc nbs.CompressedChunk) {
			err := writer.AddCmpChunk(cc)
			if err != nil {
				panic(err)
			}
			c, err := cc.ToChunk()
			if err != nil {
				panic(err)
			}
			f(&c)
		})
	}
	err = mdb.FetchAllChunks(ctx, root, f, dEnv.DoltDB.ChunkStore().HasMany)
	if err != nil {
		panic(err)
	}
	id, err := writer.Finish()
	if err != nil {
		panic(err)
	}
	if writer.ChunkCount() > 0 {
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			writer.Flush(pw)
		}()
		err = dEnv.DoltDB.ChunkStore().(nbs.TableFileStore).WriteTableFile(ctx, id, int(writer.ChunkCount()), pr, writer.ContentLength(), writer.GetMD5())
		if err != nil {
			panic(err)
		}
		err = dEnv.DoltDB.ChunkStore().(nbs.TableFileStore).AddTableFilesToManifest(ctx, map[string]int{id: int(writer.ChunkCount())})
		if err != nil {
			panic(err)
		}
	}

	newheadshashes := hash.HashSlice{}
	htoi := make(map[hash.Hash]int)
	i := 0
	for _, h := range newheads {
		newheadshashes = append(newheadshashes, h)
		htoi[h] = i
		i += 1
	}
	vs := types.NewValueStore(dEnv.DoltDB.ChunkStore())
	newheadsvalues, err := vs.ReadManyValues(ctx, newheadshashes)
	if err != nil {
		panic(err)
	}
	newheadsrefs := make(map[string]types.Ref)
	for k, h := range newheads {
		newheadsrefs[k], err = types.NewRef(newheadsvalues[htoi[h]], dEnv.DoltDB.Format())
		if err != nil {
			panic(err)
		}
	}

	err = dEnv.DoltDB.DatasDatabase().EditDatasets(ctx, func(ctx context.Context, current types.Map) (types.Map, error) {
		edit := current.Edit()
		for s, r := range newheadsrefs {
			edit = edit.Set(types.String(s), r)
		}
		return edit.Map(ctx)
	})

	if err != nil {
		panic(err)
	}

	return nil
}
