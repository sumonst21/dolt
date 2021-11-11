package cliengine

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

// CollectDBs takes a MultiRepoEnv and creates Database objects from each environment and returns a slice of these
// objects.
func CollectDBs(ctx context.Context, mrEnv *env.MultiRepoEnv) ([]sqle.SqlDatabase, error) {
	var dbs []sqle.SqlDatabase
	var db sqle.SqlDatabase
	err := mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
		postCommitHooks, err := GetCommitHooks(ctx, dEnv)
		if err != nil {
			return true, err
		}
		dEnv.DoltDB.SetCommitHooks(ctx, postCommitHooks)

		db = newDatabase(name, dEnv)

		if _, remote, ok := sql.SystemVariables.GetGlobal(sqle.ReadReplicaRemoteKey); ok && remote != "" {
			remoteName, ok := remote.(string)
			if !ok {
				return true, sql.ErrInvalidSystemVariableValue.New(remote)
			}
			db, err = newReplicaDatabase(ctx, name, remoteName, dEnv)
			if err != nil {
				return true, err
			}
		}

		dbs = append(dbs, db)
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return dbs, nil
}

func getPushOnWriteHook(ctx context.Context, dEnv *env.DoltEnv) (*doltdb.PushOnWriteHook, error) {
	_, val, ok := sql.SystemVariables.GetGlobal(sqle.ReplicateToRemoteKey)
	if !ok {
		return nil, sql.ErrUnknownSystemVariable.New(sqle.SkipReplicationErrorsKey)
	} else if val == "" {
		return nil, nil
	}

	remoteName, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidSystemVariableValue.New(val)
	}

	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return nil, err
	}

	rem, ok := remotes[remoteName]
	if !ok {
		return nil, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	ddb, err := rem.GetRemoteDB(ctx, types.Format_Default)
	if err != nil {
		return nil, err
	}

	pushHook := doltdb.NewPushOnWriteHook(ddb, dEnv.TempTableFilesDir())
	return pushHook, nil
}

// GetCommitHooks creates a list of hooks to execute on database commit. If doltdb.SkipReplicationErrorsKey is set,
// replace misconfigured hooks with doltdb.LogHook instances that prints a warning when trying to execute.
func GetCommitHooks(ctx context.Context, dEnv *env.DoltEnv) ([]datas.CommitHook, error) {
	postCommitHooks := make([]datas.CommitHook, 0)
	var skipErrors bool
	if _, val, ok := sql.SystemVariables.GetGlobal(sqle.SkipReplicationErrorsKey); !ok {
		return nil, sql.ErrUnknownSystemVariable.New(sqle.SkipReplicationErrorsKey)
	} else if val == int8(1) {
		skipErrors = true
	}

	if hook, err := getPushOnWriteHook(ctx, dEnv); err != nil {
		err = fmt.Errorf("failure loading hook; %w", err)
		if skipErrors {
			postCommitHooks = append(postCommitHooks, doltdb.NewLogHook([]byte(err.Error()+"\n")))
		} else {
			return nil, err
		}
	} else if hook != nil {
		postCommitHooks = append(postCommitHooks, hook)
	}

	return postCommitHooks, nil
}

func newDatabase(name string, dEnv *env.DoltEnv) sqle.Database {
	opts := editor.Options{
		Deaf: dEnv.DbEaFactory(),
	}
	return sqle.NewDatabase(name, dEnv.DbData(), opts)
}

// newReplicaDatabase creates a new dsqle.ReadReplicaDatabase. If the doltdb.SkipReplicationErrorsKey global variable is set,
// skip errors related to database construction only and return a partially functional dsqle.ReadReplicaDatabase
// that will log warnings when attempting to perform replica commands.
func newReplicaDatabase(ctx context.Context, name string, remoteName string, dEnv *env.DoltEnv) (sqle.ReadReplicaDatabase, error) {
	var skipErrors bool
	if _, val, ok := sql.SystemVariables.GetGlobal(sqle.SkipReplicationErrorsKey); !ok {
		return sqle.ReadReplicaDatabase{}, sql.ErrUnknownSystemVariable.New(sqle.SkipReplicationErrorsKey)
	} else if val == int8(1) {
		skipErrors = true
	}

	opts := editor.Options{
		Deaf: dEnv.DbEaFactory(),
	}

	db := sqle.NewDatabase(name, dEnv.DbData(), opts)

	rrd, err := sqle.NewReadReplicaDatabase(ctx, db, remoteName, dEnv.RepoStateReader(), dEnv.TempTableFilesDir())
	if err != nil {
		err = fmt.Errorf("%w from remote '%s'; %s", sqle.ErrFailedToLoadReplicaDB, remoteName, err.Error())
		if !skipErrors {
			return sqle.ReadReplicaDatabase{}, err
		}
		//cli.Println(err)
		return sqle.ReadReplicaDatabase{Database: db}, nil
	}
	return rrd, nil
}