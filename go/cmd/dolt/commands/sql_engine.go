package commands

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/tracing"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"gopkg.in/src-d/go-errors.v1"
	"io"
	"os"
	"runtime"
	"strings"
)

// SqlEngine is an abstraction used by the CLI that writes the results of sql queries to disk.
type SqlEngine struct {
	dbs            map[string]dsqle.SqlDatabase
	sess           *dsess.DoltSession
	contextFactory func(ctx context.Context) (*sql.Context, error)
	engine         *sqle.Engine
	resultFormat   resultFormat
}

var ErrDBNotFoundKind = errors.NewKind("database '%s' not found")

// SqlEngine packages up the context necessary to run sql queries against sqle.
func NewSqlEngine(
	ctx context.Context,
	config config.ReadWriteConfig,
	fs filesys.Filesys,
	format resultFormat,
	initialDb string,
	dbs ...dsqle.SqlDatabase,
) (*SqlEngine, error) {
	au := new(auth.None)

	parallelism := runtime.GOMAXPROCS(0)

	infoDB := information_schema.NewInformationSchemaDatabase()
	all := append(dsqleDBsAsSqlDBs(dbs), infoDB)

	pro := dsqle.NewDoltDatabaseProvider(config, fs, all...)

	engine := sqle.New(analyzer.NewBuilder(pro).WithParallelism(parallelism).Build(), &sqle.Config{Auth: au})

	if dbg, ok := os.LookupEnv("DOLT_SQL_DEBUG_LOG"); ok && strings.ToLower(dbg) == "true" {
		engine.Analyzer.Debug = true
		if verbose, ok := os.LookupEnv("DOLT_SQL_DEBUG_LOG_VERBOSE"); ok && strings.ToLower(verbose) == "true" {
			engine.Analyzer.Verbose = true
		}
	}

	nameToDB := make(map[string]dsqle.SqlDatabase)
	var dbStates []dsess.InitialDbState
	for _, db := range dbs {
		nameToDB[db.Name()] = db

		dbState, err := dsqle.GetInitialDBState(ctx, db)
		if err != nil {
			return nil, err
		}

		dbStates = append(dbStates, dbState)
	}

	// TODO: not having user and email for this command should probably be an error or warning, it disables certain functionality
	sess, err := dsess.NewDoltSession(sql.NewEmptyContext(), sql.NewBaseSession(), pro, config, dbStates...)
	if err != nil {
		return nil, err
	}

	// this is overwritten only for server sessions
	for _, db := range dbs {
		db.DbData().Ddb.SetCommitHookLogger(ctx, cli.CliOut)
	}

	// TODO: this should just be the session default like it is with MySQL
	err = sess.SetSessionVariable(sql.NewContext(ctx), sql.AutoCommitSessionVar, true)
	if err != nil {
		return nil, err
	}

	return &SqlEngine{
		dbs:            nameToDB,
		sess:           sess,
		contextFactory: newSqlContext(sess, initialDb),
		engine:         engine,
		resultFormat:   format,
	}, nil
}

func newSqlContext(sess *dsess.DoltSession, initialDb string) func(ctx context.Context) (*sql.Context, error) {
	return func(ctx context.Context) (*sql.Context, error) {
		sqlCtx := sql.NewContext(ctx,
			sql.WithSession(sess),
			sql.WithTracer(tracing.Tracer(ctx)))

		// If the session was already updated with a database then continue using it in the new session. Otherwise
		// use the initial one.
		if sessionDB := sess.GetCurrentDatabase(); sessionDB != "" {
			sqlCtx.SetCurrentDatabase(sessionDB)
		} else {
			sqlCtx.SetCurrentDatabase(initialDb)
		}

		return sqlCtx, nil
	}
}

func dsqleDBsAsSqlDBs(dbs []dsqle.SqlDatabase) []sql.Database {
	sqlDbs := make([]sql.Database, 0, len(dbs))
	for _, db := range dbs {
		sqlDbs = append(sqlDbs, db)
	}
	return sqlDbs
}

func (se *SqlEngine) iterDBs(cb func(name string, db dsqle.SqlDatabase) (stop bool, err error)) error {
	for name, db := range se.dbs {
		stop, err := cb(name, db)

		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

func (se *SqlEngine) getRoots(sqlCtx *sql.Context) (map[string]*doltdb.RootValue, error) {
	newRoots := make(map[string]*doltdb.RootValue)
	for name, db := range se.dbs {
		var err error
		newRoots[name], err = db.GetRoot(sqlCtx)

		if err != nil {
			return nil, err
		}
	}

	return newRoots, nil
}

func (se *SqlEngine) newContext(ctx context.Context) (*sql.Context, error) {
	return se.contextFactory(ctx)
}

// Execute a SQL statement and return values for printing.
func (se *SqlEngine) query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	return se.engine.Query(ctx, query)
}

func (se *SqlEngine) flushDbs(ctx *sql.Context) error {
	return se.iterDBs(func(_ string, db dsqle.SqlDatabase) (bool, error) {
		_, rowIter, err := se.engine.Query(ctx, "COMMIT;")
		if err != nil {
			return false, err
		}

		err = rowIter.Close(ctx)
		if err != nil {
			return false, err
		}

		err = db.Flush(ctx)
		if err != nil {
			return false, err
		}

		return false, nil
	})
}

func (se *SqlEngine) dbddl(ctx *sql.Context, dbddl *sqlparser.DBDDL, query string) (sql.Schema, sql.RowIter, error) {
	action := strings.ToLower(dbddl.Action)
	var rowIter sql.RowIter = nil
	var err error = nil

	if action != sqlparser.CreateStr && action != sqlparser.DropStr {
		return nil, nil, fmt.Errorf("Unhandled DBDDL action %v in query %v", action, query)
	}

	if action == sqlparser.DropStr {
		// Should not be allowed to delete repo name and information schema
		if dbddl.DBName == information_schema.InformationSchemaDatabaseName {
			return nil, nil, fmt.Errorf("DROP DATABASE isn't supported for database %s", information_schema.InformationSchemaDatabaseName)
		}
	}

	sch, rowIter, err := se.query(ctx, query)

	if rowIter != nil {
		err = rowIter.Close(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if err != nil {
		return nil, nil, err
	}

	return sch, nil, nil
}

// Executes a SQL DDL statement (create, update, etc.). Updates the new root value in
// the SqlEngine if necessary.
func (se *SqlEngine) ddl(ctx *sql.Context, ddl *sqlparser.DDL, query string) (sql.Schema, sql.RowIter, error) {
	switch ddl.Action {
	case sqlparser.CreateStr, sqlparser.DropStr, sqlparser.AlterStr, sqlparser.RenameStr, sqlparser.TruncateStr:
		_, ri, err := se.query(ctx, query)
		if err == nil {
			for _, err = ri.Next(); err == nil; _, err = ri.Next() {
			}
			if err == io.EOF {
				err = ri.Close(ctx)
			} else {
				closeErr := ri.Close(ctx)
				if closeErr != nil {
					err = errhand.BuildDError("error while executing ddl").AddCause(err).AddCause(closeErr).Build()
				}
			}
		}
		return nil, nil, err
	default:
		return nil, nil, fmt.Errorf("Unhandled DDL action %v in query %v", ddl.Action, query)
	}
}
