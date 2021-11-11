package mvdata

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cliengine"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
	"sync/atomic"
)

type SqlEngineMover struct {
 	se *cliengine.SqlEngine
 	tableSch schema.Schema // TODO: Should be a sql schema
 	db string
 	tableCreate bool // TODO: Get rid of this
 	tableName string
 	sqlCtx *sql.Context

	statsCB noms.StatsCB
	stats   types.AppliedEditStats
	statOps int32
}

var _ table.TableWriteCloser = (*SqlEngineMover)(nil)

func NewSqlEngineMover(ctx context.Context, dEnv *env.DoltEnv, tableSch schema.Schema, tableCreate bool, tableName string, statsCB noms.StatsCB) (*SqlEngineMover, error) {
	mrEnv, err := env.DoltEnvAsMultiEnv(ctx, dEnv)
	if err != nil {
		return nil, err
	}


	// Choose the first DB as the current one. This will be the DB in the working dir if there was one there
	var dbName string
	mrEnv.Iter(func(name string, _ *env.DoltEnv) (stop bool, err error) {
		dbName = name
		return true, nil
	})

	dbs, err := cliengine.CollectDBs(ctx, mrEnv)
	if err != nil {
		return nil, err
	}

	se, err := cliengine.NewSqlEngine(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, cliengine.FormatCsv, dbName, dbs...)
	if err != nil {
		return nil, err
	}

	se.SetBatchMode()

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: Move this to factory
	err = sqlCtx.Session.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}


	sm := &SqlEngineMover{
		se: se,
		tableSch: tableSch,
		db: dbName,
		tableCreate: tableCreate,
		tableName: tableName,
		statsCB: statsCB,
		sqlCtx: sqlCtx,
	}

	err = sm.createTable(sqlCtx)
	if err != nil {
		return nil, err
	}

	return sm, nil
}

func (s *SqlEngineMover) GetSchema() schema.Schema {
	return s.tableSch
}

func (s *SqlEngineMover) WriteRow(ctx context.Context, r row.Row) error {
	sqlCtx := s.sqlCtx
	
	doltSchema, err := sqlutil.FromDoltSchema(s.tableName, s.tableSch)
	if err != nil {
		return err
	}

	dRow, err := sqlutil.DoltRowToSqlRow(r, s.tableSch)
	if err != nil {
		return err
	}

	if s.statsCB != nil && atomic.LoadInt32(&s.statOps) >= 64 * 1024 {
		atomic.StoreInt32(&s.statOps, 0)
		s.statsCB(s.stats)
	}

	err = sqle.CreateShortCircuitInsert(sqlCtx, s.se.GetEngine().Analyzer, s.db, s.tableName, sql.RowsToRowIter(dRow), doltSchema)
	if err != nil {
		return err
	}

	_ = atomic.AddInt32(&s.statOps, 1)
	s.stats.Additions++

	return nil
}

func (s *SqlEngineMover) createTable(sqlCtx *sql.Context) error {
	doltSchema, err := sqlutil.FromDoltSchema(s.tableName, s.tableSch)
	if err != nil {
		return err
	}

	colStmts := make([]string, len(doltSchema))
	var primaryKeyCols []string

	for i, col := range doltSchema {
		stmt := fmt.Sprintf("  `%s` %s", col.Name, strings.ToLower(col.Type.String()))

		if !col.Nullable {
			stmt = fmt.Sprintf("%s NOT NULL", stmt)
		}

		if col.AutoIncrement {
			stmt = fmt.Sprintf("%s AUTO_INCREMENT", stmt)
		}

		// TODO: The columns that are rendered in defaults should be backticked
		if col.Default != nil {
			stmt = fmt.Sprintf("%s DEFAULT %s", stmt, col.Default.String())
		}

		if col.Comment != "" {
			stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, col.Comment)
		}

		if col.PrimaryKey {
			primaryKeyCols = append(primaryKeyCols, col.Name)
		}

		colStmts[i] = stmt
	}

	if len(primaryKeyCols) > 0 {
		primaryKey := fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(quoteIdentifiers(primaryKeyCols), ","))
		colStmts = append(colStmts, primaryKey)
	}

	query := fmt.Sprintf(
		"CREATE TABLE `%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		s.tableName,
		strings.Join(colStmts, ",\n"),
	)

	_, _, err = s.se.Query(sqlCtx, query)
	return err
}

func (s *SqlEngineMover) Close(ctx context.Context) error {
	if s.statsCB != nil {
		s.statsCB(s.stats)
	}

	return nil
}

func (s *SqlEngineMover) Commit(ctx context.Context) error {
	sqlCtx, err := s.se.NewContext(ctx)
	if err != nil {
		return nil
	}

	_, _, err = s.se.Query(sqlCtx, "COMMIT")
	return err
}

func quoteIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf("`%s`", id)
	}
	return quoted
}
