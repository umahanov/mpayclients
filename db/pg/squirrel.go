package pg

import (
	"context"
	"database/sql"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	mpayutils "github.com/umahanov/mpayutils/db/sql"
)

type SquirrelQueryable struct {
	Queryable mpayutils.Queryable
	Node      *sqlx.DB
}

func (q *SquirrelQueryable) QuerySq(ctx context.Context, query squirrel.Sqlizer) (*sqlx.Rows, error) {
	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	return q.Queryable.QueryxContext(ctx, q.Node.Rebind(queryString), args...)
}

func (q *SquirrelQueryable) QueryRowSq(ctx context.Context, query squirrel.Sqlizer) (*sqlx.Row, error) {
	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	return q.Queryable.QueryRowxContext(ctx, q.Node.Rebind(queryString), args...), nil
}

func (q *SquirrelQueryable) SelectSq(ctx context.Context, dest any, query squirrel.Sqlizer) error {
	queryString, args, err := query.ToSql()
	if err != nil {
		return err
	}
	return q.Queryable.SelectContext(ctx, dest, q.Node.Rebind(queryString), args...)
}

func (q *SquirrelQueryable) GetSq(ctx context.Context, dest any, query squirrel.Sqlizer) error {
	queryString, args, err := query.ToSql()
	if err != nil {
		return err
	}
	return q.Queryable.GetContext(ctx, dest, q.Node.Rebind(queryString), args...)
}

func (q *SquirrelQueryable) ExecSq(ctx context.Context, query squirrel.Sqlizer) (sql.Result, error) {
	queryString, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	return q.Queryable.ExecContext(ctx, q.Node.Rebind(queryString), args...)
}
