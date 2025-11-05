package wrappers

import (
	"context"
	"database/sql"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	vksql "github.com/umahanov/mpayutils/db/sql"
	"golang.yandex/hasql/v2"
)

type DatabaseWrap struct {
	Cluster      vksql.Cluster
	SelectedNode hasql.NodeStateCriterion
}

func (d DatabaseWrap) QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error) {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return nil, err
	}
	return node.QueryxContext(ctx, query, args...)
}

func (d DatabaseWrap) QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return nil
	}
	return node.QueryRowxContext(ctx, query, args...)
}

func (d DatabaseWrap) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return err
	}
	return node.SelectContext(ctx, dest, query, args...)
}

func (d DatabaseWrap) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return err
	}
	return node.GetContext(ctx, dest, query, args...)
}

func (d DatabaseWrap) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return nil, err
	}
	return node.ExecContext(ctx, query, args...)
}

func (d DatabaseWrap) QuerySq(ctx context.Context, query squirrel.Sqlizer) (*sqlx.Rows, error) {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return nil, err
	}
	return node.QuerySq(ctx, query)
}

func (d DatabaseWrap) QueryRowSq(ctx context.Context, query squirrel.Sqlizer) (*sqlx.Row, error) {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return nil, err
	}
	return node.QueryRowSq(ctx, query)
}

func (d DatabaseWrap) SelectSq(ctx context.Context, dest any, query squirrel.Sqlizer) error {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return err
	}
	return node.SelectSq(ctx, dest, query)
}

func (d DatabaseWrap) GetSq(ctx context.Context, dest any, query squirrel.Sqlizer) error {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return err
	}
	return node.GetSq(ctx, dest, query)
}

func (d DatabaseWrap) ExecSq(ctx context.Context, query squirrel.Sqlizer) (sql.Result, error) {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return nil, err
	}
	return node.ExecSq(ctx, query)
}

func (d DatabaseWrap) Tx(ctx context.Context, callback vksql.TransactionCallback) error {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return err
	}
	return node.Tx(ctx, callback)
}

func (d DatabaseWrap) ReadonlyTx(ctx context.Context, callback vksql.TransactionCallback) error {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return err
	}
	return node.ReadonlyTx(ctx, callback)
}

func (d DatabaseWrap) GetDB(ctx context.Context) (*sql.DB, error) {
	node, err := d.Cluster.GetEagerDatabase(ctx, d.SelectedNode)
	if err != nil {
		return nil, err
	}
	return node.GetDB(ctx)
}
