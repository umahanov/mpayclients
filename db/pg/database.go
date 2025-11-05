package pg

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
	mpayutilssql "github.com/umahanov/mpayutils/db/sql"
	"github.com/umahanov/mpayutils/log"
	"go.uber.org/zap"
	"golang.yandex/hasql/v2"
)

type postgresNode struct {
	mpayutilssql.Queryable
	mpayutilssql.SquirrelQueryable
	db *sqlx.DB
}

func newNodeFromHasql(node hasql.Node[*sql.DB]) *postgresNode {
	sqlxDB := sqlx.NewDb(node.DB(), "pgx")
	queryable := mpayutilssql.New(sqlxDB)

	return &postgresNode{
		Queryable: queryable,
		SquirrelQueryable: &SquirrelQueryable{
			Queryable: queryable,
			Node:      sqlxDB,
		},
		db: sqlxDB,
	}
}

func (b *postgresNode) Tx(ctx context.Context, executable mpayutilssql.TransactionCallback) error {
	return b.tx(ctx, executable)
}

func (b *postgresNode) tx(ctx context.Context, executable mpayutilssql.TransactionCallback) (err error) {
	_tx, err := b.db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.New("failed to initialize transaction")
	}

	tx := newTransactionFromTx(_tx, b)
	defer func() { _ = _tx.Rollback() }()

	if err = executable(ctx, tx); err != nil {
		log.Error(ctx, "error during evaluation of transaction callback", zap.Error(err))
		return err
	}

	err = _tx.Commit()
	if err != nil {
		log.Error(ctx, "error during transaction commit", zap.Error(err))
	}
	return err
}

func (b *postgresNode) ReadonlyTx(ctx context.Context, executable mpayutilssql.TransactionCallback) error {
	return b.readonlyTx(ctx, executable)
}

func (b *postgresNode) readonlyTx(ctx context.Context, executable mpayutilssql.TransactionCallback) (err error) {
	_tx, err := b.db.BeginTxx(ctx, nil)
	if err != nil {
		log.Error(ctx, "failed to initialize transaction", zap.Error(err))
		return err
	}

	tx := newTransactionFromTx(_tx, b)
	defer func() {
		_ = _tx.Rollback()
	}()

	err = executable(ctx, tx)
	if err != nil {
		log.Error(ctx, "error during evaluation of transaction callback", zap.Error(err))
	}

	return err
}

func (b *postgresNode) GetDB(context.Context) (*sql.DB, error) {
	return b.db.DB, nil
}
