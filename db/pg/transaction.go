package pg

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	mpayutils "github.com/umahanov/mpayutils/db/sql"
)

type postgresTransaction struct {
	mpayutils.Queryable
	mpayutils.SquirrelQueryable
	tx   *sqlx.Tx //nolint
	node *postgresNode
}

func newTransactionFromTx(tx *sqlx.Tx, node *postgresNode) *postgresTransaction {
	queryable := mpayutils.New(tx)
	return &postgresTransaction{
		Queryable: queryable,
		SquirrelQueryable: &SquirrelQueryable{
			Queryable: queryable,
			Node:      node.db,
		},
		tx:   tx,
		node: node,
	}
}

func (p *postgresTransaction) GetDB(ctx context.Context) (*sql.DB, error) {
	return p.node.GetDB(ctx)
}
