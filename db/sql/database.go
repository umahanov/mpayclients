package sql

import (
	"context"
	"database/sql"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"gitlab.corp.mail.ru/oneui/backend/go-library/db"
	"golang.yandex/hasql/v2"
)

type Queryable interface {
	QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row
	// SelectContext must be used for multiple rows
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	// GetContext must be used for exactly one field in one row
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	// ExecContext must be used if result is not needed
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type SquirrelQueryable interface {
	QuerySq(ctx context.Context, query squirrel.Sqlizer) (*sqlx.Rows, error)
	QueryRowSq(ctx context.Context, query squirrel.Sqlizer) (*sqlx.Row, error)
	SelectSq(ctx context.Context, dest any, query squirrel.Sqlizer) error
	GetSq(ctx context.Context, dest any, query squirrel.Sqlizer) error
	ExecSq(ctx context.Context, query squirrel.Sqlizer) (sql.Result, error)
}

type DBInterface interface {
	Queryable
	SquirrelQueryable
	// GetDB return raw sql.DB pointer instead of own wrappers
	GetDB(ctx context.Context) (*sql.DB, error)
}

type Transaction interface {
	DBInterface
}

type TransactionCallback func(ctx context.Context, transaction Transaction) error

type Database interface {
	DBInterface
	Tx(ctx context.Context, callback TransactionCallback) error
	ReadonlyTx(ctx context.Context, callback TransactionCallback) error
}

type Namer interface {
	Name() string
}

type Connectable interface {
	Namer
	db.Connectable
}

type Cluster interface {
	Connectable
	Hasql() *hasql.Cluster[*sql.DB]
	GetDatabaseReal(ctx context.Context, dbType hasql.NodeStateCriterion) (Database, error)
	GetDatabase(dbType hasql.NodeStateCriterion) Database
}

type Transactional[T any] interface {
	WithTx(tx Transaction) T
}
