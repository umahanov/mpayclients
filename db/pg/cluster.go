package pg

import (
	"context"
	"database/sql"

	sqlx "github.com/umahanov/mpayutils/db/sql"
	"github.com/umahanov/mpayutils/db/sql/wrappers"

	"golang.yandex/hasql/v2"
)

type PostgresCluster struct {
	backend Backend
}

func NewCluster(config BackendConfig) sqlx.Cluster {
	return &PostgresCluster{
		backend: NewBackend(config),
	}
}

func (c *PostgresCluster) Hasql() *hasql.Cluster[*sql.DB] {
	return c.backend.Hasql()
}

func (c *PostgresCluster) GetDatabaseReal(ctx context.Context, dbType hasql.NodeStateCriterion) (sqlx.Database, error) {
	node, err := c.backend.getNode(ctx, dbType)
	if err != nil {
		return nil, err
	}
	return newNodeFromHasql(*node), nil
}

func (c *PostgresCluster) GetDatabase(dbType hasql.NodeStateCriterion) sqlx.Database {
	return &wrappers.DatabaseWrap{
		Cluster:      c,
		SelectedNode: dbType,
	}
}

func (c *PostgresCluster) Connect(ctx context.Context) (err error) {
	return c.backend.Connect(ctx)
}

func (c *PostgresCluster) Disconnect(ctx context.Context) error {
	return c.backend.Disconnect(ctx)
}

func (c *PostgresCluster) Name() string {
	return c.backend.Name()
}
