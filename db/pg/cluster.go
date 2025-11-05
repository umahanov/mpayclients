package pg

import (
	"context"
	"database/sql"

	sqlx "github.com/umahanov/mpayutils/db/sql"
	"github.com/umahanov/mpayutils/db/sql/wrappers"

	"golang.yandex/hasql/v2"
)

type PostgresCluster struct {
	cluster hasqlCluster
}

func NewCluster(config PgConfig) sqlx.Cluster {
	return &PostgresCluster{
		cluster: newHasqlCluster(config),
	}
}

func (c *PostgresCluster) Hasql() *hasql.Cluster[*sql.DB] {
	return c.cluster.Hasql()
}

func (c *PostgresCluster) GetEagerDatabase(ctx context.Context, dbType hasql.NodeStateCriterion) (sqlx.Database, error) {
	node, err := c.cluster.getNode(ctx, dbType)
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
	return c.cluster.Connect(ctx)
}

func (c *PostgresCluster) Disconnect(ctx context.Context) error {
	return c.cluster.Disconnect(ctx)
}

func (c *PostgresCluster) Name() string {
	return c.cluster.Name()
}
