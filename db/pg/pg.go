package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	sqlx "github.com/umahanov/mpayutils/db/sql"
	"github.com/umahanov/mpayutils/log"
	"go.uber.org/zap"

	"golang.yandex/hasql/v2"
)

type PgHost struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type PgConfig struct {
	DSN             string        `yaml:"dsn"`
	Name            string        `yaml:"name"`
	User            string        `yaml:"user"`
	Password        string        `yaml:"password"`
	Hosts           []PgHost      `yaml:"hosts"`
	ConnectTimeout  time.Duration `yaml:"connectTimeout"`
	SSLMode         string        `yaml:"sslMode"`
	MaxOpenConns    int           `yaml:"maxOpenConns"`
	MaxIdleConns    int           `yaml:"maxIdleConns"`
	MaxConnIdleTime time.Duration `yaml:"maxConnIdleTime"`
	MaxConnLifetime time.Duration `yaml:"maxConnLifetime"`
}

type hasqlCluster struct {
	config  PgConfig
	cluster *hasql.Cluster[*sql.DB]
}

var _ sqlx.Connectable = (*hasqlCluster)(nil)

func (b *hasqlCluster) Name() string {
	return "pg"
}

func newHasqlCluster(config PgConfig) hasqlCluster {
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 3 * time.Second
	}

	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 2
	}

	return hasqlCluster{
		config: config,
	}
}

func (b *hasqlCluster) getNode(ctx context.Context, preferNode hasql.NodeStateCriterion) (*hasql.Node[*sql.DB], error) {
	ctx, cancel := context.WithTimeout(ctx, b.config.ConnectTimeout)
	defer cancel()

	return b.cluster.WaitForNode(ctx, preferNode)
}

func (b *hasqlCluster) Hasql() *hasql.Cluster[*sql.DB] {
	return b.cluster
}

func (b *hasqlCluster) Connect(ctx context.Context) (err error) {
	log.Debug(ctx, "connecting to db")
	if b.config.SSLMode == "" {
		b.config.SSLMode = "verify-full"
		log.Debug(ctx, "set ssl mode to verify-full")
	}

	var connStrings []string
	if b.config.DSN != "" {
		log.Debug(ctx, "added dsn as conn string")
		connStrings = append(connStrings, b.config.DSN)
	} else {
		for _, host := range b.config.Hosts {
			if host.Host == "" {
				return errors.New("host not set")
			}
			if host.Port == 0 {
				return errors.New("port not set")
			}

			connStrings = append(connStrings, fmt.Sprintf(
				`host=%s port=%d dbname=%s user=%s password=%s sslmode=%s`,
				host.Host,
				host.Port,
				b.config.Name,
				b.config.User,
				b.config.Password,
				b.config.SSLMode,
			))
			log.Debugf(ctx, "added connString host=%s port=%d dbname=%s user=%s password=*** sslmode=%s",
				host.Host,
				host.Port,
				b.config.Name,
				b.config.User,
				b.config.SSLMode,
			)
		}
	}

	if len(connStrings) == 0 {
		return errors.New("at least one database host or DSN should be specified")
	}

	nodes := make([]*hasql.Node[*sql.DB], 0, len(connStrings))
	for idx, connString := range connStrings {
		connConfig, err := pgx.ParseConfig(connString)
		if err != nil {
			return err
		}
		connConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe

		db := stdlib.OpenDB(*connConfig)
		db.SetMaxIdleConns(b.config.MaxIdleConns)
		db.SetMaxOpenConns(b.config.MaxOpenConns)
		db.SetConnMaxIdleTime(b.config.MaxConnIdleTime)
		db.SetConnMaxLifetime(b.config.MaxConnLifetime)

		nodes = append(nodes, hasql.NewNode(fmt.Sprintf("%s:%d", connConfig.Host, connConfig.Port), db))

		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel() //nolint
		testConn, err := db.Conn(waitCtx)
		if err != nil {
			log.Error(ctx, fmt.Sprintf("failed to open conn #%d", idx), zap.Error(err))
			continue
		}
		if err := testConn.Close(); err != nil {
			log.Error(ctx, fmt.Sprintf("failed to close conn #%d", idx), zap.Error(err))
			continue
		}
	}

	const updateInterval = 2 * time.Second

	opts := []hasql.ClusterOpt[*sql.DB]{
		hasql.WithUpdateInterval[*sql.DB](updateInterval),
		hasql.WithNodePicker(&hasql.RoundRobinNodePicker[*sql.DB]{}),
	}

	discoverer := hasql.NewStaticNodeDiscoverer(nodes...)
	cluster, err := hasql.NewCluster(discoverer, hasql.PostgreSQLChecker, opts...)
	if err != nil {
		return err
	}

	waitCtx, cancel := context.WithTimeout(ctx, 120*updateInterval)
	defer cancel()

	log.Debug(ctx, "waiting for node cluster")
	start := time.Now()
	if _, err = cluster.WaitForNode(waitCtx, hasql.Primary); err != nil {
		log.Debug(ctx, "error waiting for node", zap.Error(err), zap.Any("duration", time.Since(start).Seconds()))
		return err
	}

	log.Debug(ctx, "got primary node", zap.Any("duration", time.Since(start).Seconds()))
	b.cluster = cluster
	return nil
}

func (b *hasqlCluster) Disconnect(_ context.Context) error {
	if b.cluster == nil {
		return nil
	}

	err := b.cluster.Close()
	if err != nil {
		return err
	}
	b.cluster = nil
	return nil
}

func (b *hasqlCluster) BeginTx(ctx context.Context) (*sql.Tx, error) {
	n, err := b.getNode(ctx, hasql.Primary)
	if err != nil {
		return nil, err
	}

	return n.DB().BeginTx(ctx, nil)
}
