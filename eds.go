package mpayutils

import (
	"time"
)

type EdsClientContract interface {
}

var _ EdsClientContract = (*EdsClient)(nil)

type Config struct {
	Base      string        `yaml:"base"`
	Timeout   time.Duration `yaml:"timeout"`
	EnableLog bool          `yaml:"enableLog"`

	Host  string `yaml:"host"`
	Token string `yaml:"token"`
}

type EdsClient struct {
	client *Client

	host  string
	token string
}

func NewEdsClient(cfg Config) *EdsClient {
	return &EdsClient{
		client: NewClient(Config{
			Base:      cfg.Base,
			Timeout:   cfg.Timeout,
			EnableLog: cfg.EnableLog,
		}, "eds"),
		host:  cfg.Host,
		token: cfg.Token,
	}
}
