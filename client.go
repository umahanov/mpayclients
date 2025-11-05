package mpayutils

import (
	"net/http"
	"time"
)

type Opt func(c *http.Client)

type Client struct {
	Name string

	logEnabled bool
	base       string
	http       *http.Client
}

func NewClient(cfg Config, name string, opts ...Opt) *Client {
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
	}
	httpClient.Timeout = cfg.Timeout

	for _, o := range opts {
		o(httpClient)
	}

	return &Client{
		Name:       name,
		http:       httpClient,
		base:       cfg.Base,
		logEnabled: cfg.EnableLog,
	}
}

func WithTransport(t http.RoundTripper) Opt {
	return func(c *http.Client) {
		c.Transport = t
	}
}

func WithCustomHttpClient(client *http.Client) Opt {
	return func(c *http.Client) {
		*c = *client
	}
}

func (c *Client) LogEnabled() bool {
	return c.logEnabled
}
