package db

import "context"

type Connectable interface {
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
}
