package discovery

import "context"

type Interface interface {
	Discover(ctx context.Context) string
}
