package tailgater

import "context"

type Tailgater interface {
	Tail(ctx context.Context, message TailMessage) error
}
