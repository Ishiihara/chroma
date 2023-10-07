package retry

import (
	"context"
	"time"

	"github.com/chroma/chroma-coordinator/internal/utils/merr"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

// Do will run function with retry mechanism.
// fn is the func to run.
// Option can control the retry times and timeout.
func Do(ctx context.Context, fn func() error, opts ...Option) error {
	// log := log.Ctx(ctx)

	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	var el error

	for i := uint(0); i < c.attempts; i++ {
		if err := fn(); err != nil {
			if i%10 == 0 {
				log.Error("retry func failed", zap.Uint("retry time", i), zap.Error(err))
			}

			err = errors.Wrapf(err, "attempt #%d", i)
			el = merr.Combine(el, err)

			if !IsRecoverable(err) {
				return el
			}

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				el = merr.Combine(el, errors.Wrapf(ctx.Err(), "context done during sleep after run#%d", i))
				return el
			}

			c.sleep *= 2
			if c.sleep > c.maxSleepTime {
				c.sleep = c.maxSleepTime
			}
		} else {
			return nil
		}
	}
	return el
}

// errUnrecoverable is error instance for unrecoverable.
var errUnrecoverable = errors.New("unrecoverable error")

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return merr.Combine(err, errUnrecoverable)
}

// IsRecoverable is used to judge whether the error is wrapped by unrecoverableError.
func IsRecoverable(err error) bool {
	return !errors.Is(err, errUnrecoverable)
}
