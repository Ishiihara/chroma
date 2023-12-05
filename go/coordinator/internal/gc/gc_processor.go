package gc

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/common"
)

type GCProcessor interface {
	common.Component
	Process(ctx context.Context)
}
