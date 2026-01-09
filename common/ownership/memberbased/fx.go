package memberbased

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/ownership"
	"go.uber.org/fx"
)

type (
	watcherParams struct {
		fx.In

		Monitor        membership.Monitor
		Logger         log.Logger
		MetricsHandler metrics.Handler
	}
)

var Module = fx.Provide(
	func(p watcherParams) (ownership.HistoryShardWatcher, error) {
		return newWatcher(p)
	},
	func() ownership.HistoryShardOwner {
		return noopReporter{}
	},
)
