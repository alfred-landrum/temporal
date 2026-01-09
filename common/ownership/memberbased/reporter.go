package memberbased

import (
	"go.temporal.io/server/common/ownership"
)

type (
	noopReporter struct{}
)

func (r noopReporter) ReportAccepting() {
}

func (r noopReporter) ReportShardOwnership(int32, ownership.ShardOwnershipStatus) {
}

func (r noopReporter) ReportStopping() {
}
