//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination ownership_mock.go
package ownership

type ShardOwnershipStatus int

const (
	ShardOwnershipStatusReleased ShardOwnershipStatus = iota
	ShardOwnershipStatusAcquired
)

type (
	Address = string

	Status struct {
		// Owner is the address of the current owning host/pod/entity.
		Owner Address
		// NextOwner, if non-empty, is the address of a host that will shortly assume ownership. Clients may
		// ignoreNextOwner, and send requests to Owner. Requests to Owner will eventually return ownership lost
		// errors.
		NextOwner Address
		// If non-zero, this is a Lamport clock value that can be used to compare ownership responses.
		Version int64
	}

	HistoryShardWatcher interface {
		// Owners returns all known current shard owners.
		Owners() []Address
		// OwnershipStatus returns status for a history shard.
		OwnershipStatus(shardID int32) (Status, error)
		// AcquireNotifyChannel returns a channel that will get signaled whenever
		// a change to any shard ownership has altered. Users should then call
		// OwnershipStatus for any monitored shards to determine if a change has
		// impacted that shard. The returned release function should be called when
		// notifications are no longer needed.
		AcquireNotifyChannel() (ch chan struct{}, release func())
	}

	HistoryShardOwner interface {
		// ReportAccepting signals to the shard ownership coordinator that
		// this history instance is ready to accept shard assignments.
		ReportAccepting()
		// ReportShardOwnership reports this history instances state for the shard.
		ReportShardOwnership(shardID int32, _ ShardOwnershipStatus)
		// ReportStopping signals to the shard ownwership coordinator that
		// this history instance is stopping soon.
		ReportStopping()
	}
)
