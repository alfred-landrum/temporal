package shard

import (
	"context"
	"time"

	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	pownership "go.temporal.io/server/common/ownership"
	"go.temporal.io/server/service/history/configs"
)

const (
	shardControllerMembershipUpdateListenerName = "ShardController"
)

type (
	// ownership acts as intermediary between membership and the shard controller.
	// Upon receiving membership update events, it calls the controller's
	// acquireShards method, which acquires or closes shards as needed.
	// The controller calls its verifyOwnership method when asked to
	// acquire a shard, to check that membership believes this host should
	// own the shard.
	ownership struct {
		config           *configs.Config
		goros            goro.Group
		hostInfoProvider membership.HostInfoProvider
		watcher          pownership.HistoryShardWatcher
		reporter         pownership.HistoryShardOwner
		logger           log.Logger
	}
)

func newOwnership(
	config *configs.Config,
	watcher pownership.HistoryShardWatcher,
	reporter pownership.HistoryShardOwner,
	hostInfoProvider membership.HostInfoProvider,
	logger log.Logger,
) *ownership {
	hostIdentity := hostInfoProvider.HostInfo().Identity()
	logger = log.With(logger, tag.ComponentShardController, tag.Address(hostIdentity))
	return &ownership{
		config:           config,
		hostInfoProvider: hostInfoProvider,
		watcher:          watcher,
		reporter:         reporter,
		logger:           logger,
	}
}

func (o *ownership) start(controller *ControllerImpl) {
	o.goros.Go(func(ctx context.Context) error {
		notifyCh, release := o.watcher.AcquireNotifyChannel()
		o.eventLoop(ctx, controller, notifyCh)
		release()
		return nil
	})
}

func (o *ownership) eventLoop(ctx context.Context, controller *ControllerImpl, notifyCh <-chan struct{}) {
	acquireTicker := time.NewTicker(o.config.AcquireShardInterval())
	defer acquireTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-acquireTicker.C:
			controller.acquireShards(ctx)
		case <-notifyCh:
			controller.acquireShards(ctx)
		}
	}
}

func (o *ownership) shardAcquired(shardID int32) {
	o.reporter.ReportShardOwnership(shardID, pownership.ShardOwnershipStatusAcquired)
}

func (o *ownership) shardReleased(shardID int32) {
	o.reporter.ReportShardOwnership(shardID, pownership.ShardOwnershipStatusReleased)
}

func (o *ownership) stop() {
	o.goros.Cancel()
	o.goros.Wait()
}

type shardAcquireAction int

const (
	shardAcquireActionNone    shardAcquireAction = 0
	shardAcquireActionAcquire shardAcquireAction = 1
	shardAcquireActionRelease shardAcquireAction = 2
)

func (o *ownership) shouldAcquire(isAcquired bool, shardID int32) (shardAcquireAction, string) {
	status, err := o.watcher.OwnershipStatus(shardID)
	if err != nil {
		return shardAcquireActionNone, ""
	}
	myAddress := o.hostInfoProvider.HostInfo().GetAddress()
	switch {
	case status.NextOwner == "":
		if status.Owner == myAddress {
			return shardAcquireActionAcquire, status.Owner
		}
		return shardAcquireActionRelease, status.Owner
	case status.NextOwner == myAddress:
		return shardAcquireActionAcquire, status.NextOwner
	case status.Owner == myAddress:
		// Some other address is the NextOwner, and we are listed as Owner. If we have already acquired the shard,
		// we can keep it; the NextOwner will claim it, and we will eventually see a Shard Ownership Lost error, or the
		// NextOwner address will become the Owner address. If we have not acquired the shard, we should not try to
		// acquire it now that we know NextOwner may also be trying to acquire it, otherwise we could steal the shard
		// from them.
		if isAcquired {
			// We've acquired, return shardAcquireActionAcquire to signal "do nothing".
			return shardAcquireActionAcquire, status.NextOwner
		}
		return shardAcquireActionNone, status.NextOwner
	}
	return shardAcquireActionRelease, status.NextOwner
}
