package memberbased

import (
	"context"
	"slices"
	"sync"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/ownership"
	"go.temporal.io/server/common/primitives"
)

const (
	ownershipListenerName = "ownership"
)

type (
	Watcher struct {
		mu struct {
			sync.Mutex
			listeners []chan struct{}
		}

		goros              goro.Group
		logger             log.Logger
		membershipUpdateCh chan *membership.ChangedEvent
		metricsHandler     metrics.Handler
		resolver           membership.ServiceResolver
	}
)

func newWatcher(p watcherParams) (*Watcher, error) {
	resolver, err := p.Monitor.GetResolver(primitives.HistoryService)
	if err != nil {
		return nil, err
	}
	return &Watcher{
		resolver:           resolver,
		logger:             p.Logger,
		membershipUpdateCh: make(chan *membership.ChangedEvent, 1),
		metricsHandler:     p.MetricsHandler,
	}, nil
}

func (w *Watcher) Start() error {
	if err := w.resolver.AddListener(
		ownershipListenerName,
		w.membershipUpdateCh,
	); err != nil {
		w.logger.Fatal("Error adding listener", tag.Error(err))
	}

	w.goros.Go(func(ctx context.Context) error {
		w.eventLoop(ctx)
		return nil
	})
	return nil
}

func (w *Watcher) Stop() error {
	if err := w.resolver.RemoveListener(
		ownershipListenerName,
	); err != nil {
		w.logger.Error("Error removing membership update listener", tag.Error(err), tag.OperationFailed)
		return err
	}
	w.goros.Cancel()
	w.goros.Wait()
	return nil
}

func (w *Watcher) eventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case changedEvent := <-w.membershipUpdateCh:
			metrics.MembershipChangedCounter.With(w.metricsHandler).Record(1)

			w.logger.Info("", tag.ValueRingMembershipChangedEvent,
				tag.NumberProcessed(len(changedEvent.HostsAdded)),
				tag.NumberDeleted(len(changedEvent.HostsRemoved)),
				tag.NumberChanged(len(changedEvent.HostsChanged)),
			)

			w.notifyListeners()
		}
	}
}

func (w *Watcher) OwnershipStatus(shardID int32) (ownership.Status, error) {
	ownerInfo, err := w.resolver.Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return ownership.Status{}, err
	}
	return ownership.Status{
		Owner: ownerInfo.GetAddress(),
	}, nil
}

func (w *Watcher) Owners() (r []ownership.Address) {
	m := w.resolver.Members()
	for _, h := range m {
		r = append(r, h.GetAddress())
	}
	return
}

func (w *Watcher) AcquireNotifyChannel() (chan struct{}, func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	ch := make(chan struct{}, 1)
	w.mu.listeners = append(w.mu.listeners, ch)
	return ch, sync.OnceFunc(func() {
		w.releaseChannel(ch)
	})
}

func (w *Watcher) notifyListeners() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ch := range w.mu.listeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (w *Watcher) releaseChannel(ch chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := slices.Index(w.mu.listeners, ch)
	if idx == -1 {
		return
	}
	w.mu.listeners = slices.Delete(w.mu.listeners, idx, idx+1)
}
