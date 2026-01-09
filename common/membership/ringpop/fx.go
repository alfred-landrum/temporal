package ringpop

import (
	"go.temporal.io/server/common/membership"
	"go.uber.org/fx"
)

// RingpopMembershipModule provides membership objects given the types in factoryParams.
var RingpopMembershipModule = fx.Provide(
	provideFactory,
	provideMonitor,
	provideMembership,
	provideHostInfoProvider,
)

func provideFactory(lc fx.Lifecycle, params factoryParams) (*factory, error) {
	f, err := newFactory(params)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.StopHook(f.closeTChannel))
	return f, nil
}

func provideMonitor(lc fx.Lifecycle, f *factory) *Monitor {
	m := f.getMonitor()
	lc.Append(fx.StopHook(m.Stop))
	return m
}

func provideMembership(m *Monitor) membership.Monitor {
	return m
}

func provideHostInfoProvider(f *factory) (membership.HostInfoProvider, error) {
	return f.getHostInfoProvider()
}
