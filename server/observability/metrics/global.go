package metrics

import "sync"

var (
	defaultRegistryOnce sync.Once
	defaultRegistry     *Registry
	defaultRecorder     *RuntimeRecorder
)

// DefaultRegistry returns a process-wide P0 metrics registry.
func DefaultRegistry() *Registry {
	defaultRegistryOnce.Do(func() {
		defaultRegistry = NewRegistry()
		RegisterP0Metrics(defaultRegistry)
		defaultRecorder = NewRuntimeRecorder(defaultRegistry)
	})
	return defaultRegistry
}

// DefaultRuntimeRecorder returns a process-wide runtime recorder backed by the
// default registry.
func DefaultRuntimeRecorder() *RuntimeRecorder {
	DefaultRegistry()
	return defaultRecorder
}
