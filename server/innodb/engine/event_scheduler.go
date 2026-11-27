package engine

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ScheduledEvent struct {
	Name      string
	ExecuteAt time.Time
	Interval  time.Duration
	Repeat    bool
	Execute   func(context.Context) error
}

type scheduledEventState struct {
	definition ScheduledEvent
	next       time.Time
}

// EventScheduler is an explicit lifecycle boundary for persisted MySQL EVENT
// definitions. It is disabled by default; loading metadata never starts a
// background goroutine or executes user SQL implicitly.
type EventScheduler struct {
	mu         sync.RWMutex
	enabled    bool
	running    bool
	cancel     context.CancelFunc
	done       chan struct{}
	events     map[string]scheduledEventState
	lastErrors map[string]string
}

func NewEventScheduler(enabled bool) *EventScheduler {
	return &EventScheduler{enabled: enabled, events: make(map[string]scheduledEventState), lastErrors: make(map[string]string)}
}

func (s *EventScheduler) Start(parent context.Context) bool {
	s.mu.Lock()
	if !s.enabled || s.running {
		s.mu.Unlock()
		return false
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.running = true
	done := s.done
	s.mu.Unlock()
	go s.run(ctx, done)
	return true
}

func (s *EventScheduler) Stop() bool {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return false
	}
	cancel, done := s.cancel, s.done
	s.cancel = nil
	s.running = false
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return true
}

func (s *EventScheduler) AddEvent(event ScheduledEvent) error {
	if event.Name == "" || event.Execute == nil {
		return fmt.Errorf("event name and execute callback are required")
	}
	if event.Repeat && event.Interval <= 0 {
		return fmt.Errorf("repeating event %q requires a positive interval", event.Name)
	}
	if event.ExecuteAt.IsZero() {
		event.ExecuteAt = time.Now().UTC()
	}
	s.mu.Lock()
	s.events[event.Name] = scheduledEventState{definition: event, next: event.ExecuteAt}
	delete(s.lastErrors, event.Name)
	s.mu.Unlock()
	return nil
}

func (s *EventScheduler) RemoveEvent(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.events[name]; !ok {
		return false
	}
	delete(s.events, name)
	delete(s.lastErrors, name)
	return true
}

func (s *EventScheduler) LastError(name string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastErrors[name]
}

func (s *EventScheduler) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.runDue(ctx, now)
		}
	}
}

func (s *EventScheduler) runDue(ctx context.Context, now time.Time) {
	s.mu.Lock()
	due := make([]scheduledEventState, 0)
	for name, state := range s.events {
		if !state.next.After(now) {
			due = append(due, state)
			if state.definition.Repeat {
				state.next = now.Add(state.definition.Interval)
				s.events[name] = state
			} else {
				delete(s.events, name)
			}
		}
	}
	s.mu.Unlock()
	for _, state := range due {
		if err := state.definition.Execute(ctx); err != nil {
			s.mu.Lock()
			s.lastErrors[state.definition.Name] = err.Error()
			s.mu.Unlock()
		}
	}
}

func (s *EventScheduler) Enabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled
}

func (s *EventScheduler) Running() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

func (s *EventScheduler) SetEnabled(enabled bool) {
	s.mu.Lock()
	s.enabled = enabled
	s.mu.Unlock()
}
