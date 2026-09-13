package core

import "sync/atomic"

// StatusTracker holds Heimdall's own current operational state - running,
// restarting, stopping - so the dashboard can show "what is the app doing
// right now" rather than just a feed of past log lines.
type StatusTracker struct {
	state atomic.Value
}

func NewStatusTracker() *StatusTracker {
	st := &StatusTracker{}
	st.state.Store("running")
	return st
}

func (s *StatusTracker) Set(state string) { s.state.Store(state) }
func (s *StatusTracker) Get() string      { return s.state.Load().(string) }
