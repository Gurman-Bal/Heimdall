package core

import (
	"regexp"
	"sync"
	"time"
)

// NoiseDetector flags rapidly repeating log lines as noise even when no
// rule has been written for them yet. It's a fallback, not a replacement
// for RuleEngine: wire it in so it only runs on lines the rule engine
// didn't already have an opinion about (severity "info", type "log" - the
// engine's own fallback). That way it can only catch new chatter, never
// override something an explicit rule already classified as critical or
// warning, even if that thing happens to repeat a lot too.
type NoiseDetector struct {
	mu        sync.Mutex
	buckets   map[string]*noiseBucket
	window    time.Duration
	threshold int
}

type noiseBucket struct {
	windowStart time.Time
	count       int
}

// NewNoiseDetector reports a signature as noise once it's recurred
// `threshold` times within `window`. A sensible starting point is a few
// seconds and a handful of repeats - genuinely chatty sources (docker
// bridge churn, a crash-looping container) fire far faster than that;
// a one-off burst of 2-3 similar lines from a real event won't trip it.
func NewNoiseDetector(window time.Duration, threshold int) *NoiseDetector {
	d := &NoiseDetector{
		buckets:   map[string]*noiseBucket{},
		window:    window,
		threshold: threshold,
	}
	go d.cleanup()
	return d
}

// variableToken matches the parts of a line that differ between otherwise
// identical repeats - hex/interface identifiers, PIDs, standalone numbers -
// so "veth5281a1b" and "veth9bf18b0", or "port 3" and "port 7", normalize
// to the same signature instead of being treated as unrelated messages.
var variableToken = regexp.MustCompile(`\b(0x)?[0-9a-fA-F]{4,}\b|\b\d+\b`)

func normalize(message string) string {
	return variableToken.ReplaceAllString(message, "#")
}

// Check records one occurrence of (source, message) and reports whether
// this signature has now repeated often enough, recently enough, to call
// it noise. Safe for concurrent use.
func (d *NoiseDetector) Check(source, message string) bool {
	key := source + "\x00" + normalize(message)
	now := time.Now()

	d.mu.Lock()
	defer d.mu.Unlock()

	b, ok := d.buckets[key]
	if !ok || now.Sub(b.windowStart) > d.window {
		d.buckets[key] = &noiseBucket{windowStart: now, count: 1}
		return false
	}

	b.count++
	return b.count >= d.threshold
}

// cleanup drops buckets that haven't been hit in a while so a one-time
// burst doesn't hold memory forever.
func (d *NoiseDetector) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-10 * d.window)
		d.mu.Lock()
		for k, b := range d.buckets {
			if b.windowStart.Before(cutoff) {
				delete(d.buckets, k)
			}
		}
		d.mu.Unlock()
	}
}
