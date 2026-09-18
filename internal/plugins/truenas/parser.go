package truenas

import (
	"strings"
	"time"

	"heimdall/internal/core"
	"heimdall/internal/ingest"
)

func init() {
	ingest.Register("truenas", ParseLine)
	ingest.RegisterDefaultRules("truenas", []ingest.DefaultRule{
		{Pattern: `(?i)\b(reallocated sector|pending sector|smart.*fail)\b`, Severity: "critical", EventType: "smart_warning"},
		{Pattern: `(?i)\b(panic|critical|failed|failure)\b`, Severity: "critical", EventType: "error"},
		{Pattern: `(?i)\b(degraded|warn|warning)\b`, Severity: "warning", EventType: "warning"},
		{Pattern: `(?i)\b(denied|refused|error)\b`, Severity: "warning", EventType: "error"},
		{Pattern: `(?i)(docker0|br-[0-9a-f]+|veth[0-9a-f]+).*(entered (forwarding|blocking|disabled) state|entered (promiscuous|allmulticast) mode|renamed from eth0|unregistering)`, Severity: "ignore", EventType: "noise"},
		// Reversed form of the rule above: "eth0: renamed from vethXXXXX"
		// puts the veth token at the end, after "renamed from", instead of
		// before it - the existing pattern requires the veth token first,
		// so this form slipped through as unclassified info/log noise.
		{Pattern: `(?i)eth0: renamed from veth[0-9a-f]+`, Severity: "ignore", EventType: "noise"},
	})
}

func ParseLine(line string) core.Event {
	return core.Event{
		Timestamp: time.Now(),
		Source:    "truenas",
		Message:   strings.TrimSpace(line),
	}
}
