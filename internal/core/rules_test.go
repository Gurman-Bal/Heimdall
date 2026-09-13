package core

import "testing"

func TestRuleEngineClassify(t *testing.T) {
	re := NewRuleEngine()
	errs := re.Load("truenas", []RuleDef{
		{ID: 1, Pattern: `(?i)degraded`, Severity: "warning", EventType: "warning", Priority: 10},
		{ID: 2, Pattern: `(?i)critical failure`, Severity: "critical", EventType: "error", Priority: 5},
	})
	if len(errs) != 0 {
		t.Fatalf("unexpected compile errors: %v", errs)
	}

	severity, eventType := re.Classify("truenas", "pool status degraded")
	if severity != "warning" || eventType != "warning" {
		t.Errorf("got (%s, %s), want (warning, warning)", severity, eventType)
	}
}

func TestRuleEnginePriorityOrdering(t *testing.T) {
	re := NewRuleEngine()
	// Both patterns match the same line - lower priority number must win,
	// regardless of the order they were passed in.
	re.Load("test", []RuleDef{
		{ID: 1, Pattern: `error`, Severity: "warning", EventType: "generic_error", Priority: 100},
		{ID: 2, Pattern: `critical error`, Severity: "critical", EventType: "specific_error", Priority: 10},
	})

	severity, eventType := re.Classify("test", "a critical error occurred")
	if severity != "critical" || eventType != "specific_error" {
		t.Errorf("got (%s, %s), want (critical, specific_error) - priority ordering broken", severity, eventType)
	}
}

func TestRuleEngineNoMatchFallsBackToInfo(t *testing.T) {
	re := NewRuleEngine()
	re.Load("test", []RuleDef{{ID: 1, Pattern: `nomatch`, Severity: "critical", EventType: "x", Priority: 10}})

	severity, eventType := re.Classify("test", "totally unrelated line")
	if severity != "info" || eventType != "log" {
		t.Errorf("got (%s, %s), want (info, log)", severity, eventType)
	}
}

func TestRuleEngineInvalidRegexSkipped(t *testing.T) {
	re := NewRuleEngine()
	errs := re.Load("test", []RuleDef{
		{ID: 1, Pattern: `(unclosed`, Severity: "warning", EventType: "bad", Priority: 10},
		{ID: 2, Pattern: `valid`, Severity: "info", EventType: "good", Priority: 20},
	})

	if len(errs) != 1 {
		t.Fatalf("expected exactly 1 compile error, got %d: %v", len(errs), errs)
	}

	// the valid rule should still work despite the bad one alongside it
	severity, eventType := re.Classify("test", "this is valid")
	if severity != "info" || eventType != "good" {
		t.Errorf("valid rule didn't survive alongside a broken one: got (%s, %s)", severity, eventType)
	}
}
