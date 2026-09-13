package ingest

var registry = map[string]ParseFunc{}

// Register makes a source type available by name. Called from a plugin
// package's init(), so importing the package for its side effect is enough
// to make it usable - no other file needs to change.
func Register(sourceType string, parse ParseFunc) {
	registry[sourceType] = parse
}

// Registered returns every source type currently registered.
func Registered() []string {
	types := make([]string, 0, len(registry))
	for t := range registry {
		types = append(types, t)
	}
	return types
}

// New builds a FileSource for a registered type, or false if unknown.
func New(sourceType string, paths []string, store OffsetStore, classifier Classifier) (*FileSource, bool) {
	parse, ok := registry[sourceType]
	if !ok {
		return nil, false
	}
	return NewFileSource(sourceType, paths, parse, store, classifier), true
}

// DefaultRule is the seed-time shape for a source type's starter rules -
// separate from core.RuleDef, which carries a DB-assigned ID that doesn't
// exist yet at registration time.
type DefaultRule struct {
	Pattern   string
	Severity  string
	EventType string
}

var defaultRuleRegistry = map[string][]DefaultRule{}

// RegisterDefaultRules attaches starter rules to a source type, called from
// the same init() that registers the parser. Keeps "what rules a new plugin
// ships with" defined inside the plugin itself, instead of a central map in
// main.go that every new plugin would otherwise need to remember to edit.
func RegisterDefaultRules(sourceType string, rules []DefaultRule) {
	defaultRuleRegistry[sourceType] = rules
}

func DefaultRules(sourceType string) []DefaultRule {
	return defaultRuleRegistry[sourceType]
}
