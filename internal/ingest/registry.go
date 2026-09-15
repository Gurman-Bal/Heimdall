package ingest

var registry = map[string]ParseFunc{}

// Register makes a source type available by name.
func Register(sourceType string, parse ParseFunc) {
	registry[sourceType] = parse
}

// Registered returns every registered source type.
func Registered() []string {
	types := make([]string, 0, len(registry))

	for t := range registry {
		types = append(types, t)
	}

	return types
}

// New builds a FileSource for a registered source type.
func New(
	sourceType string,
	paths []string,
	store OffsetStore,
	classifier Classifier,
) (*FileSource, bool) {
	parse, ok := registry[sourceType]
	if !ok {
		return nil, false
	}

	return NewFileSource(
		sourceType,
		paths,
		parse,
		store,
		classifier,
	), true
}

type DefaultRule struct {
	Pattern   string
	Severity  string
	EventType string
}

var defaultRuleRegistry = map[string][]DefaultRule{}

// RegisterDefaultRules attaches starter rules to a source type.
func RegisterDefaultRules(
	sourceType string,
	rules []DefaultRule,
) {
	defaultRuleRegistry[sourceType] = rules
}

func DefaultRules(sourceType string) []DefaultRule {
	return defaultRuleRegistry[sourceType]
}
