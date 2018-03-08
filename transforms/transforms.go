package transforms

import (
	"fmt"
	"github.com/michaelbironneau/analyst/engine"
	"strings"
)

type initializer func(string) (engine.SequenceableTransform, error)

var (
	types map[string]initializer = map[string]initializer{
		"aggregate": aggregateInitializer,
		"lookup":    lookupInitializer,
		"apply":   applyInitializer,
	}
)

//Parse parses and initializes a transform given its body and input sequence.
func Parse(s string) (engine.SequenceableTransform, error) {
	words := strings.SplitN(strings.TrimSpace(s), " ", 2)
	if len(words) == 0 {
		return nil, fmt.Errorf("syntax error, expecting keyword (eg. AGGREGATE) followed by space")
	}
	var f = types[strings.ToLower(words[0])]
	if f == nil {
		return nil, fmt.Errorf("unknown transform %s", words[0])
	}
	return f(s)
}
