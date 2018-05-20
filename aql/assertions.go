package aql

import (
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"strings"
)

var (
	assertionLexer = lexer.Unquote(lexer.Upper(lexer.Must(lexer.Regexp(`(\s+)`+
		`|(?P<Keyword>(?i)IT\s|OUTPUTS\s|COLUMN\s|UNIQUE\s|SHOULD\s|HAVE\s|AT\sLEAST\s|AT\sMOST\s|EXACTLY\s|DISTINCT\s|SATISFIES\s|IS\s|NOT\s|NULL\s|NO\s|DUPLICATE\s|VALUES\s|ROWS\s|CONTAINS\s)`+
		`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)`+
		`|(?P<Number>[0-9]+)`+
		`|(?P<String>'[^']*'|"[^"]*")`+
		`|(?P<Any>.+)`,
	)), "Keyword"), "String")
)

type GlobalAssertion struct {
	NRows *HasN   `"OUTPUTS " @@ "ROWS"`
	Expr  *string `| "SATISFIES " @Any`
}

type HasN struct {
	AtLeast bool `(@"AT LEAST "`
	AtMost  bool `| @"AT MOST "`
	Exactly bool `| @"EXACTLY ")`
	N       int  `@Number `
}

type ColumnAssertion struct {
	TargetColumn *string `@Ident "HAS" `
	Distinct     *HasN   `(@@ "DISTINCT " "VALUES"`
	NoDuplicates bool    `| @"UNIQUE " "VALUES"`
	NoNulls      bool    `|@"NO " "NULL " "VALUES")`
}

type Assertion struct {
	Global *GlobalAssertion `"IT " @@`
	Column *ColumnAssertion `| "COLUMN " @@`
}

func NewAssertion(aqlBody string) (*Assertion, error) {
	p, err := participle.Build(&Assertion{}, assertionLexer)

	if err != nil {
		panic(err)
	}
	var a Assertion
	err = p.ParseString(aqlBody, &a)

	if err != nil {
		return nil, err
	}

	return &a, nil
}

func ParseAssertions(aql string) ([]Assertion, error) {
	lines := strings.Split(aql, ";")
	var ret []Assertion
	for i := range lines {
		s := strings.TrimSpace(lines[i])
		if len(s) == 0 {
			continue
		}
		a, err := NewAssertion(s)
		if err != nil {
			return nil, err
		}
		if a == nil {
			panic("assertion from NewAssertion() was nil")
		}
		ret = append(ret, *a)
	}
	return ret, nil
}
