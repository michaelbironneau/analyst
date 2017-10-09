package aql

type OptionValue struct {
	Str    *string  ` @QUOTED_STRING`
	Number *float64 `| @NUMBER`
}

type Option struct {
	Key   string       `@IDENT '='`
	Value *OptionValue `@@`
}

type SourceSink struct {
	Script   *string `SCRIPT @QUOTED_STRING`
	Database *string `| CONNECTION @IDENT`
	Block    *string `| BLOCK @IDENT`
	Global   bool    `| @GLOBAL`
}

type Query struct {
	Name        string        `QUERY @QUOTED_STRING`
	Extern      *string       `[EXTERN @QUOTED_STRING]`
	Sources     []*SourceSink `FROM @@ {"," @@}`
	Content     string        `'(' @PAREN_BODY ')'`
	Destination *SourceSink   `INTO @@`
	Options     []*Option     `[WITH '(' @@ {"," @@ } ')' ]`
}

type Script struct {
	Name        string        `SCRIPT @QUOTED_STRING`
	Extern      *string       `[EXTERN @QUOTED_STRING]`
	Sources     []*SourceSink `FROM @@ {"," @@}`
	Content     string        `'(' @PAREN_BODY ')'`
	Destination *SourceSink   `INTO @@`
	Options     []*Option     `[WITH '(' @@ {"," @@ } ')' ]`
}

type Include struct {
	Name   string `INCLUDE @QUOTED_STRING`
	Source string `FROM @QUOTED_STRING`
}

type Blocks struct {
	Queries  []*Query   `{ @@`
	Includes []*Include `| @@ `
	Scripts  []*Script  ` | @@ }`
}
