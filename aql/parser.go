package aql

type OptionValue struct {
	Str    *string  ` @QUOTED_STRING`
	Number *float64 `| @NUMBER`
}

type Option struct {
	Key   string       `@IDENT '='`
	Value *OptionValue `@@`
}

type Source struct {
	Script *string `SCRIPT @QUOTED_STRING`
	Database *string `| CONNECTION @IDENT`
	Block *string `| BLOCK @IDENT`
	Global bool `| @GLOBAL`
}

//QUERY 'name' [EXTERN 'source'] FROM source (body) INTO destination WITH (k=v, k2=v2, ...)
type Query struct {
	Name        string    `QUERY @QUOTED_STRING`
	Extern      string    `[EXTERN @QUOTED_STRING]`
	Sources     []*Source    `FROM @@ {"," @@}`
	Content     string    `'(' @PAREN_BODY ')'`
	Destination string    `INTO @IDENT`
	Options     []*Option `[WITH '(' @@ {"," @@ } ')' ]`
}
