package aql

import (
	"fmt"
	"github.com/alecthomas/participle"
	"os"
)

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
	Sources     []SourceSink `FROM @@ {"," @@}`
	Content     string        `'(' @PAREN_BODY ')'`
	Destination *SourceSink   `INTO @@`
	Options     []Option     `[WITH '(' @@ {"," @@ } ')' ]`
}

type Script struct {
	Name        string        `SCRIPT @QUOTED_STRING`
	Extern      *string       `[EXTERN @QUOTED_STRING]`
	Sources     []*SourceSink `FROM @@ {"," @@}`
	Content     string        `'(' @PAREN_BODY ')'`
	Destination *SourceSink   `INTO @@`
	Options     []Option     `[WITH '(' @@ {"," @@ } ')' ]`
}

type Test struct {
	Query   bool          `TEST [@QUERY `
	Script  bool          `|@SCRIPT ]`
	Name    string        `@QUOTED_STRING`
	Extern  *string       `[EXTERN @QUOTED_STRING]`
	Sources []SourceSink `FROM @@ {"," @@}`
	Content string        `'(' @PAREN_BODY ')'`
	Options []Option     `[WITH '(' @@ {"," @@ } ')' ]`
}

type Global struct {
	Name    string    `GLOBAL @QUOTED_STRING`
	Content string    `'(' @PAREN_BODY ')'`
	Options []Option `[WITH '(' @@ {"," @@ } ')' ]`
}

type Include struct {
	Name   string `INCLUDE @QUOTED_STRING`
	Source string `FROM @QUOTED_STRING`
}

type Description struct {
	Content string `DESCRIPTION @QUOTED_STRING`
}

type Blocks struct {
	Description *Description `[@@]`
	Queries  []Query   `{ @@`
	Includes []Include `| @@ `
	Tests    []Test    `| @@ `
	Globals []Global   `| @@ `
	Scripts  []Script  ` | @@ }`
}

func ParseString(s string) (b *Blocks, err error){
	defer func(){
		if r := recover(); r != nil {
			err = fmt.Errorf("parser error: %v", r)
			return
		}
	}()

	parser, err := participle.Build(&Blocks{}, &definition{})
	if err != nil {
		panic(err)
	}

	b = &Blocks{}
	err = parser.ParseString(s, b)
	return
}

func ParseFile(path string) (b *Blocks, err error){
	f, err := os.Open(path)
	defer f.Close()
	defer func(){
		if r := recover(); r != nil {
			err = fmt.Errorf("parser error: %v", r)
			return
		}
	}()

	parser, err := participle.Build(&Blocks{}, &definition{})
	if err != nil {
		panic(err)
	}

	b = &Blocks{}
	err = parser.Parse(f, b)
	return
}