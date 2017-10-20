package aql

import (
	"bytes"
	"fmt"
	"github.com/alecthomas/participle"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

//MaxIncludeDepth is the maximum depth of includes that will be processed before an error is returned.
const MaxIncludeDepth = 8

type OptionValue struct {
	Str    *string  ` @QUOTED_STRING`
	Number *float64 `| @NUMBER`
}

type Option struct {
	Key   string       `@IDENT '='`
	Value *OptionValue `@@`
}

type SourceSink struct {
	Script   *string `(SCRIPT @QUOTED_STRING`
	Database *string `| CONNECTION @IDENT`
	Global   bool    `| @GLOBAL`
	Block    *string `| BLOCK @IDENT)`
	Alias    *string `[AS @QUOTED_STRING]`
}

type Query struct {
	Name        string       `QUERY @QUOTED_STRING`
	Extern      *string      `[EXTERN @QUOTED_STRING]`
	Sources     []SourceSink `FROM @@ {"," @@}`
	Content     string       `['(' @PAREN_BODY ')']`
	Destination *SourceSink  `INTO @@`
	Options     []Option     `[WITH '(' @@ {"," @@ } ')' ]`
}

type Script struct {
	Name        string        `SCRIPT @QUOTED_STRING`
	Extern      *string       `[EXTERN @QUOTED_STRING]`
	Sources     []*SourceSink `FROM @@ {"," @@}`
	Content     string        `['(' @PAREN_BODY ')']`
	Destination *SourceSink   `INTO @@`
	Options     []Option      `[WITH '(' @@ {"," @@ } ')' ]`
}

type Test struct {
	Query       bool         `TEST [@QUERY `
	Script      bool         `|@SCRIPT ]`
	Name        string       `@QUOTED_STRING`
	Extern      *string      `[EXTERN @QUOTED_STRING]`
	Sources     []SourceSink `FROM @@ {"," @@}`
	Content     string       `['(' @PAREN_BODY ')']`
	Destination *SourceSink  `[INTO @@]`
	Options     []Option     `[WITH '(' @@ {"," @@ } ')' ]`
}

type Global struct {
	Name    string   `GLOBAL @QUOTED_STRING`
	Content string   `'(' @PAREN_BODY ')'`
	Options []Option `[WITH '(' @@ {"," @@ } ')' ]`
}

type Include struct {
	Source string `INCLUDE @QUOTED_STRING`
}

type Description struct {
	Content string `DESCRIPTION @QUOTED_STRING`
}

type UnparsedConnection struct {
	Name    string   `CONNECTION @QUOTED_STRING`
	Content string   `'(' @PAREN_BODY ')'`
	Options []Option `[WITH '(' @@ {"," @@ } ')' ]`
}

type Connection struct {
	Name             string
	Driver           string
	ConnectionString string
	Options          []Option
}

type JobScript struct {
	Description *Description         `[@@]`
	Queries     []Query              `{ @@`
	Connections []UnparsedConnection `| @@`
	Includes    []Include            `| @@ `
	Tests       []Test               `| @@ `
	Globals     []Global             `| @@ `
	Scripts     []Script             ` | @@ }`
}

func (b *JobScript) EvaluateParametrizedContent(globals []Option) error {
	var err error
	for i := range b.Queries {
		b.Queries[i].Content, err = evaluateContent(b.Queries[i].Content, b.Queries[i].Options, globals)
		if err != nil {
			return err
		}
	}

	for i := range b.Scripts {
		b.Scripts[i].Content, err = evaluateContent(b.Scripts[i].Content, b.Scripts[i].Options, globals)
		if err != nil {
			return err
		}
	}

	for i := range b.Tests {
		b.Tests[i].Content, err = evaluateContent(b.Tests[i].Content, b.Tests[i].Options, globals)
		if err != nil {
			return err
		}
	}

	return nil
}

func evaluateContent(content string, locals []Option, globals []Option) (string, error) {
	opts := make(map[string]interface{})
	for _, v := range globals {
		//v.Value cannot be nil
		if v.Value.Str != nil {
			opts[v.Key] = v.Value.Str
		} else {
			opts[v.Key] = v.Value.Number
		}
	}
	//override with locals
	for _, v := range locals {
		//v.Value cannot be nil
		if v.Value.Str != nil {
			opts[v.Key] = v.Value.Str
		} else {
			opts[v.Key] = v.Value.Number
		}
	}
	t := template.Must(template.New("").Parse(content))
	var b bytes.Buffer
	err := t.Execute(&b, opts)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func (b *JobScript) ResolveExternalContent() error {
	if err := b.resolveExtern(""); err != nil {
		return err
	}
	for i := range b.Includes {
		if err := b.resolveInclude(i, 0, ""); err != nil {
			return err
		}
	}
	return nil
}

func (b *JobScript) resolveExtern(cwd string) error {
	for i, query := range b.Queries {
		if query.Extern != nil {
			s, err := getContent(cwd, *query.Extern)
			if err != nil {
				return err
			}
			b.Queries[i].Content = s
			b.Queries[i].Extern = nil
		}
	}

	for i, script := range b.Scripts {
		if script.Extern != nil {
			s, err := getContent(cwd, *script.Extern)
			if err != nil {
				return err
			}
			b.Scripts[i].Content = s
			b.Scripts[i].Extern = nil
		}
	}

	for i, test := range b.Tests {
		if test.Extern != nil {
			s, err := getContent(cwd, *test.Extern)
			if err != nil {
				return err
			}
			b.Tests[i].Content = s
			b.Tests[i].Extern = nil
		}
	}

	return nil
}

func getContent(cwd, path string) (string, error) {
	b, err := ioutil.ReadFile(filepath.Join(cwd, path))
	return string(b), err
}

//resolve the given include, recursively if need be. Doesn't do bound checks on index.
func (b *JobScript) resolveInclude(index, depth int, cwd string) error {
	if depth > MaxIncludeDepth {
		return fmt.Errorf("maximum INCLUDE depth %v reached", MaxIncludeDepth)
	}
	path := b.Includes[index].Source
	bb, err := ParseFile(filepath.Join(cwd, path))
	//bb, err := ParseFile(path)
	if err != nil {
		return err
	}

	for i := range bb.Includes {
		err = bb.resolveInclude(i, depth+1, filepath.Dir(path))
		if err != nil {
			return err
		}
	}
	bb.resolveExtern(filepath.Dir(filepath.Join(cwd, path)))
	b.union(bb)
	b.Includes = nil
	return nil
}

//Union merges two sets of blocks EXCLUDING includes. It is not commutative - the blocks of the first blocks will go first,
//and the description of the second set of blocks will be ignored unless the first block has an empty description.
func (b *JobScript) union(other *JobScript) {
	if b.Description == nil && other.Description != nil {
		b.Description = other.Description
	}
	b.Queries = append(b.Queries, other.Queries...)
	b.Connections = append(b.Connections, other.Connections...)
	//b.Includes = append(b.Includes, other.Includes...)
	b.Tests = append(b.Tests, other.Tests...)
	b.Globals = append(b.Globals, other.Globals...)
	b.Scripts = append(b.Scripts, other.Scripts...)
}

func parseConnections(conns []UnparsedConnection) ([]Connection, error) {
	if len(conns) == 0 {
		return nil, nil
	}
	type connOpts struct {
		Options []Option `@@ {"," @@ }`
	}
	parser, err := participle.Build(&connOpts{}, &definition{})
	if err != nil {
		panic(err)
	}
	cs := make([]Connection, len(conns), len(conns))
	for i := range conns {
		cs[i].Name = conns[i].Name
		cs[i].Options = conns[i].Options
		var opts connOpts
		err = parser.ParseString(conns[i].Content, &opts)
		if err != nil {
			return nil, fmt.Errorf("invalid connection %s: %v", cs[i].Name, err)
		}
		err = optsToConn(opts.Options, &cs[i])
		if err != nil {
			return nil, fmt.Errorf("invalid connection %s: %v", cs[i].Name, err)
		}
	}
	return cs, nil
}

func optsToConn(opts []Option, conn *Connection) error {
	for _, o := range opts {
		if strings.ToUpper(o.Key) == "DRIVER" && o.Value.Str != nil {
			conn.Driver = *(o.Value.Str)
		} else if strings.ToUpper(o.Key) == "CONNECTIONSTRING" && o.Value.Str != nil {
			conn.ConnectionString = *(o.Value.Str)
		} else {
			return fmt.Errorf("invalid connection key %s", o.Key)
		}
	}
	if conn.ConnectionString == "" || conn.Driver == "" {
		return fmt.Errorf("both ConnectionString and Driver are required properties")
	}
	return nil
}

//ParseString parses an AQL string into a JobScript struct.
func ParseString(s string) (b *JobScript, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("parser error: %v", r)
			return
		}
	}()

	parser, err := participle.Build(&JobScript{}, &definition{})
	if err != nil {
		panic(err)
	}

	b = &JobScript{}
	err = parser.ParseString(s, b)
	return
}

//ParseFile parses an AQL file into a JobScript struct.
func ParseFile(path string) (b *JobScript, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("parser error: %v", r)
			return
		}
	}()

	parser, err := participle.Build(&JobScript{}, &definition{})
	if err != nil {
		panic(err)
	}

	b = &JobScript{}
	err = parser.Parse(f, b)
	return
}
