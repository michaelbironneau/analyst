package aql

import (
	"bytes"
	"encoding/json"
	"fmt"
	xlsx "github.com/360EntSecGroup-Skylar/excelize"
	"github.com/alecthomas/participle"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"unicode"
)

//MaxIncludeDepth is the maximum depth of includes that will be processed before an error is returned.
const MaxIncludeDepth = 8

type Block interface {
	GetName() string
	GetOptions() []Option
}

type GlobalOption struct {
	Key   string       `SET @IDENT '='`
	Value *OptionValue `@@`
}

type OptionValue struct {
	Str    *string  ` @QUOTED_STRING`
	Number *float64 `| @NUMBER`
}

type Option struct {
	Key   string       `@IDENT '='`
	Value *OptionValue `@@`
}

type SourceSink struct {
	Database  *string  `( CONNECTION @IDENT`
	Global    bool     `| @GLOBAL`
	Console   bool     `| @CONSOLE`
	Variables []string `| PARAMETER '(' @IDENT {"," @IDENT } ')'`
	Block     *string  `| BLOCK @IDENT)`
	Alias     *string  `[AS @QUOTED_STRING]`
}

type Query struct {
	Name         string       `@QUOTED_STRING`
	Extern       *string      `[EXTERN @QUOTED_STRING]`
	Sources      []SourceSink `FROM @@ { "," @@ }`
	Content      string       `['(' @PAREN_BODY ')' ]`
	Parameters   []string     `[USING PARAMETER @IDENT { "," @IDENT }]`
	Destinations []SourceSink `[INTO @@ { "," @@ } ]`
	Options      []Option     `[WITH '(' @@ {"," @@ } ')' ]`
	Dependencies []string     `[AFTER @IDENT {"," @IDENT }]`
}

/*type Exec struct {
	Name         string       `QUERY @QUOTED_STRING`
	Extern       *string      `[EXTERN @QUOTED_STRING]`
	Sources      []SourceSink `FROM @@ { "," @@ }`
	Content      string       `['(' @PAREN_BODY ')' ]`
	Parameters   []string     `[USING PARAMETER @IDENT { "," @IDENT }]`
	Options      []Option     `[WITH '(' @@ {"," @@ } ')' ]`
	Dependencies []string     `[AFTER @IDENT {"," @IDENT }]`
}*/

func (q *Query) GetName() string {
	return q.Name
}

func (q *Query) GetOptions() []Option {
	return q.Options
}

type Transform struct {
	Plugin       bool          `TRANSFORM [@PLUGIN]`
	Name         string        `@QUOTED_STRING`
	Extern       *string       `[EXTERN @QUOTED_STRING]`
	Sources      []*SourceSink `FROM @@ {"," @@}`
	Content      string        `['(' @PAREN_BODY ')']`
	Destinations []SourceSink  `[INTO @@ {"," @@}]`
	Options      []Option      `[WITH '(' @@ {"," @@ } ')' ]`
	Dependencies []string      `[AFTER @IDENT {"," @IDENT }]`
}

func (q *Transform) GetName() string {
	return q.Name
}

func (q *Transform) GetOptions() []Option {
	return q.Options
}

type Declaration struct {
	Name string `DECLARE @IDENT`
}

type Test struct {
	Query        bool         `TEST [@QUERY `
	Plugin       bool         `|@PLUGIN ]`
	Name         string       `@QUOTED_STRING`
	Extern       *string      `[EXTERN @QUOTED_STRING]`
	Sources      []SourceSink `FROM @@ {"," @@}`
	Content      string       `['(' @PAREN_BODY ')']`
	Destinations []SourceSink `[INTO @@ {"," @@}]`
	Options      []Option     `[WITH '(' @@ {"," @@ } ')' ]`
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
	Description   *Description         `[@@]`
	Queries       []Query              `{QUERY @@`
	Declarations  []Declaration        `| @@`
	Connections   []UnparsedConnection `| @@`
	Includes      []Include            `| @@ `
	Tests         []Test               `| @@ `
	Execs         []Query              `|EXEC @@ `
	Globals       []Global             `| @@ `
	GlobalOptions []GlobalOption       `| @@ `
	Transforms    []Transform          ` | @@ }`
}

func OptionScanner(blockName, namespace string, scope ...[]Option) func(needle string, dest interface{}) error {
	return func(needle string, dest interface{}) error {
		opt, ok := FindOverridableOption(needle, namespace, scope...)
		if !ok {
			return fmt.Errorf("option for block %s not found: %s", blockName, needle)
		}
		switch v := dest.(type) {
		case *float64:
			if opt.Value == nil || opt.Value.Number == nil {
				return fmt.Errorf("expected a number for option %s in block %s", needle, blockName)
			}
			*v = *opt.Value.Number
		case *string:
			if opt.Value == nil || opt.Value.Str == nil {
				return fmt.Errorf("expected a string for option %s in block %s", needle, blockName)
			}
			*v = *opt.Value.Str
		case *bool:
			src := opt.Truthy()
			*v = src
		default:
			panic(fmt.Errorf("OptionScanner found dest of unexpected type %T in block %s", dest, blockName))
		}
		return nil
	}
}

func MaybeOptionScanner(blockName, namespace string, scope ...[]Option) func(needle string, dest interface{}) (bool, error) {
	return func(needle string, dest interface{}) (bool, error) {
		opt, ok := FindOverridableOption(needle, namespace, scope...)
		if !ok {
			return false, nil
		}
		switch v := dest.(type) {
		case *float64:
			if opt.Value == nil || opt.Value.Number == nil {
				return true, fmt.Errorf("expected a number for option %s in block %s", needle, blockName)
			}
			*v = *opt.Value.Number
		case *string:
			if opt.Value == nil || opt.Value.Str == nil {
				return true, fmt.Errorf("expected a string for option %s in block %s", needle, blockName)
			}
			*v = *opt.Value.Str
		case *bool:
			src := opt.Truthy()
			*v = src
		default:
			panic(fmt.Errorf("OptionScanner found dest of unexpected type %T in block %s", dest, blockName))
		}
		return true, nil
	}
}

//String returns the option value as a string. The boolean return parameter
//will be true if the option was a string and false otherwise.
func (opt Option) String() (string, bool) {
	if opt.Value != nil && opt.Value.Str != nil {
		return *opt.Value.Str, true
	}
	return "", false
}

//StrToOpts converts an option string of the form Key1:Val1,Key2:Val2
//into a slice of Options.
func StrToOpts(s string) ([]Option, error) {
	var (
		ret     []Option
		cliOpts map[string]interface{}
	)
	err := json.Unmarshal([]byte(s), &cliOpts)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON parameters: %v", err)
	}

	for k, v := range cliOpts {
		var o Option
		o.Key = k
		switch val := v.(type) {
		case float64:
			o.Value = &OptionValue{
				Number: &val,
			}
		case int:
			vf := float64(val)
			o.Value = &OptionValue{
				Number: &vf,
			}
		case string:
			o.Value = &OptionValue{
				Str: &val,
			}
		default:
			return nil, fmt.Errorf("expected key-value option with the value either JSON number of string: %v", v)
		}
		ret = append(ret, o)
	}
	return ret, nil
}

//Truthy returns whether an option value is truthy.
// Non-zero numbers are truthy and case-insensitive variants of 'true' are truthy.
// All other strings and numbers are falsy.
func (opt Option) Truthy() bool {
	if opt.Value == nil {
		return false
	}
	if opt.Value.Str != nil {
		if strings.ToUpper(*opt.Value.Str) == "TRUE" {
			return true
		}
		return false
	}
	if opt.Value.Number != nil {
		if *opt.Value.Number == 0 {
			return false
		}
		return true
	}
	panic("should be unreachable")
}

//ParseExcelRange parses a range of the form 'A1:C4' with possible wildcards
//such as 'A1:*4'
func ParseExcelRange(s string) (x1 int, x2 *int, y1 int, y2 *int, err error) {
	ps := strings.Split(s, ":")

	if len(ps) != 2 {
		err = fmt.Errorf("expected separator ':' in range '%s'", s)
		return
	}
	ps[0] = strings.TrimSpace(ps[0])
	ps[1] = strings.TrimSpace(ps[1])

	x1, y1, err = parseCell(ps[0])

	if err != nil {
		return
	}

	x2, y2, err = parseCellWithWildcards(ps[1])

	return

}

func parseCell(s string) (x, y int, err error) {
	var (
		col string
		i   int
		r   rune
	)
	for i, r = range s {
		if unicode.IsLetter(r) {
			col += string(r)
			break
		}
	}

	if i == len(s)-1 {
		return 0, 0, fmt.Errorf("expected row number in range %s", s)
	}

	x = xlsx.TitleToNumber(col) + 1

	y, err = strconv.Atoi(s[i+1:])

	return
}

func parseCellWithWildcards(s string) (x, y *int, err error) {
	var (
		col         string
		i           int
		r           rune
		wildcardCol bool
	)
	for i, r = range s {
		if r == '*' {
			wildcardCol = true
			break //wildcard => x is nil
		}
		if unicode.IsLetter(r) {
			col += string(r)
			break
		}
	}

	if i == len(s)-1 {
		return nil, nil, fmt.Errorf("expected row number in range %s", s)
	}

	if !wildcardCol {
		xx := xlsx.TitleToNumber(col) + 1
		x = &xx
	}

	if s[i+1:] == "*" {
		return //wildcard row => y also nil
	}

	yy, errI := strconv.Atoi(s[i+1:])

	y = &yy
	err = errI
	return
}

//ParseExcelRange parses a range of the form '[x1,x2]:[y1,y2]'
//TODO: Rewrite this in a more efficient and maintainable way.
func parseExcelRange_Old(s string) (x1 int, x2 *int, y1 int, y2 *int, err error) {
	ps := strings.Split(s, ":")

	if len(ps) != 2 {
		err = fmt.Errorf("expected separator ':' in range '%s'", s)
		return
	}

	p1 := strings.Split(ps[0], ",")

	if len(p1) != 2 {
		err = fmt.Errorf("expected first point of range to be separated by ',': %s", s)
		return
	}

	p2 := strings.Split(ps[1], ",")

	if len(p1) != 2 {
		err = fmt.Errorf("expected second point of range to be separated by ',': %s", s)
		return
	}

	p1[0] = strings.TrimSpace(p1[0])
	p1[1] = strings.TrimSpace(p1[1])
	p2[0] = strings.TrimSpace(p2[0])
	p2[1] = strings.TrimSpace(p2[1])

	if p1[0][0] != '[' || p2[0][0] != '[' {
		err = fmt.Errorf("expected '[' in range %s", s)
		return
	}
	if p1[1][len(p1[1])-1] != ']' || p2[1][len(p2[1])-1] != ']' {
		err = fmt.Errorf("expected ']' in range %s", s)
	}

	//Get rid of [ and ]
	p1[0] = p1[0][1:]
	p2[0] = p2[0][1:]
	p1[1] = p1[1][0 : len(p1[1])-1]
	p2[1] = p2[1][0 : len(p2[1])-1]

	x1, err = strconv.Atoi(p1[0])

	if err != nil {
		return
	}

	y1, err = strconv.Atoi(p1[1])

	if err != nil {
		return
	}

	//not N for x2
	if len(p2[0]) != 1 || (p2[0][0] != 'N' && p2[0][0] != 'n') {
		var xx2 int
		xx2, err = strconv.Atoi(p2[0])

		if err != nil {
			return
		}
		x2 = &xx2
	}

	//not N for y2
	if len(p2[1]) != 1 || (p2[1][0] != 'N' && p2[1][0] != 'n') {
		var yy2 int
		yy2, err = strconv.Atoi(p2[1])

		if err != nil {
			return
		}
		y2 = &yy2
	}

	return
}

//FindOption traverses the slice of options and returns the one whose key matches the needle.
//The search is case-insensitive.
//The second argument indicates whether the needle was found or not.
func FindOption(options []Option, needle string) (*Option, bool) {
	n := strings.ToLower(needle)
	for _, opt := range options {
		if strings.ToLower(opt.Key) == n {
			return &opt, true
		}
	}
	return nil, false
}

//FindOverridableOption searches for the needle in the option hierarchy, in the
//order that they are given, first looking for the namespaced option and then
//the generic. The first found option is returned. For example:
// Looking for SHEET option given QUERY options and CONN options, connection 'ExcelA',
// would be accomplished with FindOverridableOption("SHEET", "ExcelA", query.Options, conn.Options)
func FindOverridableOption(needle string, namespace string, hierarchy ...[]Option) (*Option, bool) {
	for _, opts := range hierarchy {
		var (
			opt *Option
			ok  bool
		)
		//First, try destination-specific override
		opt, ok = FindOption(opts, namespace+"_"+needle)

		if ok {
			return opt, ok
		}

		//Next, try global override
		if !ok {
			opt, ok = FindOption(opts, needle)
		}

		if ok {
			return opt, ok
		}
	}

	return nil, false
}

func (b *JobScript) EvaluateParametrizedContent(globals []Option) error {
	var err error
	for i := range b.Queries {
		b.Queries[i].Content, err = evaluateContent(b.Queries[i].Content, b.Queries[i].Options, globals)
		if err != nil {
			return err
		}
	}

	for i := range b.Transforms {
		b.Transforms[i].Content, err = evaluateContent(b.Transforms[i].Content, b.Transforms[i].Options, globals)
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
	for i, query := range b.Execs {
		if query.Extern != nil {
			s, err := getContent(cwd, *query.Extern)
			if err != nil {
				return err
			}
			b.Queries[i].Content = s
			b.Queries[i].Extern = nil
		}
	}

	for i, script := range b.Transforms {
		if script.Extern != nil {
			s, err := getContent(cwd, *script.Extern)
			if err != nil {
				return err
			}
			b.Transforms[i].Content = s
			b.Transforms[i].Extern = nil
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
	b.Transforms = append(b.Transforms, other.Transforms...)
}

func (b *JobScript) ParseConnections() ([]Connection, error) {
	return parseConnections(b.Connections)
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
			conn.Options = append(conn.Options, o)
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
