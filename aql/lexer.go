package aql

import (
	"fmt"
	lexer "github.com/alecthomas/participle/lexer"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
)

type tokenType rune

type Item struct {
	ID         tokenType
	LineNumber int
	Content    string
}

const (
	QUERY tokenType = iota
	TEST
	DESCRIPTION
	TRANSFORM
	FROM
	INTO
	EXTERN
	INCLUDE
	LPAREN
	RPAREN
	PAREN_BODY
	WITH
	EQUALS
	COMMA
	QUOTED_STRING
	NUMBER
	IDENTIFIER
	GLOBAL
	CONNECTION
	BLOCK
	AS
	EOF
	AFTER
	PLUGIN
	DECLARE
	USING
	PARAMETER
	CONSOLE
	SET
	EXEC
	DATA
	ASSERTIONS
)

var (
	tokenToString = map[tokenType]string{QUERY: "QUERY", TEST: "TEST", DESCRIPTION: "DESCRIPTION",
		TRANSFORM: "TRANSFORM", FROM: "FROM", INTO: "INTO", EXTERN: "EXTERN", INCLUDE: "INCLUDE", LPAREN: "(",
		RPAREN: ")", PAREN_BODY: "PAREN_BODY", WITH: "WITH",
		EQUALS: "=", COMMA: ",", QUOTED_STRING: "QUOTED_STRING", IDENTIFIER: "IDENT", NUMBER: "NUMBER", GLOBAL: "GLOBAL",
		CONNECTION: "CONNECTION", BLOCK: "BLOCK", AS: "AS", AFTER: "AFTER", PLUGIN: "PLUGIN", DECLARE: "DECLARE", USING: "USING", PARAMETER: "PARAMETER",
		CONSOLE: "CONSOLE", SET: "SET", EXEC: "EXEC", DATA: "DATA", ASSERTIONS: "ASSERTIONS"}
	whitespace = regexp.MustCompile(`\s`)
	keywords   = map[tokenType]bool{TEST: true, QUERY: true, DESCRIPTION: true, TRANSFORM: true, FROM: true, INTO: true, EXTERN: true,
		INCLUDE: true, WITH: true, GLOBAL: true, CONNECTION: true, BLOCK: true, AS: true, AFTER: true, PLUGIN: true, DECLARE: true, USING: true, PARAMETER: true,
		CONSOLE: true, SET: true, EXEC: true, DATA: true, ASSERTIONS: true}
	keywordReverse = map[string]tokenType{"TEST": TEST, "QUERY": QUERY, "DESCRIPTION": DESCRIPTION, "TRANSFORM": TRANSFORM, "FROM": FROM,
		"INTO": INTO, "EXTERN": EXTERN, "INCLUDE": INCLUDE, "WITH": WITH, "GLOBAL": GLOBAL, "CONNECTION": CONNECTION, "BLOCK": BLOCK, "AS": AS, "AFTER": AFTER, "PLUGIN": PLUGIN, "DECLARE": DECLARE, "USING": USING, "PARAMETER": PARAMETER,
		"CONSOLE": CONSOLE, "SET": SET, "EXEC": EXEC, "DATA": DATA, "ASSERTIONS": ASSERTIONS}
)

type ForwardLexer struct {
	items []Item
	pos   int
}

type definition struct{}

func (d *definition) Lex(r io.Reader) lexer.Lexer {
	l := &ForwardLexer{}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(fmt.Errorf("error reading from file: %v", err))
	}
	i, err := Lex(string(b))
	if err != nil {
		panic(err)
	}
	//fmt.Println(i)
	l.items = i
	return lexer.Upgrade(l)
}

func (f *ForwardLexer) Peek() lexer.Token {
	if len(f.items) <= f.pos {
		return lexer.Token{
			Type: lexer.EOF,
		}
	}
	return lexer.Token{
		Type:  rune(f.items[f.pos].ID),
		Value: f.items[f.pos].Content,
		Pos: lexer.Position{
			Line: f.items[f.pos].LineNumber,
		},
	}
}

func (f *ForwardLexer) Next() lexer.Token {
	t := f.Peek()
	//fmt.Println(t)
	if !t.EOF() {
		f.pos++
	}
	return t
}

func (d *definition) Symbols() map[string]rune {
	m := make(map[string]rune)
	for k, v := range tokenToString {
		m[v] = rune(k)
	}
	m["EOF"] = rune(lexer.EOF)
	return m
}

func Lex(s string) ([]Item, error) {
	var (
		index        int
		lineNumber   int
		ret          []Item
		inQuot       bool
		parenDepth   int
		inParen      bool
		innerContent string
		identifier   string
	)
	lineNumber = 1
	for {
		if index >= len(s) {
			break
		}

		if s[index] == '(' && !inQuot {
			//start ( could mean nested parenthesis or start of block
			parenDepth++
			if !inParen {
				ret = append(ret, Item{LPAREN, lineNumber, "("}) //we only care about outermost parenthesis - AQL never nests but the queries or scripts could.
				inParen = true
				innerContent = "" //clear it out
			} else {
				innerContent += "("
			}
			index++
			continue
		}
		if s[index] == ')' && !inQuot {
			//end ) could mean nested parenthesis or end of block
			if !inParen {
				return nil, formatErr("Unexpected ')'", lineNumber)
			}
			if parenDepth > 1 {
				innerContent += ")"
			}
			if parenDepth == 1 {
				if len(ret) > 2 && lexableBlock(ret[len(ret)-2].ID) {
					//special case - if we are in WITH/VARIABLE/etc block, lex the options
					opts, err := Lex(innerContent)
					if err != nil {
						return nil, err
					}
					ret = append(ret, opts...)
				} else {
					//not in WITH/VARIABLE/etc block - could be eg. QUERY or SCRIPT
					ret = append(ret, Item{PAREN_BODY, lineNumber, innerContent})
				}
				ret = append(ret, Item{RPAREN, lineNumber, ")"}) //we only care about outermost parenthesis - AQL never nests but the queries or scripts could.
			}
			parenDepth--
			if parenDepth == 0 {
				inParen = false
				innerContent = "" //for good measure; not strictly speaking necessary as it's already done above
			}
			index++
			continue
		}

		if s[index:index+1] == "'" && !inParen {
			if inQuot {
				ret = append(ret, Item{QUOTED_STRING, lineNumber, innerContent})
				//ret = append(ret, Item{QUOTE, lineNumber, "'"})
				inQuot = false
				innerContent = ""
			} else {
				inQuot = true
				innerContent = "" //for good measure
				//ret = append(ret, Item{QUOTE, lineNumber, "'"})
			}
			index++
			continue
		}

		if inParen || inQuot {
			//within () or '', we don't try and parse the content
			innerContent += s[index : index+1]
			index++
			continue
		}

		//inline comment
		if s[index] == '-' && len(s) > index+1 && s[index+1] == '-' {
			index = scanInlineComment(s, index)
		}

		if len(s) > index+3 && s[index:index+3] == "/**" {
			index = scanMultilineComment(s, index)
		}

		if s[index] == ',' {
			if len(identifier) > 0 {
				_, err := strconv.ParseFloat(identifier, 64)
				if err == nil {
					ret = append(ret, Item{NUMBER, lineNumber, identifier})
				} else {
					ret = append(ret, Item{IDENTIFIER, lineNumber, identifier})
				}
				identifier = ""
			}
			ret = append(ret, Item{COMMA, lineNumber, ","})
			index++
			continue
		}

		if s[index] == '=' {
			if len(identifier) > 0 {
				_, err := strconv.ParseFloat(identifier, 64)
				if err == nil {
					ret = append(ret, Item{NUMBER, lineNumber, identifier})
				} else {
					ret = append(ret, Item{IDENTIFIER, lineNumber, identifier})
				}
				identifier = ""
			}
			ret = append(ret, Item{EQUALS, lineNumber, "="})
			index++
			continue
		}

		if s[index] == ';' && (index < len(s)-1 && isWhitespace(s, index+1)) {
			index++
		}

		if s[index] == '\n' {
			if len(identifier) > 0 {
				_, err := strconv.ParseFloat(identifier, 64)
				if err == nil {
					ret = append(ret, Item{NUMBER, lineNumber, identifier})
				} else {
					ret = append(ret, Item{IDENTIFIER, lineNumber, identifier})
				}
				identifier = ""
			}
			index++
			lineNumber++
			continue
		}
		if s[index] == '\t' || s[index] == ' ' || s[index] == '\r' || s[index] == '\f' {
			//ignore whitespace except if we are in identifier mode
			if len(identifier) > 0 {

				_, err := strconv.ParseFloat(identifier, 64)
				if err == nil {
					ret = append(ret, Item{NUMBER, lineNumber, identifier})
				} else {
					ret = append(ret, Item{IDENTIFIER, lineNumber, identifier})
				}
				identifier = ""
			}
			index++
			continue
		}

		if t, ss, ok := getKeyword(s, index); ok && len(identifier) == 0 {
			ret = append(ret, Item{t, lineNumber, ss})
			index = index + len(ss)
			continue
		}

		identifier += string(s[index])
		index++

	}
	if inParen {
		return nil, formatErr("Unclosed (", lineNumber)
	}
	if inQuot {
		return nil, formatErr("Unclosed '", lineNumber)
	}
	if len(identifier) > 0 {
		//closing identifier
		_, err := strconv.ParseFloat(identifier, 64)
		if err == nil {
			ret = append(ret, Item{NUMBER, lineNumber, identifier})
		} else {
			ret = append(ret, Item{IDENTIFIER, lineNumber, identifier})
		}
	}
	return ret, nil
}

//scanComment returns the index of the next character outside the inline comment
func scanInlineComment(s string, i int) int {
	var j = i
	for {
		if j >= len(s)-1 || s[j] == '\n' {
			break
		}
		j++
	}
	return j
}

//scanMultilineComment returns the index of the next character outside multiline comment
func scanMultilineComment(s string, i int) int {
	var j = i
	for {
		if j >= len(s)-3 || s[j:j+3] == "**/" {
			break
		}
		j++
	}
	return j + 3
}

func isWhitespace(s string, i int) bool {
	return whitespace.Match([]byte(s[i : i+1]))
}
func isDelimiter(s string, i int) bool {
	if isWhitespace(s, i) {
		return true
	}
	if s[i] == '\'' || s[i] == ',' || s[i] == '(' || s[i] == ')' || s[i] == '=' {
		return true
	}
	return false
}

func lexableBlock(t tokenType) bool {
	if t == WITH || t == PARAMETER {
		return true
	}
	return false
}

func getKeyword(s string, i int) (tokenType, string, bool) {
	l := len(s)
	for k, v := range keywordReverse {
		if len(k) > l-i {
			continue
		}
		if strings.ToUpper(s[i:i+len(k)]) == k {
			if l > len(k)+i && !isDelimiter(s, i+len(k)) {
				continue //this is an identifier that begins with a keyword, e.g. connectionString
			}
			return v, k, true
		}
	}
	return -1, "", false
}

func formatErr(msg string, line int) error {
	return fmt.Errorf("compilation error line %v: %s", line, msg)
}
