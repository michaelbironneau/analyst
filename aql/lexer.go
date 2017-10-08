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
	SCRIPT
	DESCRIPTION
	TRANSFORM
	FROM
	INTO
	EXTERN
	INCLUDE
	LPAREN
	RPAREN
	PAREN_BODY
	RANGE
	RANGE_BODY
	WITH
	EQUALS
	COMMA
	QUOTED_STRING
	NUMBER
	IDENTIFIER
	GLOBAL
	CONNECTION
	BLOCK
	EOF
)

var (
	tokenToString = map[tokenType]string{QUERY: "QUERY", TEST: "TEST", SCRIPT: "SCRIPT", DESCRIPTION: "DESCRIPTION",
		TRANSFORM: "TRANSFORM", FROM: "FROM", INTO: "INTO", EXTERN: "EXTERN", INCLUDE: "INCLUDE", LPAREN: "(",
		RPAREN: ")", PAREN_BODY: "PAREN_BODY", RANGE: "RANGE", RANGE_BODY: "RANGE_BODY", WITH: "WITH",
		EQUALS: "=", COMMA: ",", QUOTED_STRING: "QUOTED_STRING", IDENTIFIER: "IDENT", NUMBER: "NUMBER", GLOBAL: "GLOBAL",
		CONNECTION: "CONNECTION", BLOCK: "BLOCK"}
	whitespace = regexp.MustCompile(`\s`)
	keywords   = map[tokenType]bool{TEST: true, QUERY: true, SCRIPT: true, DESCRIPTION: true, TRANSFORM: true, FROM: true, INTO: true, EXTERN: true,
		INCLUDE: true, RANGE: true, WITH: true, GLOBAL: true, CONNECTION: true, BLOCK: true}
	keywordReverse = map[string]tokenType{"TEST": TEST, "QUERY": QUERY, "SCRIPT": SCRIPT, "DESCRIPTION": DESCRIPTION, "TRANSFORM": TRANSFORM, "FROM": FROM,
		"INTO": INTO, "EXTERN": EXTERN, "INCLUDE": INCLUDE, "RANGE": RANGE, "WITH": WITH, "GLOBAL": GLOBAL, "CONNECTION": CONNECTION, "BLOCK": BLOCK}
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
		panic(err)
	}
	i, err := Lex(string(b))
	if err != nil {
		panic(err)
	}
	fmt.Println(i)
	l.items = i
	return l
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
	fmt.Println(t)
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
				if len(ret) > 2 && ret[len(ret)-2].ID == WITH {
					//special case - if we are in WITH block, lex the options
					opts, err := Lex(innerContent)
					if err != nil {
						return nil, err
					}
					ret = append(ret, opts...)
				} else {
					//not in WITH block - could be eg. QUERY or SCRIPT
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

		if s[index] == ',' {
			ret = append(ret, Item{COMMA, lineNumber, ","})
			index++
			continue
		}

		if s[index] == '=' {
			ret = append(ret, Item{EQUALS, lineNumber, "="})
			index++
			continue
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

		if t, ss, ok := getKeyword(s, index); ok {
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

func isWhitespace(s string, i int) bool {
	return whitespace.Match([]byte(s[i : i+1]))
}

func getKeyword(s string, i int) (tokenType, string, bool) {
	l := len(s)
	for k, v := range keywordReverse {
		if len(k) > l-i {
			continue
		}
		if strings.ToUpper(s[i:i+len(k)]) == k {
			return v, k, true
		}
	}
	return -1, "", false
}

func formatErr(msg string, line int) error {
	return fmt.Errorf("compilation error line %v: %s", line, msg)
}
