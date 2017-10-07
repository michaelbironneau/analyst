package aql

import (
	"regexp"
	"strings"
	"fmt"
)

type Token int

type Item struct {
	ID Token
	LineNumber int
	Content string
}

const (
	QUERY Token = iota
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
	QUOTE
	STRING
)

var (
	tokenToString = map[Token]string{QUERY: "QUERY", TEST: "TEST", SCRIPT: "SCRIPT", DESCRIPTION: "DESCRIPTION",
	TRANSFORM: "TRANSFORM", FROM: "FROM", INTO: "INTO", EXTERN: "EXTERN", INCLUDE: "INCLUDE", LPAREN: "(",
	RPAREN: ")", PAREN_BODY: "PAREN_BODY", RANGE: "RANGE", RANGE_BODY: "RANGE_BODY", WITH: "WITH",
	EQUALS: "=", COMMA: ",", QUOTE: "'", STRING: "STRING"}
	whitespace = regexp.MustCompile(`\s`)
	keywords = map[Token]bool{TEST:true, QUERY: true, SCRIPT: true, DESCRIPTION: true, TRANSFORM: true, FROM: true, INTO: true, EXTERN: true,
	INCLUDE: true, RANGE: true, WITH: true}
	keywordReverse = map[string]Token{"TEST": TEST, "QUERY": QUERY, "SCRIPT": SCRIPT, "DESCRIPTION": DESCRIPTION, "TRANSFORM": TRANSFORM, "FROM": FROM,
	"INTO": INTO, "EXTERN": EXTERN, "INCLUDE": INCLUDE, "RANGE": RANGE, "WITH": WITH}
)

//Lex lexes an AQL script to produce a slice of tokens. If it encounters an error, it will complain (loudly).
func Lex(s string) ([]Item, error) {
	var (
		index int
		lineNumber int
		ret []Item
		inQuot bool
		parenDepth int
		inParen bool
		innerContent string
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
				ret = append(ret, Item{LPAREN, lineNumber,"("}) //we only care about outermost parenthesis - AQL never nests but the queries or scripts could.
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
				ret = append(ret, Item{PAREN_BODY, lineNumber,innerContent})
				ret = append(ret, Item{RPAREN, lineNumber,")"}) //we only care about outermost parenthesis - AQL never nests but the queries or scripts could.
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
				ret = append(ret, Item{STRING, lineNumber,innerContent})
				ret = append(ret, Item{QUOTE, lineNumber,"'"})
				inQuot = false
				innerContent = ""
			} else {
				inQuot = true
				innerContent = "" //for good measure
				ret = append(ret, Item{QUOTE, lineNumber,"'"})
			}
			index++
			continue
		}

		if inParen || inQuot {
			//within () or '', we don't try and parse the content
			innerContent += s[index:index+1]
			index++
			continue
		}

		if s[index] == ',' {
			ret = append(ret, Item{COMMA,  lineNumber,","})
			index++
			continue
		}

		if s[index] == '=' {
			ret = append(ret, Item{EQUALS, lineNumber,"="})
			index++
			continue
		}

		if s[index] == '\n' {
			index++
			lineNumber++
			continue
		}
		if s[index] == '\t' || s[index] == ' ' || s[index] == '\r' || s[index] == '\f' {
			//ignore whitespace
			index++
			continue
		}

		if t, ss, ok := getKeyword(s, index); ok {
			ret = append(ret, Item{t, lineNumber,ss })
			index = index + len(ss)
			continue
		}

		return nil, formatErr("Invalid syntax", lineNumber)
	}
	if inParen {
		return nil, formatErr("Unclosed (", lineNumber)
	}
	if inQuot {
		return nil, formatErr("Unclosed '", lineNumber)
	}
	return ret, nil
}

func isWhitespace(s string, i int) bool {
	return whitespace.Match([]byte(s[i:i+1]))
}

func getKeyword(s string, i int) (Token, string, bool){
	l := len(s)
	for k, v := range keywordReverse {
		if len(k) > l - i {
			continue
		}
		if strings.ToUpper(s[i:i+len(k)]) == k {
			return v,k, true
		}
	}
	return -1, "", false
}

func formatErr(msg string, line int) error {
	return fmt.Errorf("compilation error line %v: %s", line, msg)
}