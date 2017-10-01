package aql

import (
	"strings"
	"fmt"
	"encoding/json"
	"os"
	"net/url"
	"math"
)

type Options map[string]interface{}

type SourceSinkType int

const (
	Global SourceSinkType = iota
	SQL
	Excel
	Script
)

const RangeWildcard int = math.MinInt64

type Range struct {
	X0 int
	X1 int
	Y0 int
	Y1 int
}

type SourceSinkDef struct {
	Type SourceSinkType
	ConnectionName string  //Name of source in conn files, if SQL source
	Filename string //For Excel or Script sources
	Range Range //For Excel
}

type SourceSink []SourceSinkDef

var BlockStarters = map[Token]bool{TEST:true, QUERY: true, SCRIPT: true, DESCRIPTION: true, INCLUDE: true}


func parse(it []Item) ([]Block, error) {
	if len(it) == 0 {
		return nil, nil
	}
	var (
		currentKeyword Token
		currentBlock Block
		haveFirstBlock bool
		blockStart int
		ret []Block
		maxIndex = len(it)
		i Item
		index int
	)

	for  {
		i = it[index]
		if BlockStarters[i.ID] {
			//new Keyword
			currentKeyword = i.ID
			if !haveFirstBlock {
				index++
				haveFirstBlock = true
				continue
			}
			err := validate(currentBlock, blockStart)
			if err != nil {
				return nil, err
			}
			ret = append(ret, currentBlock)
			blockStart = i.LineNumber
			currentBlock = Block{}
		} else if !haveFirstBlock {
			return nil, formatErr("script should start with a keyword", 1)
		}

		switch currentKeyword {
		case QUERY:
			b, n, err := parseQueryBlock(it, index)
			if err != nil {
				return nil, err
			}
			ret = append(ret, b)
			if n + 1 < maxIndex {
				index = n+1
			}
		case TEST:
			b, n, err := parseTestBlock(it, index)
			if err != nil {
				return nil, err
			}
			ret = append(ret, b)
			if n + 1 < maxIndex {
				index = n+1
			}

		case SCRIPT:
			b, n, err := parseScriptBlock(it, index)
			if err != nil {
				return nil, err
			}
			ret = append(ret, b)
			if n + 1 < maxIndex {
				index = n+1
			}

		case DESCRIPTION:
			b, n, err := parseDescriptionBlock(it, index)
			if err != nil {
				return nil, err
			}
			ret = append(ret, b)
			if n + 1 < maxIndex {
				index = n+1
			}

		case INCLUDE:

		default:
			panic("left hand is not talking to right hand!")
		}

		index++
	}
}

func parseQueryBlock(it []Item, currIndex int) (b Block, newIndex int, err error){
	return Block{}, currIndex, nil
}


func parseTestBlock(it []Item, currIndex int) (b Block, newIndex int, err error){
	return Block{}, currIndex, nil
}


func parseDescriptionBlock(it []Item, currIndex int) (b Block, newIndex int, err error){
	return Block{}, currIndex, nil
}


func parseScriptBlock(it []Item, currIndex int) (b Block, newIndex int, err error){
	return Block{}, currIndex, nil
}

func validate(b Block, lineNumber int) error {
	if len(b.Name) == 0 {
		return formatErr("no name for block", lineNumber)
	}
	if b.Content == nil {
		return formatErr("empty block", lineNumber)
	}
	switch b.Type {
	case QueryTransform:

	case ScriptTransform:
	case Source:
		fallthrough
	case Sink:
		return validateSourceSinkDef(b, lineNumber)
	}
	return nil
}

func validateSourceSinkDef(b Block, lineNumber int) error {
	h, ok := b.Header.(SourceSink)
	if !ok {
		return formatErr("expecting source/sink definitions", lineNumber)
	}
	for i := range h {
		switch h[i].Type {
		case SQL:
			if h[i].ConnectionName == "" {
				return formatErr(fmt.Sprintf("unknown connection %s", h[i].ConnectionName), lineNumber)
			}
		case Excel:
			if h[i].Range.X0 == RangeWildcard  && h[i].Range.X1 == RangeWildcard {
				return formatErr("at most one of x0 and x1 can be set to N", lineNumber)
			}
			if h[i].Range.Y0 == RangeWildcard  && h[i].Range.Y1 == RangeWildcard {
				return formatErr("at most one of y0 and y1 can be set to N", lineNumber)
			}
			if h[i].Filename == "" {
				return formatErr("filename must be specified for Excel source/sink", lineNumber)
			}
			fallthrough
		case Script:
			if h[i].Filename != "" {
				//option 1: it's a URL
				if _, err := url.ParseRequestURI(h[i].Filename); err == nil {
					break
				}
				//option 2: it's a file
				if _, err := os.Stat(h[i].Filename); err != nil {
					return formatErr(fmt.Sprintf("error accessing script %v", err), lineNumber)
				}
			}
		case Global:
			//automatically valid
			return nil
		}
	}
	return nil
}


//parseOptions parses a string of type OPTION_NAME = 'OPTION_VALUE', ...OPTION_NAME = OPTION_VALUE
func parseOptions(s string, lineNumber int) (Options, error){
	ret :=make(map[string]interface{})
	o := strings.Split(s, ",")
	if len(o) == 0 {
		return nil, formatErr("empty options", lineNumber)
	}

	for i := range o {
		os := strings.Split(o[i], "=")
		if len(os) != 2 {
			return nil, formatErr("option missing '=' sign", lineNumber)
		}
		oName := strings.TrimSpace(os[0])
		if _, exists := ret[oName]; exists {
			return nil, formatErr(fmt.Sprintf("option '%s' already declared", oName), lineNumber)
		}
		oVal, err := inferType(os[1], lineNumber)
		if err != nil {
			return nil, err
		}
		ret[oName] = oVal
	}
	return ret, nil
}

//inferType infers the type of a string with no trailing whitespace
func inferType(s string, lineNumber int) (interface{}, error) {
	var i interface{}
	err := json.Unmarshal([]byte(s), &i)
	if err != nil {
		return nil, formatErr(fmt.Sprintf("invalid syntax for options: %v", err), lineNumber)
	}
	return i, nil
}
