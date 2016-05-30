package aql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)

type SourceType int

const (
	FromConnection SourceType = iota
	FromTempTable
)

type metadataBlock struct {
	Type string
	Data string
}

type parameterBlock struct {
	Name string
	Type string
}

type TempTableDeclaration struct {
	Name    string
	Columns string
}

type QueryRange struct {
	Sheet     string
	TempTable *TempTableDeclaration
	X1        interface{}
	X2        interface{}
	Y1        interface{}
	Y2        interface{}
}

type connection struct {
	Name string
	File string
}

type Query struct {
	Name       string
	Source     string
	SourceType SourceType
	Statement  string
	Range      QueryRange
}

type report struct {
	metadata    []metadataBlock
	parameters  []parameterBlock
	connections []connection
	queries     []Query
}

//parseQuery parses a query block. Query blocks look like this:
//
//  query '{NAME}' from {CONNECTION NAME} (
//          {QUERY CONTENT}
//          {QUERY CONTENT}
//  ) into sheet '{SHEET NAME}' range [{X1}, {Y1}]:[{X2},{Y2}]
// where X1,Y1 are integers and X2, Y2 are either integers or 'n'. At most one of X2/Y2 can be 'n'.
func parseQuery(block []string, keyword string, keywordEnd int) (*Query, error) {

	if len(block) < 3 {
		return nil, fmt.Errorf("Query block has invalid structure - should have at least 3 lines")
	}

	var (
		ret            Query
		retRange       QueryRange
		validFirstLine = regexp.MustCompile("(?i)^[[:space:]]*query[[:space:]]*'([[:alnum:]]+)'[[:space:]]+from[[:space:]]([[:alnum:]]+)[[:space:]]*\\($")
		excelLastLine  = regexp.MustCompile("^(?i)[[:space:]]*\\)[[:space:]]+into[[:space:]]+sheet[[:space:]]+'([[:ascii:]]*)'[[:space:]]+range[[:space:]]*\\[([0-9]+)\\,[[:space:]]*([0-9]+)\\]\\:\\[([0-9n]+)\\,[[:space:]]*([0-9n]+)\\][[:space:]]*$")
		tempDBLastLine = regexp.MustCompile("^(?i)[[:space:]]*\\)[[:space:]]+into[[:space:]]+table[[:space:]]+([[:alnum:]]+)[[:space:]]+(\\(.*\\))[[:space:]]*$")
	)

	first := validFirstLine.FindAllStringSubmatch(block[0], -1)

	if len(first) != 1 {
		return nil, fmt.Errorf("Syntax error in first line of block")
	}

	ret.Name = first[0][1]
	ret.Source = first[0][2]

	last := excelLastLine.FindAllStringSubmatch(block[len(block)-1], -1)
	last2 := tempDBLastLine.FindAllStringSubmatch(block[len(block)-1], -1)
	if len(last) != 1 && len(last2) != 1 {
		return nil, fmt.Errorf("Syntax error in last line of block")
	}
	
	
	//at most one of last and last2 can be matched so only one of the bodies of
	//the following blocks will be reached
	
	if len(last2) == 1 {
		ret.Range.TempTable = &TempTableDeclaration{
			Name:    last2[0][1],
			Columns: last2[0][2],
		}
	}

	if len(last) == 1 {
		//error return can be discarded in these as regex above has already validated them as digits
		retRange.Sheet = last[0][1]
		retRange.X1, _ = strconv.Atoi(last[0][2])
		retRange.Y1, _ = strconv.Atoi(last[0][3])
		var haveOne bool

		if last[0][4] == "n" || last[0][4] == "N" {
			retRange.X2 = "n"
			haveOne = true
		} else {
			retRange.X2, _ = strconv.Atoi(last[0][4])
		}

		if last[0][5] == "n" || last[0][5] == "N" {
			retRange.Y2 = "n"
			if haveOne {
				return nil, fmt.Errorf("At most one of x3 and x4 can be set to 'n'")
			}
		} else {
			retRange.Y2, _ = strconv.Atoi(last[0][5])
		}
		ret.Range = retRange
	}

	for i := 1; i < len(block)-1; i++ {
		ret.Statement += block[i] + "\n"
	}
	ret.Statement = ret.Statement[0 : len(ret.Statement)-1] //strip trailing newline character
	if _, err := template.New("t").Parse(ret.Statement); err != nil {
		return nil, fmt.Errorf("Error parsing template in query: %v", err)
	}
	
	return &ret, nil
}

//parseMetadataBlock parses a metadata block. Metadata blocks look like this:
//
//  KEYWORD '{CONTENT}'
func parseMetadataBlock(block []string, keyword string, keywordEnd int) (*metadataBlock, error) {
	if len(block) > 1 {
		return nil, fmt.Errorf("Metadata block should be on a single line")
	}

	var validContent = regexp.MustCompile("'.*'[[:space:]]*$")

	contentMatch := validContent.FindAllString(block[0], 1)

	switch len(contentMatch) {
	case 1:
		d := contentMatch[0][1 : len(contentMatch[0])-1]
		if _, err := template.New("m").Parse(d); err != nil {
			return nil, fmt.Errorf("Error parsing template in metadata content: %v", err)
		}
		return &metadataBlock{
			Type: keyword,
			Data: d, //exclude leading and trailing '
		}, nil
	default:
		return nil, fmt.Errorf("Invalid block: should have syntax \"KEYWORD\" 'CONTENT'")
	}
}

//parseConnectionBlock parses a connection block. Connection blocks look like this:
//
// connection {CONNECTION NAME} '{CONNECTION FILE}'
//
// or
//
// connection (
//    {CONNECTION NAME} '{CONNECTION FILE}'
//      ...
//)
func parseConnectionBlock(block []string, keyword string, keywordEnd int) ([]connection, error) {
	validConnBlock := regexp.MustCompile("^[[:space:]]*([[:alnum:]]+)[[:space:]]+'([[:ascii:]]+)'[[:space:]]*\\(*[[:space:]]*$")

	var parsedBlocks []connection
	for i := range block {
		var content [][][]byte
		if i == 0 {
			b := block[i][keywordEnd:len(block[i])]
			if strings.TrimSpace(b) == "(" {
				continue //multi-line block
			}
			content = validConnBlock.FindAllSubmatch([]byte(b), -1)
		} else {
			content = validConnBlock.FindAllSubmatch([]byte(block[i]), -1)
		}

		switch {
		case len(content) == 0 && (i == len(block)-1):
			//last line in block will just have )
			if strings.TrimSpace(block[i]) == ")" {
				break
			} else {
				return nil, fmt.Errorf("Invalid block line, expecting ')': '%s'", block[i])
			}
		case len(content) == 1:
			if len(content[0]) == 3 {
				parsedBlocks = append(parsedBlocks, connection{
					Name: strings.ToLower(string(content[0][1])),
					File: string(content[0][2]),
				})
			} else {
				return nil, fmt.Errorf("Syntax error in connection block near '%s'", block[i])
			}
		default:
			return nil, fmt.Errorf("Invalid connection block line '%s'", block[i])
		}

	}
	return parsedBlocks, nil

}

//parseParameterBlock parses a parameter block. Parameter blocks look like this:
//
// parameter {PARAMETER NAME} {PARAMETER TYPE}
//
// or
//
// parameter (
//    {PARAMETER NAME} {PARAMETER TYPE}
//      ...
//)
func parseParameterBlock(block []string, keyword string, keywordEnd int) ([]parameterBlock, error) {
	validParamBlock := regexp.MustCompile("^[[:space:]]*([[:alnum:]]+)[[:space:]]+([[:alnum:]]+)[[:space:]]*\\(*[[:space:]]*$")

	var parsedBlocks []parameterBlock
	for i := range block {
		var content [][][]byte
		if i == 0 {
			b := block[i][keywordEnd:len(block[i])]
			if strings.TrimSpace(b) == "(" {
				continue //multi-line block
			}
			content = validParamBlock.FindAllSubmatch([]byte(b), -1)
		} else {
			content = validParamBlock.FindAllSubmatch([]byte(block[i]), -1)
		}

		switch {
		case len(content) == 0 && (i == len(block)-1):
			//last line in block will just have )
			if strings.TrimSpace(block[i]) == ")" {
				break
			} else {
				return nil, fmt.Errorf("Invalid block line, expecting ')': '%s'", block[i])
			}
		case len(content) == 1:
			if len(content[0]) == 3 {
				parsedBlocks = append(parsedBlocks, parameterBlock{
					Name: capitalize(string(content[0][1])),
					Type: strings.ToLower(string(content[0][2])),
				})
			} else {
				return nil, fmt.Errorf("Syntax error in parameter block near '%s'", block[i])
			}
		default:
			//should never get reached
			return nil, fmt.Errorf("Invalid parameter block line '%s'", block[i])
		}

	}
	return parsedBlocks, nil

}

//capitalize converts the word to Title case (capitalize the first letter, lowercase the rest)
func capitalize(word string) string {
	if len(word) == 1 {
		return strings.ToUpper(word)
	}
	return strings.ToUpper(string(word[0])) + strings.ToLower(word[1:len(word)])
}

//getBlockType gets the block type and start of the block content (after keyword)
func getBlockType(block []string) (string, int, error) {

	if len(block) == 0 {
		return "", 0, fmt.Errorf("Empty block")
	}

	firstLine := block[0]

	//determine block type
	//the block type is determined by the first word in the line
	var keywordStart int
	var i int
	keywordStart = -1
	for i = range firstLine {
		if firstLine[i] == ' ' && keywordStart > -1 {
			break
		} else if firstLine[i] != ' ' && keywordStart == -1 {
			keywordStart = i
		}
	}

	if keywordStart == -1 {
		return "", 0, fmt.Errorf("Failed to get block type")
	}
	if keywordStart == len(firstLine) {
		return "", 0, fmt.Errorf("Expected a space after block type keyword")
	}
	blockKeyword := strings.ToLower(firstLine[keywordStart:i])
	return blockKeyword, i, nil
}

//splitBlocks splits a script into line-split blocks
func splitBlocks(script string) ([][]string, error) {
	lines := strings.Split(script, "\n")

	var (
		ret          [][]string
		currentBlock []string
		inOpenBlock  bool
	)
	for i := range lines {
		line := strings.TrimSpace(lines[i])

		if len(line) == 0 {
			continue
		}

		//classic block:
		//      connections (
		//          g3 'g3.conn'
		//      )
		//
		//range block:
		//      query 'name' from azure (
		//          SELECT 1
		//      ) into range [0,0]:[0,1]
		switch {
		case line[len(line)-1] == '(':
			//both classic and range blocks are opened in same way
			if inOpenBlock {
				return nil, fmt.Errorf("Line %d: Unclosed block, expecting )", i)
			}
			inOpenBlock = true
			currentBlock = []string{line}
		case line[len(line)-1] == ')' || line[0] == ')':
			//range block closed at start of line; classic at end of line
			if !inOpenBlock {
				return nil, fmt.Errorf("Line %d: Unexpected character ')' - there is no block to close.", i)
			}
			currentBlock = append(currentBlock, line)
			ret = append(ret, currentBlock)
			inOpenBlock = false
		default:
			if inOpenBlock {
				currentBlock = append(currentBlock, line)
			} else {
				//single line block
				ret = append(ret, []string{lines[i]})
			}
		}
	}
	return ret, nil
}

//Parse parses the script into a report structure
func Parse(script string) (*report, error) {
	blocks, err := splitBlocks(script)

	if err != nil {
		return nil, err
	}

	var ret report
	for i := range blocks {
		keyword, keywordStop, err := getBlockType(blocks[i])

		if err != nil {
			return nil, fmt.Errorf("Error reading block %d: %v", i+1, err.Error())
		}

		switch keyword {
		case "report", "description", "template", "output":
			//metadata blocks
			bl, err := parseMetadataBlock(blocks[i], keyword, keywordStop)

			if err != nil {
				return nil, fmt.Errorf("Error reading metadata block %d: %v", i+1, err.Error())
			}

			ret.metadata = append(ret.metadata, *bl)
		case "parameter":
			//parameter blocks
			bl, err := parseParameterBlock(blocks[i], keyword, keywordStop)

			if err != nil {
				return nil, fmt.Errorf("Error reading parameter block %d: %v", i+1, err.Error())
			}

			ret.parameters = append(ret.parameters, bl...)

		case "connection":
			//connection block
			bl, err := parseConnectionBlock(blocks[i], keyword, keywordStop)

			if err != nil {
				return nil, fmt.Errorf("Error reading connection block %d: %v", i+1, err.Error())
			}

			ret.connections = append(ret.connections, bl...)

		case "query":
			//query block
			bl, err := parseQuery(blocks[i], keyword, keywordStop)

			if err != nil {
				return nil, fmt.Errorf("Error reading query block %d: %v", i+1, err.Error())
			}

			ret.queries = append(ret.queries, *bl)
		default:
			return nil, fmt.Errorf("Unknown block type '%s'", keyword)
		}

	}
	return &ret, nil
}
