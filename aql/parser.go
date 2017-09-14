package aql

import "strings"

type Options map[string]interface{}

//parseOptions parses a string of type OPTION_NAME = 'OPTION_VALUE', ...OPTION_NAME = OPTION_VALUE
func parseOptions(s string, lineNumber int) (Options, error){
	ret :=make(map[string]interface{})
	o := strings.Split(s, ",")
	if len(o) == 0 {
		return nil, formatErr("Empty options", lineNumber)
	}

	for i := range o {
		os := strings.Split(o[i], "=")
		if len(os) != 2 {
			
		}
	}

}
