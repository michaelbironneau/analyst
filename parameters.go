package main

import (
	"fmt"
	"strings"
)

//parseParameters parses a list of parameters "param_name:val;param_name2:param_val2..."
//If the same parameter is defined multiple times the last (right-most) definition will win out.
func parseParameters(params string) (map[string]string, error) {
	paramSlice := strings.Split(params, ";")
	ret := make(map[string]string)
	for i := range paramSlice {
		nv := strings.Split(paramSlice[i], ":")
		if len(nv) != 2 {
			return nil, fmt.Errorf("Parsing parameter %d: it should be of form 'name:value'", i)
		}
		if len(nv[0]) == 0 {
			continue
		}
		ret[nv[0]] = nv[1]
	}
	return ret, nil
}
