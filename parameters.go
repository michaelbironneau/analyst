package main

import (
	"bufio"
	"fmt"
	"github.com/michaelbironneau/analyst/aql"
	"os"
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

//promptParameters prompts the user to enter parameters on STDIN.
func promptParameters(report *aql_old.Report) (map[string]string, error) {
	ret := make(map[string]string)
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Please enter each parameter when prompted, followed by the [enter] key")
	for k := range report.Parameters {
		fmt.Print(k + ": ")
		v, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		ret[k] = strings.TrimSpace(v)
	}
	return ret, nil
}
