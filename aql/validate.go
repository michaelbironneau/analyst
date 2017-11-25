package aql

import "fmt"

//Validate checks integrity and consistency of the script. It returns nil if the script is
//valid and a slice of errors otherwise. All external references should be resolved, otherwise
//the result may be incomplete.
func Validate(js *JobScript) []error {
	return nil
}

//returns name table and check that each block has a unique name
func nametable(js *JobScript) (map[string]interface{}, []error) {
	var duplicates []error
	names := make(map[string]interface{})

	for _, q := range js.Queries {
		if _, ok := names[q.Name]; ok {
			duplicates = append(duplicates, fmt.Errorf("there are two blocks with name '%s'", q.Name))
		} else {
			names[q.Name] = q
		}
	}

	for _, s := range js.Scripts {
		if _, ok := names[s.Name]; ok {
			duplicates = append(duplicates, fmt.Errorf("there are two blocks with name '%s'", s.Name))
		} else {
			names[s.Name] = s
		}
	}

	for _, s := range js.Tests {
		if _, ok := names[s.Name]; ok {
			duplicates = append(duplicates, fmt.Errorf("there are two blocks with name '%s'", s.Name))
		} else {
			names[s.Name] = s
		}
	}

	for _, s := range js.Connections {
		if _, ok := names[s.Name]; ok {
			duplicates = append(duplicates, fmt.Errorf("there are two blocks with name '%s'", s.Name))
		} else {
			names[s.Name] = s
		}
	}

	for _, s := range js.Globals {
		if _, ok := names[s.Name]; ok {
			duplicates = append(duplicates, fmt.Errorf("there are two blocks with name '%s'", s.Name))
		} else {
			names[s.Name] = s
		}
	}

	return names, duplicates
}
