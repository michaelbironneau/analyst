package aql

import (
	"fmt"
)

//Parameter is a template parameter.
type Parameter struct {
	Type  string
	Value interface{}
}

//Report represents a templated Excel report.
type Report struct {
	Name         string
	Description  string
	TemplateFile string
	TempTables map[string]bool
	OutputFile   string
	Parameters   map[string]Parameter
	Connections  map[string]string
	Queries      map[string]Query
}

//ValidateAndConvert validates the parsed report and converts it to the more useful Report struct.
func (r *report) ValidateAndConvert() (*Report, error) {
	var (
		ret  Report
		errs []error
	)
	ret.Parameters = make(map[string]Parameter)
	ret.Connections = make(map[string]string)
	ret.Queries = make(map[string]Query)
	errs = append(errs, processMetadata(r, &ret))
	errs = append(errs, processTempTables(r, &ret))
	errs = append(errs, processParameters(r, &ret))
	errs = append(errs, processConnections(r, &ret))
	errs = append(errs, processQueries(r, &ret))
	err := concatenateErrors(errs)
	return &ret, err
}

func concatenateErrors(errs []error) error {
	var s string
	for i := range errs {
		if errs[i] == nil {
			continue
		}
		s += errs[i].Error() + "\n"
	}
	if len(s) == 0 {
		return nil
	}
	return fmt.Errorf(s)
}

//processTempTables generates a map of temp tables so they can be used to match 
//later on.
func processTempTables(source *report, dest *Report) error {
	dest.TempTables = make(map[string]bool)
	for i := range source.queries {
		if source.queries[i].Range.TempTable != nil {
			dest.TempTables[source.queries[i].Range.TempTable.Name] = true
		}
	}
	return nil	
}

//processQueries performs compile-time validation of queries. The requirements are:
//
//  1) Query name should be unique
//  2) Connection name should be defined (so processQueries() should run after processConnections() )
func processQueries(source *report, dest *Report) error {
	for _, q := range source.queries {
		if _, ok := dest.Queries[q.Name]; ok {
			return fmt.Errorf("Query '%s is not unique", q.Name)
		}
		if _, ok := dest.Connections[q.Source]; !ok {
			if _, ok2 := dest.TempTables[q.Source]; !ok2 {
				return fmt.Errorf("Connection/Temp table '%s' not found for query '%s'", q.Source, q.Name)
			}
		}
		dest.Queries[q.Name] = q
	}
	return nil
}

//processConnections performs compile-time validation of connections.
//The only requirement is for connections to have a unique name.
func processConnections(source *report, dest *Report) error {
	for _, c := range source.connections {
		if _, ok := dest.Connections[c.Name]; ok {
			return fmt.Errorf("Connection '%s' is not unique", c.Name)
		}
		dest.Connections[c.Name] = c.File
	}
	return nil
}

//processParameters performs compile-time validation of parameters according to the following rules:
//
//  1) Parameter names should be unique
//  2) Parameters should have one of the following types:
//      A) string
//      B) number
//      C) date
func processParameters(source *report, dest *Report) error {
	for _, p := range source.parameters {
		if _, ok := dest.Parameters[p.Name]; ok {
			return fmt.Errorf("Parameter '%s' is not unique", p.Name)
		}
		switch p.Type {
		case "string", "number", "date":
			dest.Parameters[p.Name] = Parameter{
				Type: p.Type,
			}
		default:
			return fmt.Errorf("Unknown parameter type '%s'", p.Type)
		}
	}
	return nil
}

//processMetadata performs compile-time validation of metadata according to the following rules:
//
//  1) Report name should be present, unique, and fewer than 64 characters
//  2) Report description is optional but should be unique and fewer than 128 characters
//  3) Template should be present and unique
//  4) Output should be present and unique
//  5) Permission is optional but should be unique
//At this stage we're ignoring blocks with unknown keywords - they should have been caught by the parser.
func processMetadata(source *report, dest *Report) error {
	var (
		haveReport   bool
		haveDesc     bool
		haveTemplate bool
		haveOutput   bool
	)

	for i := range source.metadata {
		switch source.metadata[i].Type {
		case "report":
			if haveReport {
				return fmt.Errorf("Duplicate 'report' blocks")
			}
			if len(source.metadata[i].Data) > 64 {
				return fmt.Errorf("Report name is too long, it should be at most 64 characters.")
			}
			dest.Name = source.metadata[i].Data
		case "description":
			if haveDesc {
				return fmt.Errorf("Duplicate 'description' blocks")
			}
			if len(source.metadata[i].Data) > 128 {
				return fmt.Errorf("Report description is too long, it should be at most 128 characters.")
			}
			dest.Description = source.metadata[i].Data
		case "template":
			if haveTemplate {
				return fmt.Errorf("Duplicate 'template' blocks")
			}
			dest.TemplateFile = source.metadata[i].Data
		case "output":
			if haveOutput {
				return fmt.Errorf("Duplicate 'output' blocks")
			}
			dest.OutputFile = source.metadata[i].Data
		}
	}
	return nil

}

//Load parses and validates the script content into a Report struct.
func Load(script string) (*Report, error) {
	r, err := Parse(script)

	if err != nil {
		return nil, err
	}

	return r.ValidateAndConvert()
}
