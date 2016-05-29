package aql

import (
	"bytes"
	"fmt"
	"text/template"
)

//Execute takes a report with populated parameter values and executes the templates of all template-enabled fields. Panics if a template doesn't compile.
//
//  1) Metadata fields
//      - Template
//      - Output file
//  2) Queries
//
//Returns a Report with the templates executed.
func (r *Report) Execute() (*Report, error) {
	params := make(map[string]interface{})
	var ret Report

	//Fields where templates are not enabled
	ret.Name = r.Name
	ret.Description = r.Description
	ret.Parameters = r.Parameters
	ret.Connections = r.Connections
	ret.Queries = make(map[string]Query)
	for k, v := range r.Parameters {
		if v.Value == nil {
			return nil, fmt.Errorf("Value not provided for parameter '%s'", k)
		}
		params[k] = v.Value
	}

	t1 := template.Must(template.New("template").Parse(r.TemplateFile))
	t2 := template.Must(template.New("output").Parse(r.OutputFile))

	b := new(bytes.Buffer)

	if err := t1.Execute(b, params); err != nil {
		return nil, fmt.Errorf("Unexpected error executing 'template' template: %v", err)
	}

	ret.TemplateFile = b.String()

	b.Reset()

	if err := t2.Execute(b, params); err != nil {
		return nil, fmt.Errorf("Unexpected error executing 'output' template: %v", err)
	}

	ret.OutputFile = b.String()

	for k, q := range r.Queries {
		b.Reset()
		t := template.Must(template.New("q").Parse(q.Statement))

		if err := t.Execute(b, params); err != nil {
			return nil, fmt.Errorf("Unexpected error executing 'query' template '%s': %v", k, err)
		}

		ret.Queries[k] = Query{
			Name:      k,
			Source:    q.Source,
			Range:     q.Range,
			Statement: b.String(),
		}
	}

	return &ret, nil

}
