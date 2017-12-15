package plugins

import (
	"net/rpc"
	"github.com/natefinch/pie"
	"net/rpc/jsonrpc"
	"os"
)

type TransformJSONRPC struct{
	Path string
	Args []string
	client *rpc.Client
}

func (t *TransformJSONRPC) Dial() error {
	var err error
	t.client, err = pie.StartProviderCodec(jsonrpc.NewClientCodec, os.Stderr, t.Path, t.Args...)
	return err
}

func (t *TransformJSONRPC) Close() error {
	return t.client.Close()
}

type option struct {
	Name string `json:"name"`
	Value interface{} `json:"value"`
}

type inputColumns struct {
	Source string `json:"source"`
	Columns []string `json:"columns"`
}

type output struct {
	Rows []OutputRow `json:"rows"`
	Logs []LogEntry `json:"logs"`
}

func (t *TransformJSONRPC) SetOption(name string, value interface{}) error {
	var reply interface{}
	return t.client.Call("set_option", option{name, value}, &reply)
}

func (t *TransformJSONRPC) SetSources(names []string) error {
	var reply interface{}
	return t.client.Call("set_sources", names, &reply)
}

func (t *TransformJSONRPC) SetDestinations(names []string) error {
	var reply interface{}
	return t.client.Call("set_destinations", names, &reply)
}

func (t *TransformJSONRPC) SetInputColumns(source string, columns []string) error {
	var reply interface{}
	return t.client.Call("set_input_columns", inputColumns{source, columns}, &reply)
}

func (t *TransformJSONRPC) GetOutputColumns(destination string) ([]string, error){
	var reply []string
	err := t.client.Call("get_output_columns", destination, &reply)
	return reply, err
}

func (t *TransformJSONRPC) Send(rows []InputRow) ([]OutputRow, []LogEntry, error){
	var reply output
	err := t.client.Call("receive", rows, &reply)
	return reply.Rows, reply.Logs, err
}

func (t *TransformJSONRPC) EOS() ([]OutputRow, []LogEntry, error){
	var reply output

	err := t.client.Call("receive", nil, &reply)
	return reply.Rows, reply.Logs, err
}