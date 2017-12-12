package scripting

import (
	"net/rpc"
	"github.com/natefinch/pie"
	"net/rpc/jsonrpc"
	"os"
)

type transform struct{
	Client *rpc.Client
}

func NewTransformClient(path string, args...string) (*transform, error) {
	client, err := pie.StartProviderCodec(jsonrpc.NewClientCodec, os.Stderr, path, args...)
	if err != nil {
		return nil, err
	}
	return &transform{Client: client}, nil
}

func (t *transform) Close() error {
	return t.Client.Close()
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

func (t *transform) SetOption(name string, value interface{}) error {
	var reply interface{}
	return t.Client.Call("set_option", option{name, value}, &reply)
}

func (t *transform) SetSources(names []string) error {
	var reply interface{}
	return t.Client.Call("set_sources", names, &reply)
}

func (t *transform) SetDestinations(names []string) error {
	var reply interface{}
	return t.Client.Call("set_destinations", names, &reply)
}

func (t *transform) SetInputColumns(source string, columns []string) error {
	var reply interface{}
	return t.Client.Call("set_input_columns", inputColumns{source, columns}, &reply)
}

func (t *transform) GetOutputColumns(destination string) ([]string, error){
	var reply []string
	err := t.Client.Call("get_output_columns", destination, &reply)
	return reply, err
}

func (t *transform) Send(rows []InputRow) ([]OutputRow, []LogEntry, error){
	var reply output
	err := t.Client.Call("receive", rows, &reply)
	return reply.Rows, reply.Logs, err
}

func (t *transform) EOS() ([]OutputRow, []LogEntry, error){
	var reply output
	err := t.Client.Call("receive", nil, &reply)
	return reply.Rows, reply.Logs, err
}