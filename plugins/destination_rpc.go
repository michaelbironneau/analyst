package plugins

import (
	"github.com/natefinch/pie"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
)

type DestinationJSONRPC struct {
	Path   string
	Args   []string
	client *rpc.Client
}

func (t *DestinationJSONRPC) Dial() error {
	var err error
	t.client, err = pie.StartProviderCodec(jsonrpc.NewClientCodec, os.Stderr, t.Path, t.Args...)
	return err
}

func (t *DestinationJSONRPC) Close() error {
	return t.client.Close()
}

func (t *DestinationJSONRPC) SetOption(name string, value interface{}) error {
	var reply interface{}
	return t.client.Call("set_option", option{name, value}, &reply)
}

func (t *DestinationJSONRPC) SetSources(names []string) error {
	var reply interface{}
	return t.client.Call("set_sources", names, &reply)
}

func (t *DestinationJSONRPC) SetInputColumns(source string, columns []string) error {
	var reply interface{}
	return t.client.Call("set_input_columns", inputColumns{source, columns}, &reply)
}

func (t *DestinationJSONRPC) Send(rows []InputRow) ([]LogEntry, error) {
	var reply output
	err := t.client.Call("receive", rows, &reply)
	return reply.Logs, err
}

func (t *DestinationJSONRPC) EOS() ([]LogEntry, error) {
	var reply output
	err := t.client.Call("receive", nil, &reply)
	return reply.Logs, err
}
