package plugins

import (
	"github.com/natefinch/pie"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
)

type SourceJSONRPC struct {
	Path   string
	Args   []string
	client *rpc.Client
}

func (t *SourceJSONRPC) Dial() error {
	var err error
	t.client, err = pie.StartProviderCodec(jsonrpc.NewClientCodec, os.Stderr, t.Path, t.Args...)
	return err
}

func (t *SourceJSONRPC) Close() error {
	return t.client.Close()
}

func (t *SourceJSONRPC) SetOption(name string, value interface{}) error {
	var reply interface{}
	return t.client.Call("set_option", option{name, value}, &reply)
}

func (t *SourceJSONRPC) SetDestinations(names []string) error {
	var reply interface{}
	return t.client.Call("set_destinations", names, &reply)
}

func (t *SourceJSONRPC) GetOutputColumns() (map[string][]string, error) {
	var reply map[string][]string
	err := t.client.Call("get_output_columns", nil, &reply)
	return reply, err
}

func (t *SourceJSONRPC) Receive() ([]OutputRow, []LogEntry, error) {
	var reply output
	err := t.client.Call("receive", nil, &reply)
	return reply.Rows, reply.Logs, err
}
