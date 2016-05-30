package main

import (
	"github.com/BurntSushi/toml"
	"github.com/michaelbironneau/analyst/aql"
)

type Connection struct {
	Name             string
	Driver           string
	ConnectionString string
}

type ConnectionConfig struct {
	Connections []Connection `toml:"connection"`
}

func parseConn(file string) (map[string]aql.Connection, error) {
	var config ConnectionConfig
	if _, err := toml.DecodeFile(file, &config); err != nil {
		return nil, err
	}
	ret := make(map[string]aql.Connection)
	for i := range config.Connections {
		ret[config.Connections[i].Name] = aql.Connection{
			Driver:           config.Connections[i].Driver,
			ConnectionString: config.Connections[i].ConnectionString,
		}
	}
	return ret, nil
}
