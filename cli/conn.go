package main 

import (
	"github.com/BurntSushi/toml"
	"github.com/michaelbironneau/analyst/aql"
)

type ConnectionConfig struct {
	Name string 
	Driver string 
	ConnectionString string
}

func parseConn(file string) (map[string]aql.Connection, error) {
	var connections []ConnectionConfig
	if _, err := toml.DecodeFile(file, connections); err != nil {
		return nil, err
	}
	ret := make(map[string]aql.Connection)
	for i := range connections {
		ret[connections[i].Name] = aql.Connection{
			Driver: connections[i].Driver,
			ConnectionString: connections[i].ConnectionString,
		}
	}
	return ret, nil
}