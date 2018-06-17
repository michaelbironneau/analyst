package main

import (
	"encoding/json"
	"github.com/michaelbironneau/analyst"
	"github.com/michaelbironneau/analyst/engine"
	"golang.org/x/net/websocket"
)

type websocketWriter struct {
	ws      *websocket.Conn
	msgType string
}

func (w *websocketWriter) Write(p []byte) (n int, err error) {
	var entry struct {
		Entry string `json:"entry"`
	}
	entry.Entry = string(p)
	b, err := json.Marshal(entry)
	if err != nil {
		return 0, err
	}
	msg := Message{
		Type: w.msgType,
		Data: json.RawMessage(b),
	}
	bb, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}
	return w.ws.Write(bb)
}

func redirectOutputHook(ws *websocket.Conn, msgType string) engine.DestinationHook {
	return func(_ string, d engine.Destination) (engine.Destination, error) {
		cd, ok := d.(*engine.ConsoleDestination)
		if !ok {
			return nil, nil
		}
		cd.Writer = &websocketWriter{ws, msgType}
		return nil, nil
	}
}

func outputHooks(ws *websocket.Conn) analyst.RuntimeOptions {
	var opts analyst.RuntimeOptions
	opts.Logger = engine.NewGenericLogger(engine.Info, &websocketWriter{ws, MsgLog})
	opts.Hooks = []interface{}{redirectOutputHook(ws, MsgResult)}
	return opts
}
