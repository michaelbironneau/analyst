#!/usr/bin/python

# Description: A sample asynchronous RPC server plugin over STDIO in python that works with natefiinch/pie
# Usage:
#   pip install pyjsonrpc
#   go run master.go

import sys
import pyjsonrpc

#f = open('debug.txt', 'w')

class Transform(pyjsonrpc.JsonRpc):
    """
    JsonRpc subprocess test
    """
    options = {}
    sources = []
    destinations = []
    input_columns = {}
    output_columns = {"*": ["a", "b", "c"]}
    process = None

    @pyjsonrpc.rpcmethod
    def set_option(self, opt):
        """Set the option"""
        self.options[opt["name"]] = opt["value"]
        return ""

    @pyjsonrpc.rpcmethod
    def set_sources(self, names):
        self.sources = names
        return ""

    @pyjsonrpc.rpcmethod
    def set_destinations(self, names):
        self.destinations = names
        return ""

    @pyjsonrpc.rpcmethod
    def set_input_columns(self, columns):
        self.input_columns[columns["source"]] = columns["columns"]
        return ""

    @pyjsonrpc.rpcmethod
    def get_output_columns(self, destination):
        if destination in self.output_columns:
            return self.output_columns[destination]
        elif "*" in self.output_columns:
            return self.output_columns["*"]
        else:
            return []

    @pyjsonrpc.rpcmethod
    def receive(self, rows):
        data = None
        for row in rows:
            row_data = row["data"]
            data = [row_data[0]+1, row_data[1]+1, row_data[2]+1]
        return {"rows": [{"data": data}]}

def main():
    rpc = Transform()
    line = sys.stdin.readline()

    # This is a synchronous way to poll stdin, but because we
    while line:
        try:
            this_input = line
            out = rpc.call(this_input)
            if out:
                sys.stdout.write(out + "\n")
            sys.stdout.flush()
        except Exception as e:
            pass
            #f.write("Exception occured {0}\n".format(e))
            #f.flush()
        finally:
            line = sys.stdin.readline()
if __name__ == "__main__":
    main()
    #f.close()