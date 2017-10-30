package engine

//Middleware is a func that transforms a stream.
type Middleware func(Stream) Stream
