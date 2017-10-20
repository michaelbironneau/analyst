package engine

//Transform is a component that is neither a source nor a sink. It is configured with
//one or more sources, and one or more sinks.
type Transform interface {
	//Open gives the transform a stream to start pulling from
	Open(source, dest Stream)
}

type Passthrough struct{}

func (p *Passthrough) Open(source, dest Stream) {
	destChan := dest.Chan()
	for msg := range source.Chan() {
		destChan <- msg
	}
	close(destChan)
}
