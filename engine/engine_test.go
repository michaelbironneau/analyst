package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestWithoutCoordinator(t *testing.T) {
	Convey("Given a slice of messages", t, func() {
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		cols := []string{"1", "2", "3"}
		Convey("A slice source/destination/passthrough transform should leave the messages as we found then", func() {
			s := NewSliceSource(cols, msg)
			t := Passthrough{}
			d := SliceDestination{}
			l := &ConsoleLogger{}
			st := &stopper{}

			sourceStream := NewStream(s.Columns(), DefaultBufferSize)
			transformedStream := NewStream(cols, DefaultBufferSize)

			s.Open(sourceStream, l, st)
			t.Open(sourceStream, transformedStream, l, st)
			d.Open(transformedStream, l, st)

			So(d.Results(), ShouldResemble, msg)
		})
	})
}

func TestCoordinator(t *testing.T) {
	Convey("Given a coordinator and some data", t, func() {
		c := NewCoordinator(&ConsoleLogger{})
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		cols := []string{"1", "2", "3"}
		Convey("It should execute a passthrough example correctly", func() {
			s := NewSliceSource(cols, msg)
			t := Passthrough{}
			d := SliceDestination{}
			err := c.AddSource("source", s)
			So(err, ShouldBeNil)
			err = c.AddTransform("transformation", &t)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", &d)
			So(err, ShouldBeNil)
			err = c.Connect("source", "transformation")
			So(err, ShouldBeNil)
			err = c.Connect("transformation", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			So(d.Results(), ShouldResemble, msg)
		})

	})
	Convey("Given a coordinator", t, func() {
		c := NewCoordinator(&ConsoleLogger{})
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		cols := []string{"1", "2", "3"}
		Convey("It should process many sources -> one destination correctly", func() {
			s := NewSliceSource(cols, msg)
			s2 := NewSliceSource(cols, msg)
			d := SliceDestination{}
			err := c.AddSource("source 1", s)
			So(err, ShouldBeNil)
			err = c.AddSource("source 2", s2)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", &d)
			So(err, ShouldBeNil)
			err = c.Connect("source 1", "destination")
			So(err, ShouldBeNil)
			err = c.Connect("source 2", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			So(d.Results(), ShouldResemble, append(msg, msg...))
		})
		Convey("It should process one source -> multiple destinations correctly", func() {
			s := NewSliceSource(cols, msg)
			d1 := SliceDestination{}
			d2 := SliceDestination{}
			err := c.AddSource("source", s)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination 1", &d1)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination 2", &d2)
			So(err, ShouldBeNil)
			err = c.Connect("source", "destination 1")
			So(err, ShouldBeNil)
			err = c.Connect("source", "destination 2")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			So(d1.Results(), ShouldResemble, msg)
			So(d2.Results(), ShouldResemble, msg)
		})
		Convey("It should process one source -> multiple transforms -> one destination correctly", func() {
			s := NewSliceSource(cols, msg)
			p1 := Passthrough{}
			p2 := Passthrough{}
			d := SliceDestination{}
			err := c.AddSource("source", s)
			So(err, ShouldBeNil)
			err = c.AddTransform("transform 1", &p1)
			So(err, ShouldBeNil)
			err = c.AddTransform("transform 2", &p2)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", &d)
			So(err, ShouldBeNil)
			err = c.Connect("source", "transform 1")
			So(err, ShouldBeNil)
			err = c.Connect("source", "transform 2")
			So(err, ShouldBeNil)
			err = c.Connect("transform 1", "destination")
			So(err, ShouldBeNil)
			err = c.Connect("transform 2", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			So(d.Results(), ShouldResemble, append(msg, msg...))
		})

	})
}
