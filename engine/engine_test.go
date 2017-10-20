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

			e := NewStream([]string{"error"}, DefaultBufferSize)
			sourceStream := NewStream(s.Columns(), DefaultBufferSize)
			transformedStream := NewStream(cols, DefaultBufferSize)

			s.Open(sourceStream)
			t.Open(sourceStream, transformedStream)
			d.Open(transformedStream, e)

			So(d.Results(), ShouldResemble, msg)
		})
	})
}

func TestCoordinator(t *testing.T) {
	Convey("Given a coordinator and some data", t, func() {
		c := NewCoordinator()
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
}
