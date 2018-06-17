package engine

import (
	"context"
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
			l := NewConsoleLogger(Trace)
			st := &stopper{}

			sourceStream := NewStream(cols, DefaultBufferSize)
			transformedStream := NewStream(cols, DefaultBufferSize)

			s.Open(sourceStream, l, st)
			t.Open(sourceStream, transformedStream, l, st)
			d.Open(transformedStream, l, st)

			So(d.Results(), ShouldResemble, msg)
		})
	})
}

func TestCoordinatorInvalidTermination(t *testing.T) {
	Convey("Given a coordinator and a job that terminates on a transform", t, func() {
		l := NewConsoleLogger(Trace)
		tx := NewTransactionManager(l)
		c := NewCoordinator(l, tx)
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		cols := []string{"1", "2", "3"}
		s := NewSliceSource(cols, msg)
		tt := Passthrough{}
		Convey("It should return an error when compiling the job", func() {
			err := c.AddSource("source", "slice", s)
			So(err, ShouldBeNil)
			err = c.AddTransform("transformation", "passthrough", &tt)
			So(err, ShouldBeNil)
			err = c.Connect("source", "transformation")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldNotBeNil)
		})
	})
}

func TestCoordinator(t *testing.T) {
	Convey("Given a coordinator and some data", t, func() {
		l := NewConsoleLogger(Trace)
		tx := NewTransactionManager(l)
		c := NewCoordinator(l, tx)
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		cols := []string{"1", "2", "3"}
		Convey("It should execute a passthrough example correctly", func() {
			s := NewSliceSource(cols, msg)
			t := Passthrough{}
			t.SetName("passthrough")
			d := SliceDestination{Alias: "slice"}
			err := c.AddSource("source", "slice", s)
			So(err, ShouldBeNil)
			err = c.AddTransform("transformation", "passthrough", &t)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "slice", &d)
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
		l := NewConsoleLogger(Trace)
		tx := NewTransactionManager(l)
		c := NewCoordinator(l, tx)
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		msg2 := [][]interface{}{[]interface{}{"g", "h", "i"}, []interface{}{"j", "k", "l"}}
		cols := []string{"1", "2", "3"}
		Convey("It should process many sources -> one destination correctly", func() {
			s := NewSliceSource(cols, msg)
			s2 := NewSliceSource(cols, msg2)
			s.SetName("source 1")
			s2.SetName("source 2")
			d := SliceDestination{Alias: "destination"}
			err := c.AddSource("source 1", "source 1", s)
			So(err, ShouldBeNil)
			err = c.AddSource("source 2", "source 2", s2)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "destination", &d)
			So(err, ShouldBeNil)
			err = c.Connect("source 1", "destination")
			So(err, ShouldBeNil)
			err = c.Connect("source 2", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			for _, m := range append(msg, msg2...) {
				So(d.Results(), ShouldContain, m)
			}
			for _, m := range d.Results() {
				So(append(msg, msg2...), ShouldContain, m)
			}
			So(d.Results(), ShouldHaveLength, 2*len(msg))
		})
		Convey("It should process one source -> multiple destinations correctly", func() {
			s := NewSliceSource(cols, msg)
			s.SetName("source")
			d1 := SliceDestination{Alias: "dest 1"}
			d2 := SliceDestination{Alias: "dest 2"}
			err := c.AddSource("source", "source", s)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination 1", "dest 1", &d1)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination 2", "dest 2", &d2)
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
			s.SetName("s")
			p1 := Passthrough{}
			p1.SetName("transform 1")
			p2 := Passthrough{}
			p2.SetName("transform 2")
			d := SliceDestination{Alias: "destination"}
			err := c.AddSource("source", "s", s)
			So(err, ShouldBeNil)
			err = c.AddTransform("transform 1", "transform 1", &p1)
			So(err, ShouldBeNil)
			err = c.AddTransform("transform 2", "transform 2", &p2)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "destination", &d)
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
			//So(d.Results(), ShouldResemble, append(msg, msg...))
			exp := append(msg, msg...)
			actual := d.Results()
			for _, e := range exp {
				So(actual, ShouldContain, e)
			}
			for _, a := range actual {
				So(exp, ShouldContain, a)
			}
		})

	})
}

func TestTester(t *testing.T) {
	Convey("Given a coordinator and a failing test", t, func() {
		l := NewConsoleLogger(Trace)
		tx := NewTransactionManager(l)
		c := NewCoordinator(l, tx)
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		cols := []string{"1", "2", "3"}
		failTester := func(map[string]interface{}, bool) bool {
			return false
		}
		Convey("It should stop stream if a test fails", func() {
			s := NewSliceSource(cols, msg)
			s.SetName("s")
			d := SliceDestination{Alias: "d"}
			err := c.AddSource("source", "s", s)
			So(err, ShouldBeNil)
			err = c.AddTest("source", "failed test", "always failing test", failTester)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "d", &d)
			err = c.Connect("source", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldNotBeNil)
			So(d.Results(), ShouldHaveLength, 0)
		})

	})
}

func TestCancellation(t *testing.T) {
	Convey("Given a coordinator and context that cancels straight away", t, func() {
		l := NewConsoleLogger(Trace)
		tx := NewTransactionManager(l)
		c := NewCoordinator(l, tx)
		ctx, cancel := context.WithCancel(context.Background())
		c.UseContext(ctx)
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		cols := []string{"1", "2", "3"}
		Convey("It should return an error and stop straight away", func() {
			s := NewSliceSource(cols, msg)
			s.SetName("s")
			d := SliceDestination{Alias: "d"}
			err := c.AddSource("source", "s", s)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "d", &d)
			err = c.Connect("source", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			cancel()
			err = c.Execute()
			So(err, ShouldEqual, ErrInterrupted)
			So(d.Results(), ShouldHaveLength, 0)
		})

	})
}

func TestNoCancellation(t *testing.T) {
	Convey("Given a coordinator and context that never cancels", t, func() {
		l := NewConsoleLogger(Trace)
		tx := NewTransactionManager(l)
		c := NewCoordinator(l, tx)
		ctx := context.Background()
		c.UseContext(ctx)
		msg := [][]interface{}{[]interface{}{"a", "b", "c"}, []interface{}{"d", "e", "f"}}
		cols := []string{"1", "2", "3"}
		Convey("It should return no error and not panic", func() {
			s := NewSliceSource(cols, msg)
			s.SetName("s")
			d := SliceDestination{Alias: "d"}
			err := c.AddSource("source", "s", s)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "d", &d)
			err = c.Connect("source", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldEqual, nil)
			So(d.Results(), ShouldHaveLength, 2)
		})

	})
}

func TestNamedStreams(t *testing.T) {
	Convey("Given a named slice source and two destinations", t, func() {
		msg := []Message{
			Message{
				Destination: "d1",
				Data:        []interface{}{1, 2},
			},
			Message{
				Destination: "d1",
				Data:        []interface{}{3, 4},
			},
			Message{
				Destination: "d2",
				Data:        []interface{}{5, 6},
			},
		}
		s := NewNamedSliceSource([]string{"a", "b"}, msg)
		s.SetName("s")
		d1 := SliceDestination{Alias: "d1"}
		d2 := SliceDestination{Alias: "d2"}
		l := NewConsoleLogger(Trace)
		tx := NewTransactionManager(l)
		c := NewCoordinator(l, tx)
		c.AddSource("slice source", "s", s)
		c.AddDestination("slice destination 1", "d1", &d1)
		c.AddDestination("slice destination 2", "d2", &d2)
		c.Connect("slice source", "slice destination 1")
		c.Connect("slice source", "slice destination 2")
		Convey("It should route the messages to their named destinations", func() {
			err := c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			So(d1.Results(), ShouldResemble, [][]interface{}{[]interface{}{1, 2}, []interface{}{3, 4}})
			So(d2.Results(), ShouldResemble, [][]interface{}{[]interface{}{5, 6}})
		})
	})
}
