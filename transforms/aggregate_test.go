package transforms

import (
	"github.com/alecthomas/participle"
	"github.com/michaelbironneau/analyst/engine"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestAggregateParsing(t *testing.T) {
	parser, err := participle.Build(&Aggregate{}, aggregateLexer)
	if err != nil {
		panic(err)
	}
	Convey("Given a valid aggregate", t, func() {
		//1
		s1 := `
		AGGREGATE Func(A) AS Val, Func2(A, 'a') AS Val2, Col2
		GROUP BY cde`
		a := Aggregate{}
		err = parser.ParseString(s1, &a)
		So(err, ShouldBeNil)
		So(a.Select[0].Function.Function, ShouldEqual, "Func(")
		So(a.Select[0].Function.Columns[0].Column, ShouldEqual, "A")
		So(*a.Select[1].Function.Columns[1].String, ShouldEqual, "a")
		So(a.Select[0].Alias, ShouldEqual, "Val")
		So(a.GroupBy[0], ShouldEqual, "cde")

	})
}

func TestFind(t *testing.T) {
	Convey("Given a haystack", t, func() {
		h := []string{"a", "B", "vvv"}
		Convey("It should return the index of a contained needle", func() {
			ix, ok := find(h, "vVv")
			So(ok, ShouldBeTrue)
			So(ix, ShouldEqual, 2)
		})
		Convey("It should return -1 index and not found if needle is not contained", func() {
			ix, ok := find(h, "www")
			So(ok, ShouldBeFalse)
			So(ix, ShouldEqual, -1)
		})
	})
}

func TestSuperAggregate(t *testing.T) {
	Convey("Given a superaggregate", t, func() {
		s := `AGGREGATE SUM(A) As Val`
		a, err := NewAggregate(s)
		Convey("It should create the Transform", func() {
			So(err, ShouldBeNil)
			So(a.aliasOrder, ShouldResemble, []string{"Val"})
			So(a.keyColumns, ShouldBeNil)
			So(a.blank.aggregates["Val"], ShouldHaveSameTypeAs, &sum{})
			So(a.blank.key, ShouldBeEmpty)
		})
		Convey("It should correctly generate the argument maps, and they should be case-insensitive", func() {
			maker := a.argMaker["Val"]
			argMap, err := maker([]string{"val", "A", "B"})
			So(err, ShouldBeNil)
			So(argMap([]interface{}{1, 2, 3}), ShouldResemble, []interface{}{2})
		})
		Convey("It should process messages correctly", func() {
			in := engine.NewStream([]string{"val", "A", "B"}, 100)
			out := engine.NewStream(nil, 100)
			l := engine.NewConsoleLogger(engine.Trace)
			st := engine.NewStopper()
			a.SetName("Agg")
			for i := 0; i < 5; i++ {
				var msg engine.Message
				msg.Source = "Source"
				msg.Destination = "Agg"
				msg.Data = []interface{}{"HHH", i, i + 1}
				in.Chan("Agg") <- msg
			}
			close(in.Chan("Agg"))
			a.Open(in, out, l, st)
			var count int
			for msg := range out.Chan(engine.DestinationWildcard) {
				So(out.Columns(), ShouldResemble, []string{"Val"})
				count++
				So(count, ShouldEqual, 1)
				So(msg.Source, ShouldEqual, "Agg")
				So(msg.Data, ShouldResemble, []interface{}{10.0})
			}

		})
	})

}

func TestFunctionParameters(t *testing.T) {
	Convey("Given an aggregate that takes static parameters", t, func() {
		s := `AGGREGATE ZOH(Time, Value, '2017-01-01T12:00:00Z', '2017-01-01T12:30:00Z') AS Val`
		a, err := NewAggregate(s)
		So(err, ShouldBeNil)
		Convey("It should process messages correctly", func() {
			in := engine.NewStream([]string{"time", "value"}, 100)
			out := engine.NewStream(nil, 100)
			l := engine.ConsoleLogger{}
			st := engine.NewStopper()
			a.SetName("Agg")
			inChan := in.Chan("Agg")
			inChan <- engine.Message{
				Source:      "Upstream",
				Destination: "Agg",
				Data:        []interface{}{"2017-01-01T12:00:00Z", 0.0, "2017-01-01T12:00:00Z", "2017-01-01T12:30:00Z"},
			}
			inChan <- engine.Message{
				Source:      "Upstream",
				Destination: "Agg",
				Data:        []interface{}{"2017-01-01T12:20:00Z", 3.0, "2017-01-01T12:00:00Z", "2017-01-01T12:30:00Z"},
			}
			close(in.Chan("Agg"))
			a.Open(in, out, &l, st)
			var count int
			for msg := range out.Chan(engine.DestinationWildcard) {
				So(out.Columns(), ShouldResemble, []string{"Val"})
				count++
				So(count, ShouldEqual, 1)
				So(msg.Source, ShouldEqual, "Agg")
				So(msg.Data, ShouldResemble, []interface{}{1.0})
			}
		})
	})
}

func TestGroupByAggregate(t *testing.T) {
	Convey("Given a valid group-by aggregate", t, func() {
		s := `
			AGGREGATE SUM(A) As Val, B
			GROUP BY B
			`
		a, err := NewAggregate(s)
		Convey("It should create the Transform", func() {
			So(err, ShouldBeNil)
			So(a.aliasOrder, ShouldResemble, []string{"Val", "B"})
			So(a.keyColumns, ShouldResemble, []string{"B"})
			So(a.blank.aggregates["Val"], ShouldHaveSameTypeAs, &sum{})
			So(a.blank.key, ShouldBeEmpty)
		})
		Convey("It should correctly generate the argument maps, and they should be case-insensitive", func() {
			maker := a.argMaker["Val"]
			argMap, err := maker([]string{"val", "A", "B"})
			So(err, ShouldBeNil)
			So(argMap([]interface{}{1, 2, 3}), ShouldResemble, []interface{}{2})
		})
		Convey("It should correctly generate the key map", func() {
			maker := a.keyMaker["B"]
			argMap, err := maker([]string{"val", "A", "B"})
			So(err, ShouldBeNil)
			So(argMap([]interface{}{1, 2, 3}), ShouldResemble, []interface{}{3})
		})
		Convey("It should process messages correctly", func() {
			in := engine.NewStream([]string{"val", "A", "B"}, 100)
			out := engine.NewStream(nil, 100)
			l := engine.ConsoleLogger{}
			st := engine.NewStopper()
			a.SetName("Agg")
			for i := 0; i < 5; i++ {
				var msg engine.Message
				msg.Source = "Source"
				msg.Destination = "Agg"
				msg.Data = []interface{}{"HHH", i, i % 2}
				in.Chan("Agg") <- msg
			}
			close(in.Chan("Agg"))
			a.Open(in, out, &l, st)
			var count int
			for msg := range out.Chan(engine.DestinationWildcard) {
				if count == 0 {
					So(out.Columns(), ShouldResemble, []string{"Val", "B"})
				}
				count++
				So(msg.Source, ShouldEqual, "Agg")
				if msg.Data[1].(int) == 0 {
					So(msg.Data, ShouldResemble, []interface{}{6.0, 0})
				} else {
					So(msg.Data, ShouldResemble, []interface{}{4.0, 1})
				}
			}
			So(count, ShouldEqual, 2)

		})
	})

}
