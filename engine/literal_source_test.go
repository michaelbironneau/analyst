package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestLiteralSourceArray(t *testing.T) {
	s1 := `[
	[1, "A"],
	[2, "B"],
	[3, "C"]
	]`
	cols := []string{"a", "b"}
	Convey("Given a valid content string (JSON flat array) and columns", t, func() {
		Convey("The literal source can marshall them to rows and emit them", func() {
			l := &LiteralSource{
				Content: s1,
				Columns: cols,
				Name:    "Literal Source",
			}
			l.SetName("Outgoing name")
			s := NewStream(nil, 100)
			logger := NewConsoleLogger(Trace)
			st := NewStopper()
			l.Open(s, logger, st)
			var index int
			var expected = [][]interface{}{
				[]interface{}{1, "A"},
				[]interface{}{2, "B"},
				[]interface{}{3, "C"},
			}
			So(s.Columns(), ShouldResemble, cols)
			for msg := range s.Chan(DestinationWildcard) {
				So(msg.Source, ShouldEqual, "Outgoing name")
				for i := range msg.Data {
					So(msg.Data[i], ShouldEqual, expected[index][i])
				}

				index++
			}
		})
	})
}

func TestLiteralSourceObjects(t *testing.T) {
	s1 := `[
	{"a": 1, "b": "A"},
	{"b": "B", "a": 2},
	{"a": 3, "b": "C"}
	]`
	cols := []string{"a", "b"}
	Convey("Given a valid content string (JSON array of objects) and columns", t, func() {
		Convey("The literal source can marshall them to rows and emit them", func() {
			l := &LiteralSource{
				Content: s1,
				Columns: cols,
				Name:    "Literal Source",
				Format:  JSONObjects,
			}
			l.SetName("Outgoing name")
			s := NewStream(nil, 100)
			logger := NewConsoleLogger(Trace)
			st := NewStopper()
			l.Open(s, logger, st)
			var index int
			var expected = [][]interface{}{
				[]interface{}{1, "A"},
				[]interface{}{2, "B"},
				[]interface{}{3, "C"},
			}
			So(s.Columns(), ShouldResemble, cols)
			for msg := range s.Chan(DestinationWildcard) {
				So(msg.Source, ShouldEqual, "Outgoing name")
				for i := range msg.Data {
					So(msg.Data[i], ShouldEqual, expected[index][i])
				}

				index++
			}
		})
	})
}

func TestCSVSourceArray(t *testing.T) {
	s1 :=
		`1,A
2,B
3,C`
	cols := []string{"a", "b"}
	Convey("Given a valid content string (JSON flat array) and columns", t, func() {
		Convey("The literal source can marshall them to rows and emit them", func() {
			l := &LiteralSource{
				Content: s1,
				Columns: cols,
				Name:    "Literal Source",
				Format:  CSVWithoutHeader,
			}
			l.SetName("Outgoing name")
			s := NewStream(nil, 100)
			logger := NewConsoleLogger(Trace)
			st := NewStopper()
			l.Open(s, logger, st)
			var index int
			var expected = [][]interface{}{
				[]interface{}{"1", "A"},
				[]interface{}{"2", "B"},
				[]interface{}{"3", "C"},
			}
			So(s.Columns(), ShouldResemble, cols)
			for msg := range s.Chan(DestinationWildcard) {
				So(msg.Source, ShouldEqual, "Outgoing name")
				for i := range msg.Data {
					So(msg.Data[i], ShouldEqual, expected[index][i])
				}

				index++
			}
		})
	})
}
