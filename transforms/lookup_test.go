package transforms

import (
	"fmt"
	"github.com/alecthomas/participle"
	"github.com/michaelbironneau/analyst/engine"
	. "github.com/smartystreets/goconvey/convey"
	"sync"
	"testing"
)

func TestLookupParsing(t *testing.T) {
	parser, err := participle.Build(&Lookup{}, lookupLexer)
	if err != nil {
		panic(err)
	}
	Convey("Given a valid lookup", t, func() {
		//1
		s1 := `
		LOOKUP db1.Col1, db2.Col2, db1.Col3 FROM db1
		INNER JOIN db2 ON db1.Col1 = db2.Col1 AND db1.Col3 = db2.Col2
		`
		a := Lookup{}
		err = parser.ParseString(s1, &a)
		So(err, ShouldBeNil)
		So(a.InnerJoin, ShouldBeTrue)
		So(a.OuterJoin, ShouldBeFalse)
		So(a.FromSource, ShouldEqual, "db1")
		So(a.LookupSource, ShouldEqual, "db2")
		So(a.Projection, ShouldHaveLength, 3)
		So(a.Projection[0].Source, ShouldEqual, "db1")
		So(a.Projection[0].Column, ShouldEqual, "Col1")
		So(a.Conditions, ShouldHaveLength, 2)
		So(a.Conditions[1].T1Column.Column, ShouldEqual, "Col3")
		So(a.Conditions[1].T2Column.Source, ShouldEqual, "db2")
	})
}

func TestProject(t *testing.T) {
	Convey("Given projection and actual columns", t, func() {
		projection := []string{"a", "b"}
		actual := []string{"c", "b", "a"}
		Convey("It should return a correct projection", func() {
			p, err := project(projection, actual)
			So(err, ShouldBeNil)
			projected := p([]interface{}{1, 2, 3})
			So(projected, ShouldResemble, map[string]interface{}{"a": 3, "b": 2})
		})
	})
}

func TestSplitProject(t *testing.T) {
	Convey("Given a slice of lookup columns", t, func() {
		cols := []LookupColumn{
			LookupColumn{
				Source: "A",
				Column: "aa",
			}, LookupColumn{
				Source: "b",
				Column: "ba",
			},
			LookupColumn{
				Source: "a",
				Column: "AA",
			},
		}
		base := "a"
		lookup := "b"
		Convey("It should successfully split them into base and lookup projections", func() {
			b, l, err := splitProjections(cols, base, lookup)
			So(err, ShouldBeNil)
			So(l[0], ShouldEqual, "ba")
			So(b, ShouldHaveLength, 2)
		})
	})
}

func TestLookup(t *testing.T) {
	s := `
	LOOKUP a.Id, b.Name, a.Something from A
	INNER JOIN b ON b.Id = a.Id and a.Id2 = b.Id2
	`
	Convey("Given a valid LOOKUP transform script", t, func() {
		aCols := []string{"id", "something", "id2"}
		bCols := []string{"id", "name", "id2"}

		aMsgs := [][]interface{}{
			[]interface{}{1, "A", 2},
			[]interface{}{2, "B", 3},
			[]interface{}{"D", "C", 3},
		}
		bMsgs := [][]interface{}{
			[]interface{}{2, "bob", 3},
			[]interface{}{"D", "john", 3},
		}
		l, err := NewLookup(s)
		l.SetName("lookup")
		Convey("The lookup object should be correctly created", func() {
			So(err, ShouldBeNil)
			So(l.outgoingName, ShouldEqual, "lookup")
			So(l.outerJoin, ShouldBeFalse)
			So(l.sourceSeq, ShouldResemble, []string{"b", "a"})
			So(l.baseJoinColumns, ShouldResemble, []string{"id", "id2"})
			So(l.lookupJoinColumns, ShouldResemble, []string{"id", "id2"})
		})
		Convey("The lookup should run correctly", func() {
			inA := engine.NewStream(aCols, 100)
			inB := engine.NewStream(bCols, 100)
			out := engine.NewStream(nil, 100)
			logger := engine.ConsoleLogger{}
			st := engine.NewStopper()
			for i := range aMsgs {
				var msg engine.Message
				msg.Source = "a"
				msg.Destination = "lookupl"
				msg.Data = aMsgs[i]
				inA.Chan("lookup") <- msg
			}
			for i := range bMsgs {
				var msg engine.Message
				msg.Source = "b"
				msg.Destination = "lookupl"
				msg.Data = bMsgs[i]
				inB.Chan("lookup") <- msg
			}
			close(inA.Chan("lookup"))
			close(inB.Chan("lookup"))
			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				l.Open(inA, out, &logger, st)
				wg.Done()
			}()
			go func() {
				l.Open(inB, out, &logger, st)
				wg.Done()
			}()
			wg.Wait()
			var count int
			for row := range out.Chan(engine.DestinationWildcard) {
				count++
				So(row.Data, ShouldHaveLength, 3)
				So(row.Data[0], ShouldBeIn, []interface{}{2, "D"})
				So(row.Data[1], ShouldBeIn, []interface{}{"bob", "john"})
				So(row.Data[2], ShouldBeIn, []interface{}{"A", "B", "C"})
				fmt.Println(row)
			}
			So(count, ShouldEqual, 2)
		})
	})

}
