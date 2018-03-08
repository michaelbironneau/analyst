package transforms

import (
	"github.com/alecthomas/participle"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"github.com/michaelbironneau/analyst/engine"
	"time"
)

func TestApplyParsing(t *testing.T) {
	parser, err := participle.Build(&Apply{}, applyLexer)
	if err != nil {
		panic(err)
	}
	Convey("Given a valid apply", t, func() {
		//1
		s1 := `
		APPLY col1, CAST(col2 AS INT) AS alias1, CAST(col3 AS VARCHAR ), col4 AS alias2
		`
		a := Apply{}
		err = parser.ParseString(s1, &a)
		So(err, ShouldBeNil)
		So(a.Projections, ShouldHaveLength, 4)
		So(a.Projections[0].Cast, ShouldBeNil)
		So(a.Projections[0].Lookup.Column, ShouldEqual, "col1")
		So(a.Projections[0].Lookup.Alias, ShouldBeNil)
		So(a.Projections[1].Cast, ShouldNotBeNil)
		So(a.Projections[1].Cast.DestType, ShouldEqual, "INT")
		So(*a.Projections[1].Cast.Alias, ShouldEqual, "alias1")
		So(a.Projections[2].Cast.Alias, ShouldBeNil)
		So(*a.Projections[3].Lookup.Alias, ShouldEqual, "alias2")
	})
}

func TestApply(t *testing.T){
	s := `APPLY ColA, CAST(ColB AS DATETIME) AS ColBTime, CAST(ColC AS INT)`
	Convey("Given a valid APPLY transform script", t, func() {
		aCols := []string{"ColA", "ColB", "ColC"}
		aMsgs := [][]interface{}{
			[]interface{}{"id", "2018-12-01T12:00:00Z", "1"},
		}
		l, err := NewApply(s)
		So(err, ShouldBeNil)
		l.SetName("apply")
		Convey("The apply object should be correctly created", func() {
			So(l.outgoingName, ShouldEqual, "apply")
			So(l.castFns, ShouldHaveLength, 3)
			So(l.castFns[0], ShouldBeNil)
			So(l.castFns[1], ShouldNotBeNil)
			So(l.sourceCols, ShouldResemble, aCols)
			So(l.outputCols, ShouldResemble, []string{"ColA", "ColBTime", "ColC"})
		})
		Convey("The lookup should run correctly", func() {
			inA := engine.NewStream(aCols, 100)
			out := engine.NewStream(nil, 100)
			logger := engine.NewConsoleLogger(engine.Trace)
			st := engine.NewStopper()
			for i := range aMsgs {
				var msg engine.Message
				msg.Source = "a"
				msg.Destination = "apply"
				msg.Data = aMsgs[i]
				inA.Chan("apply") <- msg
			}
			close(inA.Chan("apply"))
			l.Open(inA, out, logger, st)
			var count int
			So(out.Columns(), ShouldResemble, []string{"ColA", "ColBTime", "ColC"})
			for row := range out.Chan(engine.DestinationWildcard) {
				count++
				So(row.Data, ShouldHaveLength, 3)
				So(row.Data[0], ShouldEqual, "id")
				expTime, _ := time.Parse(time.RFC3339, "2018-12-01T12:00:00Z")
				So(row.Data[1].(*time.Time).Unix(), ShouldEqual, expTime.Unix())
				So(row.Data[2], ShouldEqual, 1)
			}
			So(count, ShouldEqual, 1)
		})
	})
}