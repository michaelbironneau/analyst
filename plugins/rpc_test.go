package plugins

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"github.com/michaelbironneau/analyst/engine"
)

func TestRPC(t *testing.T) {
	Convey("Given an RPC client", t, func(){
		Convey("It should process messages correctly", func(){
			tc := TransformJSONRPC{Path:"python", Args: []string{"./rpc_test.py"}}
			err := tc.Dial()
			So(err, ShouldBeNil)
			defer tc.Close()
			So(err, ShouldBeNil)
			err = tc.SetSources([]string{"a"})
			So(err, ShouldBeNil)
			err = tc.SetDestinations([]string{"b"})
			So(err, ShouldBeNil)
			rows := []InputRow{
				InputRow{
					Data: []interface{}{"a"},
					Source: "a",
				},
			}
			output, _, err := tc.Send(rows)
			So(err, ShouldBeNil)
			So(output, ShouldHaveLength, 1)
			So(output[0].Data, ShouldResemble, []interface{}{"a"})
		})
	})
}

func TestRPCWithWrapper(t *testing.T){
	Convey("Given an RPC client", t, func(){
		Convey("It should receive wrapped source messages correctly", func(){
			sRPC := SourceJSONRPC{Path:"python", Args: []string{"./source.py"}}
			s := source{
				S: &sRPC,
				Alias: "source",
				Destinations: []string{"a"},
			}
			err := s.Ping()
			So(err, ShouldBeNil)

			st := engine.NewStream([]string{"a", "b", "c"},100)
			l := engine.ConsoleLogger{}
			stop := engine.NewStopper()

			s.Open(st, &l, stop)

			msg := <- st.Chan("")

			So(msg.Data[0], ShouldEqual, 0)
			So(msg.Data[1], ShouldEqual, 1)
			So(msg.Data[2], ShouldEqual, 2)
			So(msg.Source, ShouldEqual, "source")

		})
	})
}
