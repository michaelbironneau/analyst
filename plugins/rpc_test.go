package plugins

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"github.com/michaelbironneau/analyst/engine"
	"github.com/michaelbironneau/analyst/aql"
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

func TestSourceRPCWithWrapper(t *testing.T){
	Convey("Given an RPC client", t, func(){
		Convey("It should receive wrapped Source messages correctly", func(){
			sRPC := SourceJSONRPC{Path:"python", Args: []string{"./source.py"}}
			optVal := "asdf"
			s := Source{
				Plugin:       &sRPC,
				alias:        "Source",
			}
			err := s.Configure([]aql.Option{
				aql.Option{
					Key: "test",
					Value: &aql.OptionValue{
						Str: &optVal,
					},
				},
			})
			So(err, ShouldBeNil)
			err = s.Ping()
			So(err, ShouldBeNil)

			st := engine.NewStream([]string{"a", "b", "c"},100)
			l := engine.ConsoleLogger{}
			stop := engine.NewStopper()

			s.Open(st, &l, stop)

			msg := <- st.Chan("")

			So(msg.Data[0], ShouldEqual, 0)
			So(msg.Data[1], ShouldEqual, 1)
			So(msg.Data[2], ShouldEqual, 2)
			So(msg.Source, ShouldEqual, "Source")

		})
	})
}

func TestDestinationRPCWithWrapper(t *testing.T){
	Convey("Given an RPC client", t, func(){
		Convey("It should send messages to RPC Destination correctly", func(){
			sRPC := DestinationJSONRPC{Path:"python", Args: []string{"./destination.py"}}
			optVal := "asdf"
			s := Destination{
				Plugin: &sRPC,
				alias:  "Destination",
			}
			err := s.Configure([]aql.Option{
				aql.Option{
					Key: "test",
					Value: &aql.OptionValue{
						Str: &optVal,
					},
				},
			})


			So(err, ShouldBeNil)

			err = s.SetInputColumns("Source", []string{"a", "b", "c"})

			So(err, ShouldBeNil)

			err = s.Ping()
			So(err, ShouldBeNil)

			st := engine.NewStream([]string{"a", "b", "c"},100)
			l := engine.ConsoleLogger{}
			stop := engine.NewStopper()
			var m engine.Message
			m.Source = "Source"
			m.Data = []interface{}{"a", "b", "c"}
			st.Chan("") <- m
			close(st.Chan(""))
			s.Open(st, &l, stop)

		})
	})
}


func TestTransformRPCWithWrapper(t *testing.T){
	Convey("Given an RPC client", t, func(){
		Convey("It should process messages to/from TransformPlugin correctly", func(){
			sRPC := TransformJSONRPC{Path:"python", Args: []string{"./transform.py"}}
			optVal := "asdf"
			s := Transform{
				Plugin:       &sRPC,
			}
			err := s.Configure([]aql.Option{
				aql.Option{
					Key: "test",
					Value: &aql.OptionValue{
						Str: &optVal,
					},
				},
			})

			s.SetName("Name")

			So(err, ShouldBeNil)

			err = s.SetInputColumns("Source", []string{"a", "b", "c"})


			So(err, ShouldBeNil)

			err = s.Ping()
			So(err, ShouldBeNil)

			st := engine.NewStream([]string{"a", "b", "c"},100)
			out := engine.NewStream([]string{"a", "b", "c"},100)
			l := engine.ConsoleLogger{}
			stop := engine.NewStopper()
			var m engine.Message
			m.Source = "Source"
			m.Data = []interface{}{1, 2, 3}
			st.Chan("") <- m
			go func(){
				s.Open(st, out, &l, stop)
			}()

			msg := <- out.Chan("")

			So(msg.Source, ShouldEqual, "Name")
			So(msg.Destination, ShouldEqual, "")
			So(msg.Data[0], ShouldEqual, 2)
			So(msg.Data[1], ShouldEqual, 3)
			So(msg.Data[2], ShouldEqual, 4)

		})
	})
}