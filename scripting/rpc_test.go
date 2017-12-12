package scripting

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestRPC(t *testing.T) {
	Convey("Given an RPC client", t, func(){
		Convey("It should process messages correctly", func(){
			tc, err := NewTransformClient("python", "./rpc_test.py")
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
