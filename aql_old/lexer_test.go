package aql

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestLiterals(t *testing.T){
	Convey("When lexing a script containing only whitespace", t, func(){
		s := " \t \n "
		Convey("It should return nothing and no error", func(){
			tt, err := Lex(s)
			So(tt, ShouldHaveLength, 0)
			So(err, ShouldBeNil)
		})
	})
	Convey("When lexing a script with only commas and equal signs", t, func(){
		s := ",="
		Convey("It should return the right tokens and no error", func(){
			tt, err := Lex(s)
			So(tt, ShouldHaveLength, 2)
			So(tt[0].ID, ShouldEqual, COMMA)
			So(tt[1].ID, ShouldEqual, EQUALS)
			So(err, ShouldBeNil)
		})
	})
	Convey("When lexing a multi-line script", t, func(){
		s := ",\n,"
		Convey("It should return the right tokens and no error", func(){
			tt, err := Lex(s)
			So(tt, ShouldHaveLength, 2)
			So(tt[0].ID, ShouldEqual, COMMA)
			So(tt[0].LineNumber, ShouldEqual, 1)
			So(tt[1].ID, ShouldEqual, COMMA)
			So(tt[1].LineNumber, ShouldEqual, 2)
			So(err, ShouldBeNil)
		})
	})
}

func TestKeywords(t *testing.T){
	Convey("When lexing a script with keywords", t, func(){
		s := "QUERY TEST FROM\n INTO  DESCRIPTION  TRANSFORM EXTERN INCLUDE   \t RANGE WITH"
		ts := []Token{QUERY, TEST, FROM, INTO, DESCRIPTION, TRANSFORM, EXTERN, INCLUDE, RANGE, WITH}
		Convey("It should return the right tokens", func(){
			tt, err := Lex(s)
			So(err, ShouldBeNil)
			So(tt, ShouldHaveLength, len(ts))
			for i := range ts {
				So(tt[i].ID, ShouldEqual, ts[i])
			}
		})
	})
}

func TestInnerContent(t *testing.T){
	Convey("When lexing a script with () or ''", t, func(){
		s := "QUERY (content)"
		Convey("It should return the content and the right tokens", func(){
			tt, err := Lex(s)
			So(err, ShouldBeNil)
			So(tt, ShouldHaveLength, 4)
			So(tt[0].ID, ShouldEqual, QUERY)
			So(tt[1].ID, ShouldEqual, LPAREN)
			So(tt[2].ID, ShouldEqual, PAREN_BODY)
			So(tt[3].ID, ShouldEqual, RPAREN)
		})
		Convey("It should correctly parse nested () or ''", func(){
			s = "QUERY (content(a)')"
			tt, err := Lex(s)
			So(err, ShouldBeNil)
			So(tt, ShouldHaveLength, 4)
			So(tt[0].ID, ShouldEqual, QUERY)
			So(tt[1].ID, ShouldEqual, LPAREN)
			So(tt[2].ID, ShouldEqual, PAREN_BODY)
			So(tt[2].Content, ShouldEqual, "content(a)'")
			So(tt[3].ID, ShouldEqual, RPAREN)
			s = "QUERY 'content(a)('"
			tt, err = Lex(s)
			So(err, ShouldBeNil)
			So(tt, ShouldHaveLength, 4)
			So(tt[0].ID, ShouldEqual, QUERY)
			So(tt[1].ID, ShouldEqual, QUOTE)
			So(tt[2].ID, ShouldEqual, STRING)
			So(tt[2].Content, ShouldEqual, "content(a)(")
			So(tt[3].ID, ShouldEqual, QUOTE)
		})

		Convey("It should report an error when an unclosed ( is detected", func(){
			s = "QUERY ("
			_, err := Lex(s)
			So(err, ShouldNotBeNil)
			s = "QUERY )"
			_, err = Lex(s)
			So(err, ShouldNotBeNil)
		})
		Convey("It should report an error when an unclosed ' is detected", func(){
			s = "QUERY '"
			_, err := Lex(s)
			So(err, ShouldNotBeNil)
		})
	})
}