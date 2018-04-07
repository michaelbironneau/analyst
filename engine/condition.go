package engine

import (
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/datasource"
	"fmt"
	"github.com/araddon/qlbridge/vm"
	"github.com/araddon/qlbridge/value"
	"strings"
)

//Condition is a func that returns true if the message passes the test and false otherwise.
type Condition func(msg map[string]interface{}, eof bool) bool

func init(){
	builtins.LoadAllBuiltins()
}

func NewSQLCondition(sql string) (Condition, error) {
	exprAst, err := expr.ParseExpression(sql)
	if err != nil {
		return nil, fmt.Errorf("error parsing condition '%s': %v", sql, err)
	}
	return func(msg map[string]interface{}, eof bool) bool {
		if eof {
			return true
		}
		evalContext := datasource.NewContextSimpleNative(msg)
		val, ok := vm.Eval(evalContext, exprAst)
		if !ok  {
			return false
		}

		return castToBool(val) //use same truthiness conventions as AQL options
	}, nil
}

func castToBool(val value.Value) bool {
	if val.Err() {
		return false
	}
	if val.Nil() {
		return false
	}
	s := strings.ToLower(val.ToString())
	return s == "1" || s == "true"
}

func HasAtLeastNRowsCondition(n int) (Condition, error){
	i := 0
	return func(msg map[string]interface{}, eof bool) bool {
		if eof {
			if i < n {
				return false
			}
			return true
		}
		i++
		return true
	}, nil
}

func HasAtMostNRowsCondition(n int) (Condition, error){
	i := 0
	return func(msg map[string]interface{}, eof bool) bool {
		if eof {
			if i > n {
				return false
			}
			return true
		}
		i++
		if i > n {
			return false
		}

		return true
	}, nil
}