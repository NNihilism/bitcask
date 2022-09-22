package util

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
)

func ConvertToBSlice(vi interface{}) []byte {
	typ := reflect.ValueOf(vi)
	switch typ.Kind() {
	case reflect.String:
		if val := vi.(string); val == "OK" || val == "PONG" {
			return []byte(val)
		} else {
			return []byte(fmt.Sprintf("\"%s\"", vi.(string)))
		}
	case reflect.Slice:
		return sliceToBSlice(vi)
	case reflect.Int:
		valStr := strconv.Itoa(vi.(int))
		return []byte(fmt.Sprintf("(integer) %s", valStr))
	case reflect.Int64:
		valStr := strconv.Itoa(int(vi.(int64)))
		return []byte(fmt.Sprintf("(integer) %s", valStr))
	case reflect.Invalid:
		return []byte("(nil)")
	default:
		return []byte("(undefine)")
	}
}

func sliceToBSlice(vi interface{}) (res []byte) {
	// Only consider []byte and [][]byte
	v := reflect.ValueOf(vi)
	l := v.Len()
	if l == 0 { // empty slice
		return ConvertToBSlice(nil)
	} else if v.Index(0).Kind() == reflect.Uint8 { // []byte
		return []byte(fmt.Sprintf("\"%s\"", v.Bytes()))
	} else { // [][]byte
		// Convert to [][]byte and get the element.
		// If use v.Index() to get element and pass it to ConvertToBSlice(), it will return "(undefine)"
		// Because:
		// reflect.ValueOf(v.Index(i)).Kind() ----- struct
		// v.index(i).Kind() ------ slice
		s := vi.([][]byte)
		parts := make([][]byte, 3*l)
		for i := 0; i < l; i++ {
			parts[i*3] = []byte(fmt.Sprintf("%s) ", strconv.Itoa(i+1)))
			parts[i*3+1] = ConvertToBSlice(s[i])
			parts[i*3+2] = []byte("\n")
		}
		return bytes.Join(parts[:len(parts)-1], []byte(""))
	}
}

func NewErrUnknownCMD(cmd []byte, args [][]byte) error {
	for i, arg := range args {
		args[i] = []byte(fmt.Sprintf("'%s'", arg))
	}
	return fmt.Errorf("(error) ERR unknown command '%s', with args beginning with: %s", cmd, bytes.Join(args, []byte(", ")))
}
