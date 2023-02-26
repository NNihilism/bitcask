package util

import "strconv"

// Float64ToStr Convert type float64 to string
func Float64ToStr(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

// StrToFloat64 convert type string to float64
func StrToFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}

// StrToInt64 converts type string to int64.
func StrToInt64(val string) (int64, error) {
	return strconv.ParseInt(val, 10, 64)
}

// StrToUint converts type string to uint64.
func StrToUint(val string) (uint64, error) {
	return strconv.ParseUint(val, 10, 64)
}

func StrArrToByteArr(strArr []string) [][]byte {
	res := make([][]byte, len(strArr))
	for i, str := range strArr {
		res[i] = []byte(str)
	}
	return res
}

func BytesArrToStrArr(arrs [][]byte) []string {
	res := make([]string, len(arrs))
	for i := range arrs {
		res[i] = string(arrs[i])
	}
	return res
}
