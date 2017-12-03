package sql

import (
	"github.com/natande/gox"
)

func toReadableArgs(args []interface{}) []interface{} {
	if gox.LogLevel <= gox.LogLevelDebug {
		readableArgs := make([]interface{}, len(args))
		for i, a := range args {
			if b, ok := a.([]byte); ok {
				readableArgs[i] = string(b)
			} else {
				readableArgs[i] = a
			}
		}
		return readableArgs
	}
	return args
}
