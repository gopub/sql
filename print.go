package sql

import (
	"github.com/gopub/log"
)

func toReadableArgs(args []interface{}) []interface{} {
	if log.DebugLevel >= log.GetLevel() {
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
