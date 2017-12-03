package sql

import "github.com/natande/gox"

func printArgs(query string, args ...interface{}) {
	if gox.LogLevel <= gox.LogLevelDebug {
		readableArgs := make([]interface{}, len(args))
		for i, a := range args {
			if b, ok := a.([]byte); ok {
				readableArgs[i] = string(b)
			} else {
				readableArgs[i] = a
			}
		}
		gox.LogDebug(query, readableArgs)
	}
}
