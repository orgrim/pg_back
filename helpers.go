package main

import "io"

func WrappedClose(c io.Closer, err *error) {
	cErr := c.Close()
	if cErr != nil && *err == nil {
		*err = cErr
	}
}
