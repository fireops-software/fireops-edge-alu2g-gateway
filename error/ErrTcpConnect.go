package error

import "fmt"

type ErrTcpConnect string

// Error implements error.
func (e ErrTcpConnect) Error() string {
	return fmt.Sprintf("ErrTcpConnect: %s", string(e))
}

func NewErrTcpConnect(format string, args ...any) error {
	return ErrTcpConnect(fmt.Sprintf(format, args...))
}
