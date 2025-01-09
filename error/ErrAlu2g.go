package error

import "fmt"

type ErrAlu2g string

// Error implements error.
func (e ErrAlu2g) Error() string {
	return fmt.Sprintf("ErrAlu2g: %s", string(e))
}

func NewErrAlu2g(format string, args ...any) error {
	return ErrAlu2g(fmt.Sprintf(format, args...))
}
