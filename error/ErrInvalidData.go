package error

import "fmt"

type ErrInvalidData string

// Error implements error.
func (e ErrInvalidData) Error() string {
	return fmt.Sprintf("ErrInvalidData: %s", string(e))
}

func NewErrInvalidData(format string, args ...any) error {
	return ErrInvalidData(fmt.Sprintf(format, args...))
}
