package testing

import "errors"

// ErrSimulatedError is returned when error simulation is enabled
var ErrSimulatedError = errors.New("simulated plugin error")
