package domain

import "errors"

// IssuanceFailure preserves the original cause for errors.Is/As without placing
// raw RPC messages, SQL values or credentials in its printable representation.
type IssuanceFailure struct {
	Stage  string
	Reason string
	Cause  error
}

func (e *IssuanceFailure) Error() string { return "ticket issuance failed at " + e.Stage }
func (e *IssuanceFailure) Unwrap() error { return e.Cause }
func WrapIssuanceFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	var existing *IssuanceFailure
	if errors.As(err, &existing) {
		return err
	}
	return &IssuanceFailure{Stage: stage, Cause: err}
}
