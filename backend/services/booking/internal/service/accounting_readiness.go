package service

import "errors"

// ErrAccountingNotReady is returned before a new Booking is persisted when the
// Event service cannot confirm that the tier's accounting evidence is ready
// for a sale.
var ErrAccountingNotReady = errors.New("accounting_not_ready")
