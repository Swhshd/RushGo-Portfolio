package domain

import (
	"testing"
	"time"
)

func TestPaymentEvidenceFreshnessBoundary(t *testing.T) {
	decisionAt := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		observedAt time.Time
		decisionAt time.Time
		want       bool
	}{
		{name: "same instant", observedAt: decisionAt, decisionAt: decisionAt, want: true},
		{name: "five seconds", observedAt: decisionAt.Add(-5 * time.Second), decisionAt: decisionAt, want: true},
		{name: "exact limit", observedAt: decisionAt.Add(-MaxPaymentEvidenceAge), decisionAt: decisionAt, want: true},
		{name: "over limit", observedAt: decisionAt.Add(-MaxPaymentEvidenceAge - time.Nanosecond), decisionAt: decisionAt, want: false},
		{name: "future observation", observedAt: decisionAt.Add(time.Nanosecond), decisionAt: decisionAt, want: false},
		{name: "missing observation", observedAt: time.Time{}, decisionAt: decisionAt, want: false},
		{name: "missing decision time", observedAt: decisionAt, decisionAt: time.Time{}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PaymentEvidenceFreshAt(tt.observedAt, tt.decisionAt); got != tt.want {
				t.Fatalf("PaymentEvidenceFreshAt(%v, %v) = %t, want %t", tt.observedAt, tt.decisionAt, got, tt.want)
			}
		})
	}
}

func TestPaymentEvidenceElapsedBoundsRejectStaleEvidence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		monotonicElapsed time.Duration
		wallElapsed      time.Duration
		want             bool
	}{
		{name: "zero age", monotonicElapsed: 0, wallElapsed: 0, want: true},
		{name: "exact limit", monotonicElapsed: MaxPaymentEvidenceAge, wallElapsed: MaxPaymentEvidenceAge, want: true},
		{name: "monotonic over limit", monotonicElapsed: MaxPaymentEvidenceAge + time.Nanosecond, wallElapsed: time.Second, want: false},
		{name: "wall over limit after suspend", monotonicElapsed: 2 * time.Second, wallElapsed: 61 * time.Second, want: false},
		{name: "negative monotonic age", monotonicElapsed: -time.Nanosecond, wallElapsed: time.Second, want: false},
		{name: "negative wall age", monotonicElapsed: time.Second, wallElapsed: -time.Nanosecond, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := PaymentEvidenceElapsedWithinBound(tc.monotonicElapsed, tc.wallElapsed); got != tc.want {
				t.Fatalf("PaymentEvidenceElapsedWithinBound(%s, %s) = %t, want %t", tc.monotonicElapsed, tc.wallElapsed, got, tc.want)
			}
		})
	}
}
