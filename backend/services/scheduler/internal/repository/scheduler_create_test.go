package repository

import (
	"testing"
	"time"

	"github.com/ticketbox/scheduler/internal/domain"
)

func TestValidateCreateConfigOnlyAllowsRegisteredSafeTaskTypes(t *testing.T) {
	cases := []struct {
		name string
		cfg  domain.SchedulerConfig
		want bool
	}{
		{name: "registered reservation cleaner", cfg: domain.SchedulerConfig{Name: "nightly-cleaner", JobType: domain.JobTypeReservationCleaner, Timeout: time.Second, IntervalExpression: "*/30 * * * * *"}, want: true},
		{name: "unknown type", cfg: domain.SchedulerConfig{Name: "arbitrary", JobType: "shell", Timeout: time.Second, IntervalExpression: "*/30 * * * * *"}},
		{name: "invalid name", cfg: domain.SchedulerConfig{Name: "../cleanup", JobType: domain.JobTypeReservationCleaner, Timeout: time.Second, IntervalExpression: "*/30 * * * * *"}},
		{name: "uppercase name", cfg: domain.SchedulerConfig{Name: "Nightly-Cleaner", JobType: domain.JobTypeReservationCleaner, Timeout: time.Second, IntervalExpression: "*/30 * * * * *"}},
		{name: "noncanonical whitespace", cfg: domain.SchedulerConfig{Name: " nightly-cleaner ", JobType: domain.JobTypeReservationCleaner, Timeout: time.Second, IntervalExpression: "*/30 * * * * *"}},
		{name: "invalid cron", cfg: domain.SchedulerConfig{Name: "nightly-cleaner", JobType: domain.JobTypeReservationCleaner, Timeout: time.Second, IntervalExpression: "60 * * * * *"}},
		{name: "timeout below range", cfg: domain.SchedulerConfig{Name: "nightly-cleaner", JobType: domain.JobTypeReservationCleaner, Timeout: 0, IntervalExpression: "*/30 * * * * *"}},
		{name: "timeout above range", cfg: domain.SchedulerConfig{Name: "nightly-cleaner", JobType: domain.JobTypeReservationCleaner, Timeout: 601 * time.Second, IntervalExpression: "*/30 * * * * *"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCreateConfig(tc.cfg)
			if (err == nil) != tc.want {
				t.Fatalf("ValidateCreateConfig() error = %v, want valid=%v", err, tc.want)
			}
		})
	}
}
