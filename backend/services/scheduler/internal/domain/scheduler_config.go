package domain

import (
	"time"

	"github.com/google/uuid"
)

type JobType string

const JobTypeReservationCleaner JobType = "reservation-cleaner-job"
const JobTypeLegacyUnsupported JobType = "legacy-unsupported"

type SchedulerConfig struct {
	Id                 uuid.UUID
	Name               string
	JobType            JobType
	IsEnabled          bool
	IntervalExpression string
	Timeout            time.Duration
	Version            int
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Valid              bool
	ValidationError    string
}

// type SchedulerConfigChangedPayload struct {
// 	IsEnable           bool
// 	IntervalExpression string
// }
