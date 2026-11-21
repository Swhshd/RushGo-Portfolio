//go:build !ticket_e2e

package kafka

import "context"

func beforeOffsetCommit(context.Context, Event, string, int, int64) error { return nil }

func ArmTicketE2ECommitFault(string) error { return nil }
