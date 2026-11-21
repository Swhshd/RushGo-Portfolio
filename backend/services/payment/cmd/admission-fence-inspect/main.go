package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/ticketbox/payment/internal/admissioninspect"
)

// Set at build time with:
// -ldflags "-X github.com/ticketbox/payment/cmd/admission-fence-inspect.sourceVersion=<version>"
var sourceVersion = "development"

func main() { os.Exit(run()) }

func run() int {
	flags := flag.NewFlagSet("admission-fence-inspect", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	source := flags.String("source", admissioninspect.SourceBoth, "database to inspect: both, payment, or booking")
	if err := flags.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printUsage(os.Stdout)
			return 0
		}
		if !writeJSON(os.Stdout, invalidReport("invalid_arguments")) {
			return 2
		}
		return 2
	}
	if flags.NArg() != 0 || (*source != admissioninspect.SourceBoth && *source != admissioninspect.SourcePayment && *source != admissioninspect.SourceBooking) {
		if !writeJSON(os.Stdout, invalidReport("invalid_arguments")) {
			return 2
		}
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	report := admissioninspect.Inspect(ctx, admissioninspect.Options{
		Source:           *source,
		PaymentDSN:       os.Getenv("PAYMENT_DATABASE_URL"),
		BookingDSN:       os.Getenv("BOOKING_DATABASE_URL"),
		SourceVersion:    sourceVersion,
		TotalTimeout:     10 * time.Second,
		StatementTimeout: 2 * time.Second,
	})
	if !writeJSON(os.Stdout, report) {
		return 2
	}
	if report.HasFailure() {
		return 1
	}
	return 0
}

func invalidReport(status string) admissioninspect.Report {
	return admissioninspect.Report{
		Status:                      status,
		SourceVersion:               sourceVersion,
		CrossDatabaseAtomicSnapshot: false,
		SnapshotNote:                "No database query was run.",
	}
}

func writeJSON(writer io.Writer, value any) bool {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "unable to write diagnostic JSON")
		return false
	}
	return true
}

func printUsage(writer io.Writer) {
	_, _ = fmt.Fprintln(writer, `Usage: admission-fence-inspect [--source both|payment|booking]

Reads selected service databases using read-only PostgreSQL transactions.
Database DSNs are read from PAYMENT_DATABASE_URL and BOOKING_DATABASE_URL.
The report contains separate timestamps and does not form a cross-database snapshot.`)
}
