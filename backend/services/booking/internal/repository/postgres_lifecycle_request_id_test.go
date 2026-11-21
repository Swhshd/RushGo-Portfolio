package repository

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

type lifecycleRequestIDRow struct {
	requestID any
}

func (r lifecycleRequestIDRow) Scan(dest ...any) error {
	values := []any{
		uuid.New(), uuid.New(), uuid.New(), "PENDING", int64(725), int32(1), time.Now(), false,
		int64(1), "OPEN", nil, nil, nil, "usd", "pessimistic", nil, nil, false, nil, r.requestID,
	}
	for index, raw := range values {
		target := reflect.ValueOf(dest[index]).Elem()
		if raw == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		value := reflect.ValueOf(raw)
		if target.Kind() == reflect.Pointer {
			pointed := reflect.New(target.Type().Elem())
			if value.Type().AssignableTo(pointed.Elem().Type()) {
				pointed.Elem().Set(value)
			} else if value.Type().ConvertibleTo(pointed.Elem().Type()) {
				pointed.Elem().Set(value.Convert(pointed.Elem().Type()))
			} else {
				return &reflect.ValueError{Method: "Scan", Kind: value.Kind()}
			}
			target.Set(pointed)
			continue
		}
		if value.Type().AssignableTo(target.Type()) {
			target.Set(value)
		} else if value.Type().ConvertibleTo(target.Type()) {
			target.Set(value.Convert(target.Type()))
		} else {
			return &reflect.ValueError{Method: "Scan", Kind: value.Kind()}
		}
	}
	return nil
}

func TestScanLifecycleRequestIDNullableProjection(t *testing.T) {
	for _, test := range []struct {
		name string
		id   any
		want string
	}{
		{name: "present", id: "request-abc", want: "request-abc"},
		{name: "legacy null", id: nil, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, _, _, err := scanLifecycle(lifecycleRequestIDRow{requestID: test.id})
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Booking.RequestID != test.want {
				t.Fatalf("request_id projection = %q, want %q", snapshot.Booking.RequestID, test.want)
			}
		})
	}
}
