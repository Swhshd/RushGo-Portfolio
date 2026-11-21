package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	schedulerv1 "github.com/ticketbox/pkg/proto/scheduler/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type schedulerCreateClientStub struct {
	schedulerv1.SchedulerServiceClient
	request  *schedulerv1.CreateSchedulerConfigRequest
	response *schedulerv1.SchedulerItem
	err      error
	calls    int
}

func (s *schedulerCreateClientStub) CreateSchedulerConfig(_ context.Context, req *schedulerv1.CreateSchedulerConfigRequest, _ ...grpc.CallOption) (*schedulerv1.SchedulerItem, error) {
	s.calls++
	s.request = req
	return s.response, s.err
}

func performSchedulerCreateRequest(client *schedulerCreateClientStub, body string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.POST("/api/scheduler/jobs", NewSchedulerHandler(client).CreateSchedulerJob)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/scheduler/jobs", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(recorder, request)
	return recorder
}

func TestSchedulerHandlerCreateForwardsSafeConfigAndReturnsDisabledRow(t *testing.T) {
	createdAt := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	client := &schedulerCreateClientStub{response: &schedulerv1.SchedulerItem{
		Id: "7efb4f22-4d16-4b79-9112-fd07dc5e622a", Name: "nightly-cleaner", JobType: "reservation-cleaner-job",
		CronExpression: "0 */5 * * * *", IsEnabled: false, Version: 1, Timeout: 30,
		Valid: true, CreatedAt: timestamppb.New(createdAt), UpdatedAt: timestamppb.New(createdAt),
	}}
	response := performSchedulerCreateRequest(client, `{"name":"nightly-cleaner","job_type":"reservation-cleaner-job","cron_expression":"0 */5 * * * *","timeout":30}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("HTTP status = %d, want 201: %s", response.Code, response.Body.String())
	}
	if client.calls != 1 || client.request == nil || client.request.Name != "nightly-cleaner" || client.request.JobType != "reservation-cleaner-job" || client.request.CronExpression != "0 */5 * * * *" || client.request.Timeout != 30 {
		t.Fatalf("scheduler request = %+v, calls=%d", client.request, client.calls)
	}
	var body struct {
		Scheduler struct {
			Name      string `json:"name"`
			JobType   string `json:"job_type"`
			Enabled   bool   `json:"is_enabled"`
			Version   int    `json:"version"`
			Valid     bool   `json:"valid"`
			CreatedAt string `json:"created_at"`
		} `json:"scheduler"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Scheduler.Name != "nightly-cleaner" || body.Scheduler.JobType != "reservation-cleaner-job" || body.Scheduler.Enabled || body.Scheduler.Version != 1 || !body.Scheduler.Valid || body.Scheduler.CreatedAt != createdAt.Format(time.RFC3339) {
		t.Fatalf("created scheduler projection = %+v", body.Scheduler)
	}
}

func TestSchedulerHandlerCreateValidatesAndMapsConflicts(t *testing.T) {
	t.Run("invalid body does not call scheduler", func(t *testing.T) {
		client := &schedulerCreateClientStub{}
		response := performSchedulerCreateRequest(client, `{"name":"../unsafe","job_type":"shell","cron_expression":"* * * * *","timeout":0}`)
		if response.Code != http.StatusBadRequest || client.calls != 0 {
			t.Fatalf("status=%d calls=%d body=%s", response.Code, client.calls, response.Body.String())
		}
	})
	t.Run("existing name is conflict", func(t *testing.T) {
		client := &schedulerCreateClientStub{err: status.Error(codes.AlreadyExists, "duplicate")}
		response := performSchedulerCreateRequest(client, `{"name":"nightly-cleaner","job_type":"reservation-cleaner-job","cron_expression":"0 */5 * * * *","timeout":30}`)
		if response.Code != http.StatusConflict || client.calls != 1 {
			t.Fatalf("status=%d calls=%d body=%s", response.Code, client.calls, response.Body.String())
		}
	})
}
