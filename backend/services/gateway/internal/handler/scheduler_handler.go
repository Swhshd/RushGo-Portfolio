package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	schedulerv1 "github.com/ticketbox/pkg/proto/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SchedulerHandler struct {
	schedulerClient schedulerv1.SchedulerServiceClient
}

func NewSchedulerHandler(schedulerClient schedulerv1.SchedulerServiceClient) *SchedulerHandler {
	return &SchedulerHandler{schedulerClient: schedulerClient}
}

func (h *SchedulerHandler) ListSchedulerJobs(c *gin.Context) {
	resp, err := h.schedulerClient.ListActiveSchedulers(c.Request.Context(), &schedulerv1.ListActiveSchedulersRequest{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list scheduler jobs"})
		return
	}

	jobs := make([]gin.H, 0, len(resp.Schedulers))
	for _, j := range resp.Schedulers {
		jobs = append(jobs, gin.H{
			"id":               j.Id,
			"name":             j.Name,
			"job_type":         j.JobType,
			"cron_expression":  j.CronExpression,
			"is_enabled":       j.IsEnabled,
			"version":          j.Version,
			"timeout":          j.Timeout,
			"valid":            j.Valid,
			"validation_error": j.ValidationError,
			"created_at":       j.CreatedAt.AsTime().Format(time.RFC3339),
			"updated_at":       j.UpdatedAt.AsTime().Format(time.RFC3339),
		})
	}

	c.JSON(http.StatusOK, gin.H{"schedulers": jobs})
}

func (h *SchedulerHandler) UpdateSchedulerJob(c *gin.Context) {
	var req struct {
		IsEnabled       bool   `json:"is_enable"`
		CronExpression  string `json:"cron_expression" binding:"required"`
		Timeout         int32  `json:"timeout" binding:"min=1,max=600"`
		ExpectedVersion int32  `json:"expected_version" binding:"min=1"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	_, err := h.schedulerClient.UpdateSchedulerById(c.Request.Context(), &schedulerv1.UpdateSchedulerByIdRequest{
		Id:              c.Param("id"),
		IsEnable:        req.IsEnabled,
		CronExpression:  req.CronExpression,
		Timeout:         req.Timeout,
		ExpectedVersion: req.ExpectedVersion,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update scheduler job", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "scheduler job updated"})
}

func (h *SchedulerHandler) CreateSchedulerJob(c *gin.Context) {
	var req struct {
		Name           string `json:"name" binding:"required,max=100"`
		JobType        string `json:"job_type" binding:"required"`
		CronExpression string `json:"cron_expression" binding:"required,max=50"`
		Timeout        int32  `json:"timeout" binding:"min=1,max=600"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid scheduler job configuration"})
		return
	}
	created, err := h.schedulerClient.CreateSchedulerConfig(c.Request.Context(), &schedulerv1.CreateSchedulerConfigRequest{
		Name: req.Name, JobType: req.JobType, CronExpression: req.CronExpression, Timeout: req.Timeout,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			c.JSON(http.StatusConflict, gin.H{"error": "a scheduler job with this name already exists"})
			return
		}
		if status.Code(err) == codes.InvalidArgument {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid scheduler job configuration"})
			return
		}
		if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "scheduler service is temporarily unavailable"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create scheduler job"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"scheduler": gin.H{
		"id": created.Id, "name": created.Name, "job_type": created.JobType,
		"cron_expression": created.CronExpression, "is_enabled": created.IsEnabled,
		"version": created.Version, "timeout": created.Timeout,
		"valid":            created.Valid,
		"validation_error": created.ValidationError,
		"created_at":       created.CreatedAt.AsTime().Format(time.RFC3339),
		"updated_at":       created.UpdatedAt.AsTime().Format(time.RFC3339),
	}})
}
