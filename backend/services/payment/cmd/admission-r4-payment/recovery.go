//go:build admission_r4

package main

import (
	"context"
	"sync"
	"time"

	"github.com/ticketbox/payment/internal/service"
	"go.uber.org/zap"
)

type admissionRecovery struct {
	mu      sync.Mutex
	paused  bool
	running bool
	idle    chan struct{}
	service *service.AdmissionCoordinator
	logger  *zap.Logger
}

func newAdmissionRecovery(coordinator *service.AdmissionCoordinator, logger *zap.Logger) *admissionRecovery {
	idle := make(chan struct{})
	close(idle)
	return &admissionRecovery{idle: idle, service: coordinator, logger: logger}
}

func (r *admissionRecovery) run(ctx context.Context, force bool) (int, error) {
	r.mu.Lock()
	if r.running || (r.paused && !force) {
		r.mu.Unlock()
		return 0, nil
	}
	r.running = true
	r.idle = make(chan struct{})
	r.mu.Unlock()

	processed, err := r.service.RunAdmissionRecoveryOnce(ctx)

	r.mu.Lock()
	r.running = false
	close(r.idle)
	r.mu.Unlock()
	return processed, err
}

func (r *admissionRecovery) pause(ctx context.Context) error {
	r.mu.Lock()
	r.paused = true
	idle := r.idle
	r.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *admissionRecovery) unpause() {
	r.mu.Lock()
	r.paused = false
	r.mu.Unlock()
}

func (r *admissionRecovery) isPaused() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.paused
}

func (r *admissionRecovery) start(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recoveryCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
			_, err := r.run(recoveryCtx, false)
			cancel()
			if err != nil && ctx.Err() == nil {
				r.logger.Warn("test-only admission recovery returned an error")
			}
		}
	}
}
