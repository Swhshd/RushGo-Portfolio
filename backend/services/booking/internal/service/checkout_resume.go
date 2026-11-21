package service

import (
	"context"
	"github.com/google/uuid"
	"github.com/ticketbox/booking/internal/domain"
	"github.com/ticketbox/booking/internal/repository"
	paymentv1 "github.com/ticketbox/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

func (s *BookingService) SetCheckoutPaymentClient(client paymentv1.PaymentServiceClient) {
	s.checkoutPayment = client
}

// ResumeBooking preserves the original Booking, generation and Payment identity.
func (s *BookingService) ResumeBooking(ctx context.Context, bookingID, userID uuid.UUID) (*CreateBookingResponse, error) {
	v, err := s.bookingRepo.GetLifecycle(ctx, domain.LifecycleRef{BookingID: bookingID, Generation: 1})
	if err != nil {
		return nil, err
	}
	if v == nil || v.Booking == nil || v.Booking.UserID != userID {
		return nil, repository.ErrNotFound
	}
	response := &CreateBookingResponse{Booking: v.Booking, CheckoutState: "processing", CreationState: "starting", RetryAfterSeconds: 1, ReservationDeadline: v.ReservationDeadline}
	if v.Phase == "TERMINAL" {
		response.CheckoutState = "completed"
		response.CreationState = "terminal"
		response.RetryAfterSeconds = 0
		if v.Booking.Status == domain.StatusExpired {
			response.CheckoutState = "expired"
		}
		return response, nil
	}
	if v.ReservationDeadline == nil || !v.ReservationDeadline.After(time.Now()) {
		response.CheckoutState = "expired"
		return response, nil
	}
	if v.Phase != "OPEN" {
		return response, nil
	}
	if s.checkoutPayment == nil {
		return nil, status.Error(codes.Unavailable, "payment authority unavailable")
	}
	payment, err := s.checkoutPayment.ResumePayment(ctx, &paymentv1.FinancialPaymentRequest{BookingId: bookingID.String(), Generation: v.Generation})
	if status.Code(err) == codes.NotFound {
		return response, nil
	}
	if err != nil || payment == nil {
		return nil, status.Error(codes.Unavailable, "payment authority unavailable")
	}
	latest, err := s.bookingRepo.GetLifecycle(ctx, domain.LifecycleRef{BookingID: bookingID, Generation: v.Generation})
	if err != nil {
		return nil, err
	}
	response.Booking = latest.Booking
	if latest.Phase != "OPEN" || latest.ReservationDeadline == nil || !latest.ReservationDeadline.After(time.Now()) {
		return response, nil
	}
	if payment.CreateState == "ready" && payment.PaymentIntentClientSecret != "" {
		response.CheckoutState = "ready"
		response.CreationState = "ready"
		response.PaymentClientSecret = payment.PaymentIntentClientSecret
		response.CanCancel = true
		response.RetryAfterSeconds = 0
	}
	return response, nil
}
