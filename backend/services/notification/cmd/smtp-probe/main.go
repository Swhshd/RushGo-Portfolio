// smtp-probe sends at most one explicitly authorized message, without database
// writes or retries. Business notifications continue through the durable worker.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/ticketbox/notification/internal/domain"
	"github.com/ticketbox/notification/internal/service"
	"github.com/ticketbox/pkg/authpolicy"
	"github.com/ticketbox/pkg/config"
)

func main() { os.Exit(run()) }
func run() int {
	recipient := flag.String("recipient", "", "explicit user-authorized test mailbox")
	confirm := flag.String("authorize-recipient", "", "repeat authorized mailbox; omitted means no send")
	deployment := flag.String("deployment", "", "expected deployment identity")
	id := flag.String("message-id", "", "new UUID, retained for this single attempt")
	receipt := flag.String("receipt", "", "new private receipt file; must not exist")
	send := flag.Bool("send", false, "send one message; default only validates configuration")
	flag.Parse()
	cfg, err := config.Load()
	if err != nil {
		fmt.Println("FAIL: configuration rejected")
		return 1
	}
	if !cfg.Release.External() || cfg.Release.Environment == "production" || *deployment != cfg.Release.DeploymentIdentity {
		fmt.Println("FAIL: explicit candidate deployment required")
		return 1
	}
	sender, err := service.NewSMTPNotificationSender(service.SMTPConfig{Host: cfg.SMTPHost, Port: cfg.SMTPPort, TLSMode: cfg.SMTPTLSMode, Username: cfg.SMTPUsername, Password: cfg.SMTPPassword, From: cfg.SMTPFrom, FrontendOrigin: cfg.AuthFrontendOrigin, Timeout: 10 * time.Second})
	if err != nil {
		fmt.Println("NOT_CONFIGURED: SMTP configuration invalid")
		return 1
	}
	if !*send {
		fmt.Println("CONFIG_VALID: external delivery NOT_VERIFIED; no message sent")
		return 0
	}
	to, messageID, err := authorizeProbe(*recipient, *confirm, *id, *receipt)
	if err != nil {
		fmt.Println("FAIL: recipient authorization, message UUID and new receipt required")
		return 1
	}
	file, err := os.OpenFile(*receipt, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		fmt.Println("FAIL: receipt unavailable; no message sent")
		return 1
	}
	defer file.Close()
	record := map[string]any{"messageId": messageID.String(), "deploymentIdentity": cfg.Release.DeploymentIdentity, "sourceCommit": cfg.Release.SourceCommit, "status": "PREPARED_NO_AUTOMATIC_RETRY", "delivered": "NOT_VERIFIED"}
	if json.NewEncoder(file).Encode(record) != nil || file.Sync() != nil {
		fmt.Println("FAIL: durable receipt unavailable; no message sent")
		return 1
	}
	err = sender.Send(context.Background(), &domain.Notification{ID: messageID, Type: "smtp_probe", Recipient: to, Locale: "en"})
	status := "PROVIDER_ACCEPTED_NOT_DELIVERED"
	if errors.Is(err, service.ErrDeliveryOutcomeUnknown) {
		status = "UNKNOWN_DO_NOT_RESEND"
	} else if err != nil {
		status = "NOT_ACCEPTED"
	}
	record["status"] = status
	if json.NewEncoder(file).Encode(record) != nil || file.Sync() != nil {
		fmt.Println("FAIL: result receipt unavailable; outcome unknown, do not resend")
		return 1
	}
	fmt.Println(status)
	if err != nil {
		return 1
	}
	return 0
}

func authorizeProbe(recipient, confirmation, id, receipt string) (string, uuid.UUID, error) {
	to, err := authpolicy.CanonicalEmail(recipient)
	messageID, idErr := uuid.Parse(id)
	if err != nil || confirmation != recipient || idErr != nil || messageID == uuid.Nil || receipt == "" {
		return "", uuid.Nil, errors.New("explicit test recipient authorization and new receipt identity required")
	}
	return to, messageID, nil
}
