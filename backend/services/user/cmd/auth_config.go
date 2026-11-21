package main

import (
	"errors"
	"github.com/ticketbox/pkg/config"
	"github.com/ticketbox/user/internal/service"
)

func validateUserAuthConfig(c *config.Config) error {
	if c.Release.External() && (c.ServiceName != "user-service" || len(c.JWTSecret) < 32) {
		return errors.New("external User identity or JWT configuration invalid")
	}
	if _, err := service.NewSMTPMailSender(service.SMTPConfig{Host: c.SMTPHost, Port: c.SMTPPort, TLSMode: c.SMTPTLSMode, Username: c.SMTPUsername, Password: c.SMTPPassword, From: c.SMTPFrom}); err != nil {
		return errors.New("invalid auth mail config")
	}
	if c.AuthFrontendOrigin != "" {
		if _, err := service.ValidateFrontendOrigin(c.AuthFrontendOrigin); err != nil {
			return errors.New("invalid frontend origin")
		}
	} else if c.SMTPHost != "" {
		return errors.New("auth frontend origin required")
	}
	return nil
}
