package service

import (
	"fmt"
	"github.com/ticketbox/user/internal/domain"
	"net/url"
	"strings"
)

func authMailTemplate(input AuthMail) (string, string, error) {
	switch input.Purpose {
	case RegistrationMail, EmailChangeMail:
		if len(input.Code) != 6 || input.ResetURL != "" {
			return "", "", domain.ErrValidation
		}
		for _, r := range input.Code {
			if r < '0' || r > '9' {
				return "", "", domain.ErrValidation
			}
		}
		purpose := "verification"
		if input.Purpose == EmailChangeMail {
			purpose = "email change"
		}
		return "Your RushGo " + purpose + " code", fmt.Sprintf("Your %s code is %s. It expires in 10 minutes. If you did not request this, ignore this message.", purpose, input.Code), nil
	case ResetMail:
		u, err := url.Parse(input.ResetURL)
		if err != nil || input.Code != "" || strings.ContainsAny(input.ResetURL, "\r\n") || u.Path != "/reset-password" || !strings.HasPrefix(u.Fragment, "token=") {
			return "", "", domain.ErrValidation
		}
		origin := *u
		origin.Path = ""
		origin.Fragment = ""
		if _, err = ValidateFrontendOrigin(origin.String()); err != nil {
			return "", "", domain.ErrValidation
		}
		return "Reset your RushGo password", "Use this link to reset your password within 20 minutes:\n" + input.ResetURL + "\nIf you did not request this, ignore this message.", nil
	default:
		return "", "", domain.ErrValidation
	}
}
