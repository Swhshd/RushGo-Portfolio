package main

import (
	"github.com/gin-gonic/gin"
	"github.com/ticketbox/gateway/internal/middleware"
	"github.com/ticketbox/pkg/config"
)

func validateGatewayProxyConfig(c *config.Config) error {
	return middleware.ConfigureTrustedProxies(gin.New(), c.TrustedProxies)
}
