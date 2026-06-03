package server

import (
	"log"

	"github.com/gin-gonic/gin"

	"mimo2api/internal/auth"
	"mimo2api/internal/config"
)

func SetupRouter() *gin.Engine {
	gin.SetMode(config.GinMode)
	r := gin.Default()
	if err := r.SetTrustedProxies(config.TrustedProxies); err != nil {
		log.Printf("Invalid trusted proxies config: %v", err)
	}

	// CORS middleware
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, x-api-key, api-key")
		c.Header("Access-Control-Max-Age", "86400")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// UI and open endpoints
	r.GET("/", WebUIHandler)
	r.GET("/webui", WebUIHandler)

	uiAPI := r.Group("/api")
	{
		// Public endpoints (no auth required, matching Python WEBUI_PUBLIC_PATHS)
		uiAPI.POST("/auth/login", auth.LoginHandler)
		uiAPI.POST("/auth/logout", auth.LogoutHandler)
		uiAPI.GET("/auth/session", auth.SessionHandler)
		uiAPI.GET("/system/status", SystemStatusHandler)
		uiAPI.GET("/stats", StatsHandler)
		uiAPI.GET("/status/history", StatusHistoryHandler)

		// Protected UI routes
		protected := uiAPI.Group("/")
		protected.Use(auth.WebUIMiddleware())
		{

			protected.GET("/users/list", UsersListHandler)
			protected.POST("/users/add", UsersAddHandler)
			protected.DELETE("/users/delete/:id", UsersDeleteHandler)

			protected.GET("/model_mapping", ModelMappingHandler)
			protected.PUT("/model_mapping", PutModelMappingHandler)
			protected.DELETE("/model_mapping/*name", DeleteModelMappingHandler)
			protected.POST("/rebuild", RebuildHandler)
		}
	}

	// API Endpoints
	api := r.Group("/v1")
	api.Use(auth.APIAuthMiddleware())
	{
		api.POST("/chat/completions", ChatCompletionsHandler)
		api.POST("/messages", ChatCompletionsHandler)
		api.GET("/models", ModelsHandler)
		api.POST("/responses", ResponsesHandler)
	}

	// Anthropic alias
	anthropic := r.Group("/anthropic/v1")
	anthropic.Use(auth.APIAuthMiddleware())
	{
		anthropic.POST("/messages", ChatCompletionsHandler)
		anthropic.GET("/models", AnthropicModelsHandler)
	}

	// WS Bridge (for nodes to connect to if needed, though native claw connects outbound)
	// If bridging is still needed, add it here
	r.GET("/ws", WSTunnelHandler)

	return r
}

func WebUIHandler(c *gin.Context) {
	c.File("webui.html")
}
