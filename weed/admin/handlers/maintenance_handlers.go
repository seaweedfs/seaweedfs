package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// MaintenanceHandlers handles maintenance-related HTTP requests
type MaintenanceHandlers struct {
	adminServer *dash.AdminServer
}

// NewMaintenanceHandlers creates a new instance of MaintenanceHandlers
func NewMaintenanceHandlers(adminServer *dash.AdminServer) *MaintenanceHandlers {
	return &MaintenanceHandlers{
		adminServer: adminServer,
	}
}

// ShowMaintenanceQueue displays the maintenance queue page
func (h *MaintenanceHandlers) ShowMaintenanceQueue(c *gin.Context) {
	data, err := h.getMaintenanceQueueData()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Render HTML template
	c.Header("Content-Type", "text/html")
	maintenanceComponent := app.MaintenanceQueue(data)
	layoutComponent := layout.Layout(c, maintenanceComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowMaintenanceWorkers displays the maintenance workers page
func (h *MaintenanceHandlers) ShowMaintenanceWorkers(c *gin.Context) {
	workersData, err := h.adminServer.GetMaintenanceWorkersData()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Render HTML template
	c.Header("Content-Type", "text/html")
	workersComponent := app.MaintenanceWorkers(workersData)
	layoutComponent := layout.Layout(c, workersComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowMaintenanceConfig displays the maintenance configuration page
func (h *MaintenanceHandlers) ShowMaintenanceConfig(c *gin.Context) {
	config, err := h.getMaintenanceConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Render HTML template
	c.Header("Content-Type", "text/html")
	configComponent := app.MaintenanceConfig(config)
	layoutComponent := layout.Layout(c, configComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// UpdateMaintenanceConfig updates maintenance configuration from form
func (h *MaintenanceHandlers) UpdateMaintenanceConfig(c *gin.Context) {
	var config dash.MaintenanceConfig
	if err := c.ShouldBind(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := h.updateMaintenanceConfig(&config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Redirect(http.StatusSeeOther, "/maintenance/config")
}

// Helper methods that delegate to AdminServer

func (h *MaintenanceHandlers) getMaintenanceQueueData() (*dash.MaintenanceQueueData, error) {
	tasks, err := h.getMaintenanceTasks()
	if err != nil {
		return nil, err
	}

	workers, err := h.getMaintenanceWorkers()
	if err != nil {
		return nil, err
	}

	stats, err := h.getMaintenanceQueueStats()
	if err != nil {
		return nil, err
	}

	return &dash.MaintenanceQueueData{
		Tasks:       tasks,
		Workers:     workers,
		Stats:       stats,
		LastUpdated: time.Now(),
	}, nil
}

func (h *MaintenanceHandlers) getMaintenanceQueueStats() (*dash.QueueStats, error) {
	// This would integrate with the maintenance queue to get real statistics
	// For now, return mock data
	return &dash.QueueStats{
		PendingTasks:   5,
		RunningTasks:   2,
		CompletedToday: 15,
		FailedToday:    1,
		TotalTasks:     23,
	}, nil
}

func (h *MaintenanceHandlers) getMaintenanceTasks() ([]*dash.MaintenanceTask, error) {
	// This would integrate with the maintenance queue to get real tasks
	// For now, return mock data
	return []*dash.MaintenanceTask{}, nil
}

func (h *MaintenanceHandlers) getMaintenanceWorkers() ([]*dash.MaintenanceWorker, error) {
	// This would integrate with the maintenance system to get real workers
	// For now, return mock data
	return []*dash.MaintenanceWorker{}, nil
}

func (h *MaintenanceHandlers) getMaintenanceConfig() (*dash.MaintenanceConfigData, error) {
	// Delegate to AdminServer's real persistence method
	return h.adminServer.GetMaintenanceConfigData()
}

func (h *MaintenanceHandlers) updateMaintenanceConfig(config *dash.MaintenanceConfig) error {
	// Delegate to AdminServer's real persistence method
	return h.adminServer.UpdateMaintenanceConfigData(config)
}
