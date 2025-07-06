package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/components"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
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

// ShowTaskConfig displays the configuration page for a specific task type
func (h *MaintenanceHandlers) ShowTaskConfig(c *gin.Context) {
	taskTypeName := c.Param("taskType")

	// Get the task type
	taskType := maintenance.GetMaintenanceTaskType(taskTypeName)
	if taskType == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task type not found"})
		return
	}

	// Get the UI provider for this task type
	uiRegistry := tasks.GetGlobalUIRegistry()
	typesRegistry := tasks.GetGlobalTypesRegistry()

	var provider types.TaskUIProvider
	for workerTaskType := range typesRegistry.GetAllDetectors() {
		if string(workerTaskType) == string(taskType) {
			provider = uiRegistry.GetProvider(workerTaskType)
			break
		}
	}

	if provider == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "UI provider not found for task type"})
		return
	}

	// Try to get templ UI provider first
	templUIProvider := getTemplUIProvider(taskType)
	var configSections []components.ConfigSectionData

	if templUIProvider != nil {
		// Use the new templ-based UI provider
		currentConfig := templUIProvider.GetCurrentConfig()
		sections, err := templUIProvider.RenderConfigSections(currentConfig)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render configuration sections: " + err.Error()})
			return
		}
		configSections = sections
	} else {
		// Fallback to basic configuration for providers that haven't been migrated yet
		configSections = []components.ConfigSectionData{
			{
				Title:       "Configuration Settings",
				Icon:        "fas fa-cogs",
				Description: "Configure task detection and scheduling parameters",
				Fields: []interface{}{
					components.CheckboxFieldData{
						FormFieldData: components.FormFieldData{
							Name:        "enabled",
							Label:       "Enable Task",
							Description: "Whether this task type should be enabled",
						},
						Checked: true,
					},
					components.NumberFieldData{
						FormFieldData: components.FormFieldData{
							Name:        "max_concurrent",
							Label:       "Max Concurrent Tasks",
							Description: "Maximum number of concurrent tasks",
							Required:    true,
						},
						Value: 2,
						Step:  "1",
						Min:   floatPtr(1),
					},
					components.DurationFieldData{
						FormFieldData: components.FormFieldData{
							Name:        "scan_interval",
							Label:       "Scan Interval",
							Description: "How often to scan for tasks",
							Required:    true,
						},
						Value: "30m",
					},
				},
			},
		}
	}

	// Create task configuration data using templ components
	configData := &app.TaskConfigTemplData{
		TaskType:       taskType,
		TaskName:       provider.GetDisplayName(),
		TaskIcon:       provider.GetIcon(),
		Description:    provider.GetDescription(),
		ConfigSections: configSections,
	}

	// Render HTML template using templ components
	c.Header("Content-Type", "text/html")
	taskConfigComponent := app.TaskConfigTempl(configData)
	layoutComponent := layout.Layout(c, taskConfigComponent)
	err := layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// UpdateTaskConfig updates configuration for a specific task type
func (h *MaintenanceHandlers) UpdateTaskConfig(c *gin.Context) {
	taskTypeName := c.Param("taskType")

	// Get the task type
	taskType := maintenance.GetMaintenanceTaskType(taskTypeName)
	if taskType == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task type not found"})
		return
	}

	// Try to get templ UI provider first
	templUIProvider := getTemplUIProvider(taskType)

	// Parse form data
	err := c.Request.ParseForm()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse form data: " + err.Error()})
		return
	}

	// Convert form data to map
	formData := make(map[string][]string)
	for key, values := range c.Request.PostForm {
		formData[key] = values
	}

	var config interface{}

	if templUIProvider != nil {
		// Use the new templ-based UI provider
		config, err = templUIProvider.ParseConfigForm(formData)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse configuration: " + err.Error()})
			return
		}

		// Apply configuration using templ provider
		err = templUIProvider.ApplyConfig(config)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to apply configuration: " + err.Error()})
			return
		}
	} else {
		// Fallback to old UI provider for tasks that haven't been migrated yet
		uiRegistry := tasks.GetGlobalUIRegistry()
		typesRegistry := tasks.GetGlobalTypesRegistry()

		var provider types.TaskUIProvider
		for workerTaskType := range typesRegistry.GetAllDetectors() {
			if string(workerTaskType) == string(taskType) {
				provider = uiRegistry.GetProvider(workerTaskType)
				break
			}
		}

		if provider == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "UI provider not found for task type"})
			return
		}

		// Parse configuration from form using old provider
		config, err = provider.ParseConfigForm(formData)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse configuration: " + err.Error()})
			return
		}

		// Apply configuration using old provider
		err = provider.ApplyConfig(config)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to apply configuration: " + err.Error()})
			return
		}
	}

	// Redirect back to task configuration page
	c.Redirect(http.StatusSeeOther, "/maintenance/config/"+taskTypeName)
}

// UpdateMaintenanceConfig updates maintenance configuration from form
func (h *MaintenanceHandlers) UpdateMaintenanceConfig(c *gin.Context) {
	var config maintenance.MaintenanceConfig
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

func (h *MaintenanceHandlers) getMaintenanceQueueData() (*maintenance.MaintenanceQueueData, error) {
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

	return &maintenance.MaintenanceQueueData{
		Tasks:       tasks,
		Workers:     workers,
		Stats:       stats,
		LastUpdated: time.Now(),
	}, nil
}

func (h *MaintenanceHandlers) getMaintenanceQueueStats() (*maintenance.QueueStats, error) {
	// This would integrate with the maintenance queue to get real statistics
	// For now, return mock data
	return &maintenance.QueueStats{
		PendingTasks:   5,
		RunningTasks:   2,
		CompletedToday: 15,
		FailedToday:    1,
		TotalTasks:     23,
	}, nil
}

func (h *MaintenanceHandlers) getMaintenanceTasks() ([]*maintenance.MaintenanceTask, error) {
	// This would integrate with the maintenance queue to get real tasks
	// For now, return mock data
	return []*maintenance.MaintenanceTask{}, nil
}

func (h *MaintenanceHandlers) getMaintenanceWorkers() ([]*maintenance.MaintenanceWorker, error) {
	// This would integrate with the maintenance system to get real workers
	// For now, return mock data
	return []*maintenance.MaintenanceWorker{}, nil
}

func (h *MaintenanceHandlers) getMaintenanceConfig() (*maintenance.MaintenanceConfigData, error) {
	// Delegate to AdminServer's real persistence method
	return h.adminServer.GetMaintenanceConfigData()
}

func (h *MaintenanceHandlers) updateMaintenanceConfig(config *maintenance.MaintenanceConfig) error {
	// Delegate to AdminServer's real persistence method
	return h.adminServer.UpdateMaintenanceConfigData(config)
}

// floatPtr is a helper function to create float64 pointers
func floatPtr(f float64) *float64 {
	return &f
}

// Global templ UI registry
var globalTemplUIRegistry *types.UITemplRegistry

// initTemplUIRegistry initializes the global templ UI registry
func initTemplUIRegistry() {
	if globalTemplUIRegistry == nil {
		globalTemplUIRegistry = types.NewUITemplRegistry()

		// Register vacuum templ UI provider using shared instances
		vacuumDetector, vacuumScheduler := vacuum.GetSharedInstances()
		vacuum.RegisterUITempl(globalTemplUIRegistry, vacuumDetector, vacuumScheduler)

		// Register erasure coding templ UI provider using shared instances
		erasureCodingDetector, erasureCodingScheduler := erasure_coding.GetSharedInstances()
		erasure_coding.RegisterUITempl(globalTemplUIRegistry, erasureCodingDetector, erasureCodingScheduler)

		// Register balance templ UI provider using shared instances
		balanceDetector, balanceScheduler := balance.GetSharedInstances()
		balance.RegisterUITempl(globalTemplUIRegistry, balanceDetector, balanceScheduler)
	}
}

// getTemplUIProvider gets the templ UI provider for a task type
func getTemplUIProvider(taskType maintenance.MaintenanceTaskType) types.TaskUITemplProvider {
	initTemplUIRegistry()

	// Convert maintenance task type to worker task type
	typesRegistry := tasks.GetGlobalTypesRegistry()
	for workerTaskType := range typesRegistry.GetAllDetectors() {
		if string(workerTaskType) == string(taskType) {
			return globalTemplUIRegistry.GetProvider(workerTaskType)
		}
	}

	return nil
}
