package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
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
		glog.Infof("DEBUG ShowMaintenanceQueue: error getting data: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	glog.Infof("DEBUG ShowMaintenanceQueue: got data with %d tasks", len(data.Tasks))
	if data.Stats != nil {
		glog.Infof("DEBUG ShowMaintenanceQueue: stats = {pending: %d, running: %d, completed: %d}",
			data.Stats.PendingTasks, data.Stats.RunningTasks, data.Stats.CompletedToday)
	} else {
		glog.Infof("DEBUG ShowMaintenanceQueue: stats is nil")
	}

	// Render HTML template
	c.Header("Content-Type", "text/html")
	maintenanceComponent := app.MaintenanceQueue(data)
	layoutComponent := layout.Layout(c, maintenanceComponent)
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		glog.Infof("DEBUG ShowMaintenanceQueue: render error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}

	glog.Infof("DEBUG ShowMaintenanceQueue: template rendered successfully")
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
	// DEBUG: Log that this method is being called
	glog.Infof("DEBUG: MaintenanceHandlers.ShowMaintenanceConfig called")

	config, err := h.getMaintenanceConfig()
	if err != nil {
		glog.Errorf("DEBUG: Error getting maintenance config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Get the schema for dynamic form rendering
	schema := maintenance.GetMaintenanceConfigSchema()
	glog.Infof("DEBUG: Got schema with %d fields", len(schema.Fields))

	// Render HTML template using schema-driven approach
	c.Header("Content-Type", "text/html")
	configComponent := app.MaintenanceConfigSchema(config, schema)
	layoutComponent := layout.Layout(c, configComponent)
	glog.Infof("DEBUG: About to render MaintenanceConfigSchema template")
	err = layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		glog.Errorf("DEBUG: Template render error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
	glog.Infof("DEBUG: MaintenanceConfigSchema template rendered successfully")
}

// ShowTaskConfig displays the configuration page for a specific task type
func (h *MaintenanceHandlers) ShowTaskConfig(c *gin.Context) {
	taskTypeName := c.Param("taskType")

	// Get the schema for this task type
	schema := tasks.GetTaskConfigSchema(taskTypeName)
	if schema == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task type not found or no schema available"})
		return
	}

	// Get the UI provider for current configuration
	uiRegistry := tasks.GetGlobalUIRegistry()
	typesRegistry := tasks.GetGlobalTypesRegistry()

	var provider types.TaskUIProvider
	for workerTaskType := range typesRegistry.GetAllDetectors() {
		if string(workerTaskType) == taskTypeName {
			provider = uiRegistry.GetProvider(workerTaskType)
			break
		}
	}

	if provider == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "UI provider not found for task type"})
		return
	}

	// Get current configuration
	currentConfig := provider.GetCurrentConfig()

	// Apply schema defaults to ensure all fields have values
	if err := schema.ApplyDefaults(currentConfig); err != nil {
		glog.Errorf("Failed to apply schema defaults for %s: %v", taskTypeName, err)
	}

	// Create task configuration data
	configData := &maintenance.TaskConfigData{
		TaskType:    maintenance.MaintenanceTaskType(taskTypeName),
		TaskName:    schema.DisplayName,
		TaskIcon:    schema.Icon,
		Description: schema.Description,
	}

	// Render HTML template using schema-based approach
	c.Header("Content-Type", "text/html")
	taskConfigComponent := app.TaskConfigSchema(configData, schema, currentConfig)
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

	// Try to get templ UI provider first - temporarily disabled
	// templUIProvider := getTemplUIProvider(taskType)

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

	// Temporarily disabled templ UI provider
	// if templUIProvider != nil {
	//	// Use the new templ-based UI provider
	//	config, err = templUIProvider.ParseConfigForm(formData)
	//	if err != nil {
	//		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse configuration: " + err.Error()})
	//		return
	//	}
	//	// Apply configuration using templ provider
	//	err = templUIProvider.ApplyConfig(config)
	//	if err != nil {
	//		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to apply configuration: " + err.Error()})
	//		return
	//	}
	// } else {
	// Fallback to old UI provider for tasks that haven't been migrated yet
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
	// } // End of disabled templ UI provider else block

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

	data := &maintenance.MaintenanceQueueData{
		Tasks:       tasks,
		Workers:     workers,
		Stats:       stats,
		LastUpdated: time.Now(),
	}

	return data, nil
}

func (h *MaintenanceHandlers) getMaintenanceQueueStats() (*maintenance.QueueStats, error) {
	// Use the exported method from AdminServer
	return h.adminServer.GetMaintenanceQueueStats()
}

func (h *MaintenanceHandlers) getMaintenanceTasks() ([]*maintenance.MaintenanceTask, error) {
	// Call the maintenance manager directly to get all tasks
	if h.adminServer == nil {
		return []*maintenance.MaintenanceTask{}, nil
	}

	manager := h.adminServer.GetMaintenanceManager()
	if manager == nil {
		return []*maintenance.MaintenanceTask{}, nil
	}

	// Get ALL tasks using empty parameters - this should match what the API returns
	allTasks := manager.GetTasks("", "", 0)
	return allTasks, nil
}

func (h *MaintenanceHandlers) getMaintenanceWorkers() ([]*maintenance.MaintenanceWorker, error) {
	// Get workers from the admin server's maintenance manager
	if h.adminServer == nil {
		return []*maintenance.MaintenanceWorker{}, nil
	}

	if h.adminServer.GetMaintenanceManager() == nil {
		return []*maintenance.MaintenanceWorker{}, nil
	}

	// Get workers from the maintenance manager
	workers := h.adminServer.GetMaintenanceManager().GetWorkers()
	return workers, nil
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

// Global templ UI registry - temporarily disabled
// var globalTemplUIRegistry *types.UITemplRegistry

// initTemplUIRegistry initializes the global templ UI registry - temporarily disabled
func initTemplUIRegistry() {
	// Temporarily disabled due to missing types
	// if globalTemplUIRegistry == nil {
	//	globalTemplUIRegistry = types.NewUITemplRegistry()
	//	// Register vacuum templ UI provider using shared instances
	//	vacuumDetector, vacuumScheduler := vacuum.GetSharedInstances()
	//	vacuum.RegisterUITempl(globalTemplUIRegistry, vacuumDetector, vacuumScheduler)
	//	// Register erasure coding templ UI provider using shared instances
	//	erasureCodingDetector, erasureCodingScheduler := erasure_coding.GetSharedInstances()
	//	erasure_coding.RegisterUITempl(globalTemplUIRegistry, erasureCodingDetector, erasureCodingScheduler)
	//	// Register balance templ UI provider using shared instances
	//	balanceDetector, balanceScheduler := balance.GetSharedInstances()
	//	balance.RegisterUITempl(globalTemplUIRegistry, balanceDetector, balanceScheduler)
	// }
}

// getTemplUIProvider gets the templ UI provider for a task type - temporarily disabled
func getTemplUIProvider(taskType maintenance.MaintenanceTaskType) interface{} {
	// initTemplUIRegistry()
	// Convert maintenance task type to worker task type
	// typesRegistry := tasks.GetGlobalTypesRegistry()
	// for workerTaskType := range typesRegistry.GetAllDetectors() {
	//	if string(workerTaskType) == string(taskType) {
	//		return globalTemplUIRegistry.GetProvider(workerTaskType)
	//	}
	// }
	return nil
}
