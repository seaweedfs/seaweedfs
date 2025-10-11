package handlers

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
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

// ShowTaskDetail displays the task detail page
func (h *MaintenanceHandlers) ShowTaskDetail(c *gin.Context) {
	taskID := c.Param("id")
	glog.Infof("DEBUG ShowTaskDetail: Starting for task ID: %s", taskID)

	// Add timeout to prevent indefinite hangs when worker is unresponsive
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	// Use a channel to handle timeout for task detail retrieval
	type result struct {
		taskDetail *maintenance.TaskDetailData
		err        error
	}
	resultChan := make(chan result, 1)

	go func() {
		taskDetail, err := h.adminServer.GetMaintenanceTaskDetail(taskID)
		resultChan <- result{taskDetail: taskDetail, err: err}
	}()

	select {
	case res := <-resultChan:
		if res.err != nil {
			glog.Errorf("DEBUG ShowTaskDetail: error getting task detail for %s: %v", taskID, res.err)
			c.String(http.StatusNotFound, "Task not found: %s (Error: %v)", taskID, res.err)
			return
		}

		glog.Infof("DEBUG ShowTaskDetail: got task detail for %s, task type: %s, status: %s", taskID, res.taskDetail.Task.Type, res.taskDetail.Task.Status)

		c.Header("Content-Type", "text/html")
		taskDetailComponent := app.TaskDetail(res.taskDetail)
		layoutComponent := layout.Layout(c, taskDetailComponent)
		err := layoutComponent.Render(ctx, c.Writer)
		if err != nil {
			glog.Errorf("DEBUG ShowTaskDetail: render error: %v", err)
			c.String(http.StatusInternalServerError, "Failed to render template: %v", err)
			return
		}

		glog.Infof("DEBUG ShowTaskDetail: template rendered successfully for task %s", taskID)

	case <-ctx.Done():
		glog.Warningf("ShowTaskDetail: timeout waiting for task detail data for task %s", taskID)
		c.JSON(http.StatusRequestTimeout, gin.H{
			"error":      "Request timeout - task detail retrieval took too long. This may indicate the worker is unresponsive or stuck.",
			"suggestion": "Try refreshing the page or check if the worker executing this task is responsive. If the task is stuck, it may need to be cancelled manually.",
			"task_id":    taskID,
		})
		return
	}
}

// ShowMaintenanceQueue displays the maintenance queue page
func (h *MaintenanceHandlers) ShowMaintenanceQueue(c *gin.Context) {
	// Reduce timeout since we fixed the deadlock issue
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Use a channel to handle timeout for data retrieval
	type result struct {
		data *maintenance.MaintenanceQueueData
		err  error
	}
	resultChan := make(chan result, 1)

	go func() {
		data, err := h.getMaintenanceQueueData()
		resultChan <- result{data: data, err: err}
	}()

	select {
	case res := <-resultChan:
		if res.err != nil {
			glog.V(1).Infof("ShowMaintenanceQueue: error getting data: %v", res.err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": res.err.Error()})
			return
		}

		glog.V(2).Infof("ShowMaintenanceQueue: got data with %d tasks", len(res.data.Tasks))

		// Render HTML template
		c.Header("Content-Type", "text/html")
		maintenanceComponent := app.MaintenanceQueue(res.data)
		layoutComponent := layout.Layout(c, maintenanceComponent)
		err := layoutComponent.Render(ctx, c.Writer)
		if err != nil {
			glog.V(1).Infof("ShowMaintenanceQueue: render error: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
			return
		}

		glog.V(3).Infof("ShowMaintenanceQueue: template rendered successfully")

	case <-ctx.Done():
		glog.Warningf("ShowMaintenanceQueue: timeout waiting for data")
		c.JSON(http.StatusRequestTimeout, gin.H{
			"error":      "Request timeout - maintenance data retrieval took too long. This may indicate a system issue.",
			"suggestion": "Try refreshing the page or contact system administrator if the problem persists.",
		})
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

	// Get the schema for dynamic form rendering
	schema := maintenance.GetMaintenanceConfigSchema()

	// Render HTML template using schema-driven approach
	c.Header("Content-Type", "text/html")
	configComponent := app.MaintenanceConfigSchema(config, schema)
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

	// Note: Do NOT apply schema defaults to current config as it overrides saved values
	// Only apply defaults when creating new configs, not when displaying existing ones

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

// UpdateTaskConfig updates task configuration from form
func (h *MaintenanceHandlers) UpdateTaskConfig(c *gin.Context) {
	taskTypeName := c.Param("taskType")
	taskType := types.TaskType(taskTypeName)

	// Parse form data
	err := c.Request.ParseForm()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse form data: " + err.Error()})
		return
	}

	// Debug logging - show received form data
	glog.V(1).Infof("Received form data for task type %s:", taskTypeName)
	for key, values := range c.Request.PostForm {
		glog.V(1).Infof("  %s: %v", key, values)
	}

	// Get the task configuration schema
	schema := tasks.GetTaskConfigSchema(taskTypeName)
	if schema == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Schema not found for task type: " + taskTypeName})
		return
	}

	// Get the config instance from the UI provider - this is a dynamic approach
	// that doesn't require hardcoding task types
	currentUIRegistry := tasks.GetGlobalUIRegistry()
	currentTypesRegistry := tasks.GetGlobalTypesRegistry()

	var config types.TaskConfig
	var currentProvider types.TaskUIProvider

	// Find the UI provider for this task type
	for workerTaskType := range currentTypesRegistry.GetAllDetectors() {
		if string(workerTaskType) == string(taskType) {
			currentProvider = currentUIRegistry.GetProvider(workerTaskType)
			break
		}
	}

	if currentProvider == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported task type: " + taskTypeName})
		return
	}

	// Get a config instance from the UI provider
	config = currentProvider.GetCurrentConfig()
	if config == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get config for task type: " + taskTypeName})
		return
	}

	// Apply schema defaults - config instances should already have defaults applied during creation
	glog.V(2).Infof("Using config defaults for task type: %s", taskTypeName)

	// Copy current config values (currentProvider is already set above)
	// Apply current values using protobuf directly - no map conversion needed!
	currentPolicy := config.ToTaskPolicy()
	if err := config.FromTaskPolicy(currentPolicy); err != nil {
		glog.Warningf("Failed to load current config for %s: %v", taskTypeName, err)
	}

	// Parse form data using schema-based approach (this will override with new values)
	err = h.parseTaskConfigFromForm(c.Request.PostForm, schema, config)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse configuration: " + err.Error()})
		return
	}

	// Debug logging - config parsed for task type
	glog.V(1).Infof("Parsed configuration for task type: %s", taskTypeName)

	// Validate the configuration
	if validationErrors := schema.ValidateConfig(config); len(validationErrors) > 0 {
		errorMessages := make([]string, len(validationErrors))
		for i, err := range validationErrors {
			errorMessages[i] = err.Error()
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "Configuration validation failed", "details": errorMessages})
		return
	}

	// Apply configuration using UIProvider
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

	// Apply configuration using provider
	err = provider.ApplyTaskConfig(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to apply configuration: " + err.Error()})
		return
	}

	// Save task configuration to protobuf file using ConfigPersistence
	if h.adminServer != nil && h.adminServer.GetConfigPersistence() != nil {
		err = h.saveTaskConfigToProtobuf(taskType, config)
		if err != nil {
			glog.Warningf("Failed to save task config to protobuf file: %v", err)
			// Don't fail the request, just log the warning
		}
	}

	// Trigger a configuration reload in the maintenance manager
	if h.adminServer != nil {
		if manager := h.adminServer.GetMaintenanceManager(); manager != nil {
			err = manager.ReloadTaskConfigurations()
			if err != nil {
				glog.Warningf("Failed to reload task configurations: %v", err)
			} else {
				glog.V(1).Infof("Successfully reloaded task configurations after updating %s", taskTypeName)
			}
		}
	}

	// Redirect back to task configuration page
	c.Redirect(http.StatusSeeOther, "/maintenance/config/"+taskTypeName)
}

// parseTaskConfigFromForm parses form data using schema definitions
func (h *MaintenanceHandlers) parseTaskConfigFromForm(formData map[string][]string, schema *tasks.TaskConfigSchema, config interface{}) error {
	configValue := reflect.ValueOf(config)
	if configValue.Kind() == reflect.Ptr {
		configValue = configValue.Elem()
	}

	if configValue.Kind() != reflect.Struct {
		return fmt.Errorf("config must be a struct or pointer to struct")
	}

	configType := configValue.Type()

	for i := 0; i < configValue.NumField(); i++ {
		field := configValue.Field(i)
		fieldType := configType.Field(i)

		// Handle embedded structs recursively
		if fieldType.Anonymous && field.Kind() == reflect.Struct {
			err := h.parseTaskConfigFromForm(formData, schema, field.Addr().Interface())
			if err != nil {
				return fmt.Errorf("error parsing embedded struct %s: %w", fieldType.Name, err)
			}
			continue
		}

		// Get JSON tag name
		jsonTag := fieldType.Tag.Get("json")
		if jsonTag == "" {
			continue
		}

		// Remove options like ",omitempty"
		if commaIdx := strings.Index(jsonTag, ","); commaIdx > 0 {
			jsonTag = jsonTag[:commaIdx]
		}

		// Find corresponding schema field
		schemaField := schema.GetFieldByName(jsonTag)
		if schemaField == nil {
			continue
		}

		// Parse value based on field type
		if err := h.parseFieldFromForm(formData, schemaField, field); err != nil {
			return fmt.Errorf("error parsing field %s: %w", schemaField.DisplayName, err)
		}
	}

	return nil
}

// parseFieldFromForm parses a single field value from form data
func (h *MaintenanceHandlers) parseFieldFromForm(formData map[string][]string, schemaField *config.Field, fieldValue reflect.Value) error {
	if !fieldValue.CanSet() {
		return nil
	}

	switch schemaField.Type {
	case config.FieldTypeBool:
		// Checkbox fields - present means true, absent means false
		_, exists := formData[schemaField.JSONName]
		fieldValue.SetBool(exists)

	case config.FieldTypeInt:
		if values, ok := formData[schemaField.JSONName]; ok && len(values) > 0 {
			if intVal, err := strconv.Atoi(values[0]); err != nil {
				return fmt.Errorf("invalid integer value: %s", values[0])
			} else {
				fieldValue.SetInt(int64(intVal))
			}
		}

	case config.FieldTypeFloat:
		if values, ok := formData[schemaField.JSONName]; ok && len(values) > 0 {
			if floatVal, err := strconv.ParseFloat(values[0], 64); err != nil {
				return fmt.Errorf("invalid float value: %s", values[0])
			} else {
				fieldValue.SetFloat(floatVal)
			}
		}

	case config.FieldTypeString:
		if values, ok := formData[schemaField.JSONName]; ok && len(values) > 0 {
			fieldValue.SetString(values[0])
		}

	case config.FieldTypeInterval:
		// Parse interval fields with value + unit
		valueKey := schemaField.JSONName + "_value"
		unitKey := schemaField.JSONName + "_unit"

		if valueStrs, ok := formData[valueKey]; ok && len(valueStrs) > 0 {
			value, err := strconv.Atoi(valueStrs[0])
			if err != nil {
				return fmt.Errorf("invalid interval value: %s", valueStrs[0])
			}

			unit := "minutes" // default
			if unitStrs, ok := formData[unitKey]; ok && len(unitStrs) > 0 {
				unit = unitStrs[0]
			}

			// Convert to seconds
			seconds := config.IntervalValueUnitToSeconds(value, unit)
			fieldValue.SetInt(int64(seconds))
		}

	default:
		return fmt.Errorf("unsupported field type: %s", schemaField.Type)
	}

	return nil
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

// RetryTask manually retries a maintenance task
func (h *MaintenanceHandlers) RetryTask(c *gin.Context) {
	taskID := c.Param("id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Task ID is required"})
		return
	}

	manager := h.adminServer.GetMaintenanceManager()
	if manager == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Maintenance manager not available"})
		return
	}

	err := manager.RetryTask(taskID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": fmt.Sprintf("Task %s has been queued for retry", taskID),
	})
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
	// Call the maintenance manager directly to get recent tasks (limit for performance)
	if h.adminServer == nil {
		return []*maintenance.MaintenanceTask{}, nil
	}

	manager := h.adminServer.GetMaintenanceManager()
	if manager == nil {
		return []*maintenance.MaintenanceTask{}, nil
	}

	// Get recent tasks only (last 100) to prevent slow page loads
	// Users can view more tasks via pagination if needed
	allTasks := manager.GetTasks("", "", 100)
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

// saveTaskConfigToProtobuf saves task configuration to protobuf file
func (h *MaintenanceHandlers) saveTaskConfigToProtobuf(taskType types.TaskType, config types.TaskConfig) error {
	configPersistence := h.adminServer.GetConfigPersistence()
	if configPersistence == nil {
		return fmt.Errorf("config persistence not available")
	}

	// Use the new ToTaskPolicy method - much simpler and more maintainable!
	taskPolicy := config.ToTaskPolicy()

	// Save using generic method - no more hardcoded task types!
	return configPersistence.SaveTaskPolicyGeneric(string(taskType), taskPolicy)
}
