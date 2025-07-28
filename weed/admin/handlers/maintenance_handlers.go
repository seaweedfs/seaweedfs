package handlers

import (
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

	// Get the task configuration schema
	schema := tasks.GetTaskConfigSchema(taskTypeName)
	if schema == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Schema not found for task type: " + taskTypeName})
		return
	}

	// Create a new config instance based on task type and apply schema defaults
	var config interface{}
	switch taskType {
	case types.TaskTypeVacuum:
		config = &vacuum.Config{}
	case types.TaskTypeBalance:
		config = &balance.Config{}
	case types.TaskTypeErasureCoding:
		config = &erasure_coding.Config{}
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported task type: " + taskTypeName})
		return
	}

	// Apply schema defaults first
	if err := schema.ApplyDefaults(config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to apply defaults: " + err.Error()})
		return
	}

	// First, get the current configuration to preserve existing values
	currentUIRegistry := tasks.GetGlobalUIRegistry()
	currentTypesRegistry := tasks.GetGlobalTypesRegistry()

	var currentProvider types.TaskUIProvider
	for workerTaskType := range currentTypesRegistry.GetAllDetectors() {
		if string(workerTaskType) == string(taskType) {
			currentProvider = currentUIRegistry.GetProvider(workerTaskType)
			break
		}
	}

	if currentProvider != nil {
		// Copy current config values to the new config
		currentConfig := currentProvider.GetCurrentConfig()
		if currentConfigMap, ok := currentConfig.(interface{ ToMap() map[string]interface{} }); ok {
			currentMap := currentConfigMap.ToMap()
			// Apply current values first
			if configInterface, ok := config.(interface {
				FromMap(map[string]interface{}) error
			}); ok {
				if err := configInterface.FromMap(currentMap); err != nil {
					glog.Warningf("Failed to load current config for %s: %v", taskTypeName, err)
				}
			}
		}
	}

	// Parse form data using schema-based approach (this will override with new values)
	err = h.parseTaskConfigFromForm(c.Request.PostForm, schema, config)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse configuration: " + err.Error()})
		return
	}

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
	err = provider.ApplyConfig(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to apply configuration: " + err.Error()})
		return
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
