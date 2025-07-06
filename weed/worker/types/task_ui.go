package types

import (
	"fmt"
	"html/template"
	"time"
)

// TaskUIProvider defines how tasks provide their configuration UI
type TaskUIProvider interface {
	// GetTaskType returns the task type
	GetTaskType() TaskType

	// GetDisplayName returns the human-readable name
	GetDisplayName() string

	// GetDescription returns a description of what this task does
	GetDescription() string

	// GetIcon returns the icon CSS class or HTML for this task type
	GetIcon() string

	// RenderConfigForm renders the configuration form HTML
	RenderConfigForm(currentConfig interface{}) (template.HTML, error)

	// ParseConfigForm parses form data into configuration
	ParseConfigForm(formData map[string][]string) (interface{}, error)

	// GetCurrentConfig returns the current configuration
	GetCurrentConfig() interface{}

	// ApplyConfig applies the new configuration
	ApplyConfig(config interface{}) error
}

// TaskStats represents runtime statistics for a task type
type TaskStats struct {
	TaskType       TaskType      `json:"task_type"`
	DisplayName    string        `json:"display_name"`
	Enabled        bool          `json:"enabled"`
	LastScan       time.Time     `json:"last_scan"`
	NextScan       time.Time     `json:"next_scan"`
	PendingTasks   int           `json:"pending_tasks"`
	RunningTasks   int           `json:"running_tasks"`
	CompletedToday int           `json:"completed_today"`
	FailedToday    int           `json:"failed_today"`
	MaxConcurrent  int           `json:"max_concurrent"`
	ScanInterval   time.Duration `json:"scan_interval"`
}

// UIRegistry manages task UI providers
type UIRegistry struct {
	providers map[TaskType]TaskUIProvider
}

// NewUIRegistry creates a new UI registry
func NewUIRegistry() *UIRegistry {
	return &UIRegistry{
		providers: make(map[TaskType]TaskUIProvider),
	}
}

// RegisterUI registers a task UI provider
func (r *UIRegistry) RegisterUI(provider TaskUIProvider) {
	r.providers[provider.GetTaskType()] = provider
}

// GetProvider returns the UI provider for a task type
func (r *UIRegistry) GetProvider(taskType TaskType) TaskUIProvider {
	return r.providers[taskType]
}

// GetAllProviders returns all registered UI providers
func (r *UIRegistry) GetAllProviders() map[TaskType]TaskUIProvider {
	result := make(map[TaskType]TaskUIProvider)
	for k, v := range r.providers {
		result[k] = v
	}
	return result
}

// Common UI data structures for shared components
type TaskListData struct {
	Tasks       []*Task      `json:"tasks"`
	TaskStats   []*TaskStats `json:"task_stats"`
	LastUpdated time.Time    `json:"last_updated"`
}

type TaskDetailsData struct {
	Task        *Task         `json:"task"`
	TaskType    TaskType      `json:"task_type"`
	DisplayName string        `json:"display_name"`
	Description string        `json:"description"`
	Stats       *TaskStats    `json:"stats"`
	ConfigForm  template.HTML `json:"config_form"`
	LastUpdated time.Time     `json:"last_updated"`
}

// Common form field types for simple form building
type FormField struct {
	Name        string       `json:"name"`
	Label       string       `json:"label"`
	Type        string       `json:"type"` // text, number, checkbox, select, duration
	Value       interface{}  `json:"value"`
	Description string       `json:"description"`
	Required    bool         `json:"required"`
	Options     []FormOption `json:"options,omitempty"` // For select fields
}

type FormOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

// Helper for building forms in code
type FormBuilder struct {
	fields []FormField
}

// NewFormBuilder creates a new form builder
func NewFormBuilder() *FormBuilder {
	return &FormBuilder{
		fields: make([]FormField, 0),
	}
}

// AddTextField adds a text input field
func (fb *FormBuilder) AddTextField(name, label, description string, value string, required bool) *FormBuilder {
	fb.fields = append(fb.fields, FormField{
		Name:        name,
		Label:       label,
		Type:        "text",
		Value:       value,
		Description: description,
		Required:    required,
	})
	return fb
}

// AddNumberField adds a number input field
func (fb *FormBuilder) AddNumberField(name, label, description string, value float64, required bool) *FormBuilder {
	fb.fields = append(fb.fields, FormField{
		Name:        name,
		Label:       label,
		Type:        "number",
		Value:       value,
		Description: description,
		Required:    required,
	})
	return fb
}

// AddCheckboxField adds a checkbox field
func (fb *FormBuilder) AddCheckboxField(name, label, description string, value bool) *FormBuilder {
	fb.fields = append(fb.fields, FormField{
		Name:        name,
		Label:       label,
		Type:        "checkbox",
		Value:       value,
		Description: description,
		Required:    false,
	})
	return fb
}

// AddSelectField adds a select dropdown field
func (fb *FormBuilder) AddSelectField(name, label, description string, value string, options []FormOption, required bool) *FormBuilder {
	fb.fields = append(fb.fields, FormField{
		Name:        name,
		Label:       label,
		Type:        "select",
		Value:       value,
		Description: description,
		Required:    required,
		Options:     options,
	})
	return fb
}

// AddDurationField adds a duration input field
func (fb *FormBuilder) AddDurationField(name, label, description string, value time.Duration, required bool) *FormBuilder {
	fb.fields = append(fb.fields, FormField{
		Name:        name,
		Label:       label,
		Type:        "duration",
		Value:       value.String(),
		Description: description,
		Required:    required,
	})
	return fb
}

// Build generates the HTML form fields with Bootstrap styling
func (fb *FormBuilder) Build() template.HTML {
	html := ""

	for _, field := range fb.fields {
		html += fb.renderField(field)
	}

	return template.HTML(html)
}

// renderField renders a single form field with Bootstrap classes
func (fb *FormBuilder) renderField(field FormField) string {
	html := "<div class=\"mb-3\">\n"

	// Special handling for checkbox fields
	if field.Type == "checkbox" {
		checked := ""
		if field.Value.(bool) {
			checked = " checked"
		}
		html += "  <div class=\"form-check\">\n"
		html += "    <input type=\"checkbox\" class=\"form-check-input\" id=\"" + field.Name + "\" name=\"" + field.Name + "\"" + checked + ">\n"
		html += "    <label class=\"form-check-label\" for=\"" + field.Name + "\">" + field.Label + "</label>\n"
		html += "  </div>\n"
		// Description for checkbox
		if field.Description != "" {
			html += "  <div class=\"form-text text-muted\">" + field.Description + "</div>\n"
		}
		html += "</div>\n"
		return html
	}

	// Label for non-checkbox fields
	required := ""
	if field.Required {
		required = " <span class=\"text-danger\">*</span>"
	}
	html += "  <label for=\"" + field.Name + "\" class=\"form-label\">" + field.Label + required + "</label>\n"

	// Input based on type
	switch field.Type {
	case "text":
		html += "  <input type=\"text\" class=\"form-control\" id=\"" + field.Name + "\" name=\"" + field.Name + "\" value=\"" + field.Value.(string) + "\""
		if field.Required {
			html += " required"
		}
		html += ">\n"

	case "number":
		html += "  <input type=\"number\" class=\"form-control\" id=\"" + field.Name + "\" name=\"" + field.Name + "\" step=\"any\" value=\"" +
			fmt.Sprintf("%v", field.Value) + "\""
		if field.Required {
			html += " required"
		}
		html += ">\n"

	case "select":
		html += "  <select class=\"form-select\" id=\"" + field.Name + "\" name=\"" + field.Name + "\""
		if field.Required {
			html += " required"
		}
		html += ">\n"
		for _, option := range field.Options {
			selected := ""
			if option.Value == field.Value.(string) {
				selected = " selected"
			}
			html += "    <option value=\"" + option.Value + "\"" + selected + ">" + option.Label + "</option>\n"
		}
		html += "  </select>\n"

	case "duration":
		html += "  <input type=\"text\" class=\"form-control\" id=\"" + field.Name + "\" name=\"" + field.Name + "\" value=\"" + field.Value.(string) +
			"\" placeholder=\"e.g., 30m, 2h, 24h\""
		if field.Required {
			html += " required"
		}
		html += ">\n"
	}

	// Description for non-checkbox fields
	if field.Description != "" {
		html += "  <div class=\"form-text text-muted\">" + field.Description + "</div>\n"
	}

	html += "</div>\n"
	return html
}
