// Code generated by templ - DO NOT EDIT.

// templ: version: v0.3.906
package app

//lint:file-ignore SA4006 This context is only used if a nested component is present.

import "github.com/a-h/templ"
import templruntime "github.com/a-h/templ/runtime"

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
)

func MaintenanceConfig(data *maintenance.MaintenanceConfigData) templ.Component {
	return templruntime.GeneratedTemplate(func(templ_7745c5c3_Input templruntime.GeneratedComponentInput) (templ_7745c5c3_Err error) {
		templ_7745c5c3_W, ctx := templ_7745c5c3_Input.Writer, templ_7745c5c3_Input.Context
		if templ_7745c5c3_CtxErr := ctx.Err(); templ_7745c5c3_CtxErr != nil {
			return templ_7745c5c3_CtxErr
		}
		templ_7745c5c3_Buffer, templ_7745c5c3_IsBuffer := templruntime.GetBuffer(templ_7745c5c3_W)
		if !templ_7745c5c3_IsBuffer {
			defer func() {
				templ_7745c5c3_BufErr := templruntime.ReleaseBuffer(templ_7745c5c3_Buffer)
				if templ_7745c5c3_Err == nil {
					templ_7745c5c3_Err = templ_7745c5c3_BufErr
				}
			}()
		}
		ctx = templ.InitializeContext(ctx)
		templ_7745c5c3_Var1 := templ.GetChildren(ctx)
		if templ_7745c5c3_Var1 == nil {
			templ_7745c5c3_Var1 = templ.NopComponent
		}
		ctx = templ.ClearChildren(ctx)
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 1, "<div class=\"container-fluid\"><div class=\"row mb-4\"><div class=\"col-12\"><div class=\"d-flex justify-content-between align-items-center\"><h2 class=\"mb-0\"><i class=\"fas fa-cog me-2\"></i> Maintenance Configuration</h2><div class=\"btn-group\"><a href=\"/maintenance\" class=\"btn btn-outline-secondary\"><i class=\"fas fa-arrow-left me-1\"></i> Back to Queue</a></div></div></div></div><div class=\"row\"><div class=\"col-12\"><div class=\"card\"><div class=\"card-header\"><h5 class=\"mb-0\">System Settings</h5></div><div class=\"card-body\"><form><div class=\"mb-3\"><div class=\"form-check form-switch\"><input class=\"form-check-input\" type=\"checkbox\" id=\"enabled\"")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		if data.IsEnabled {
			templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 2, " checked")
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 3, "> <label class=\"form-check-label\" for=\"enabled\"><strong>Enable Maintenance System</strong></label></div><small class=\"form-text text-muted\">When enabled, the system will automatically scan for and execute maintenance tasks.</small></div><div class=\"mb-3\"><label for=\"scanInterval\" class=\"form-label\">Scan Interval (minutes)</label> <input type=\"number\" class=\"form-control\" id=\"scanInterval\" value=\"")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var2 string
		templ_7745c5c3_Var2, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%.0f", float64(data.Config.ScanIntervalSeconds)/60))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 50, Col: 110}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var2))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 4, "\" placeholder=\"30 (default)\" min=\"1\" max=\"1440\"> <small class=\"form-text text-muted\">How often to scan for maintenance tasks (1-1440 minutes). <strong>Default: 30 minutes</strong></small></div><div class=\"mb-3\"><label for=\"workerTimeout\" class=\"form-label\">Worker Timeout (minutes)</label> <input type=\"number\" class=\"form-control\" id=\"workerTimeout\" value=\"")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var3 string
		templ_7745c5c3_Var3, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%.0f", float64(data.Config.WorkerTimeoutSeconds)/60))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 60, Col: 111}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var3))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 5, "\" placeholder=\"5 (default)\" min=\"1\" max=\"60\"> <small class=\"form-text text-muted\">How long to wait for worker heartbeat before considering it inactive (1-60 minutes). <strong>Default: 5 minutes</strong></small></div><div class=\"mb-3\"><label for=\"taskTimeout\" class=\"form-label\">Task Timeout (hours)</label> <input type=\"number\" class=\"form-control\" id=\"taskTimeout\" value=\"")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var4 string
		templ_7745c5c3_Var4, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%.0f", float64(data.Config.TaskTimeoutSeconds)/3600))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 70, Col: 111}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var4))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 6, "\" placeholder=\"2 (default)\" min=\"1\" max=\"24\"> <small class=\"form-text text-muted\">Maximum time allowed for a single task to complete (1-24 hours). <strong>Default: 2 hours</strong></small></div><div class=\"mb-3\"><label for=\"globalMaxConcurrent\" class=\"form-label\">Global Concurrent Limit</label> <input type=\"number\" class=\"form-control\" id=\"globalMaxConcurrent\" value=\"")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var5 string
		templ_7745c5c3_Var5, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%d", data.Config.Policy.GlobalMaxConcurrent))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 80, Col: 103}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var5))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 7, "\" placeholder=\"4 (default)\" min=\"1\" max=\"20\"> <small class=\"form-text text-muted\">Maximum number of maintenance tasks that can run simultaneously across all workers (1-20). <strong>Default: 4</strong></small></div><div class=\"mb-3\"><label for=\"maxRetries\" class=\"form-label\">Default Max Retries</label> <input type=\"number\" class=\"form-control\" id=\"maxRetries\" value=\"")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var6 string
		templ_7745c5c3_Var6, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%d", data.Config.MaxRetries))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 90, Col: 87}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var6))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 8, "\" placeholder=\"3 (default)\" min=\"0\" max=\"10\"> <small class=\"form-text text-muted\">Default number of times to retry failed tasks (0-10). <strong>Default: 3</strong></small></div><div class=\"mb-3\"><label for=\"retryDelay\" class=\"form-label\">Retry Delay (minutes)</label> <input type=\"number\" class=\"form-control\" id=\"retryDelay\" value=\"")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var7 string
		templ_7745c5c3_Var7, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%.0f", float64(data.Config.RetryDelaySeconds)/60))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 100, Col: 108}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var7))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 9, "\" placeholder=\"15 (default)\" min=\"1\" max=\"120\"> <small class=\"form-text text-muted\">Time to wait before retrying failed tasks (1-120 minutes). <strong>Default: 15 minutes</strong></small></div><div class=\"mb-3\"><label for=\"taskRetention\" class=\"form-label\">Task Retention (days)</label> <input type=\"number\" class=\"form-control\" id=\"taskRetention\" value=\"")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var8 string
		templ_7745c5c3_Var8, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%.0f", float64(data.Config.TaskRetentionSeconds)/(24*3600)))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 110, Col: 118}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var8))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 10, "\" placeholder=\"7 (default)\" min=\"1\" max=\"30\"> <small class=\"form-text text-muted\">How long to keep completed/failed task records (1-30 days). <strong>Default: 7 days</strong></small></div><div class=\"d-flex gap-2\"><button type=\"button\" class=\"btn btn-primary\" onclick=\"saveConfiguration()\"><i class=\"fas fa-save me-1\"></i> Save Configuration</button> <button type=\"button\" class=\"btn btn-secondary\" onclick=\"resetToDefaults()\"><i class=\"fas fa-undo me-1\"></i> Reset to Defaults</button></div></form></div></div></div></div><!-- Individual Task Configuration Menu --><div class=\"row mt-4\"><div class=\"col-12\"><div class=\"card\"><div class=\"card-header\"><h5 class=\"mb-0\"><i class=\"fas fa-cogs me-2\"></i> Task Configuration</h5></div><div class=\"card-body\"><p class=\"text-muted mb-3\">Configure specific settings for each maintenance task type.</p><div class=\"list-group\">")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		for _, menuItem := range data.MenuItems {
			templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 11, "<a href=\"")
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			var templ_7745c5c3_Var9 templ.SafeURL
			templ_7745c5c3_Var9, templ_7745c5c3_Err = templ.JoinURLErrs(templ.SafeURL(menuItem.Path))
			if templ_7745c5c3_Err != nil {
				return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 147, Col: 69}
			}
			_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var9))
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 12, "\" class=\"list-group-item list-group-item-action\"><div class=\"d-flex w-100 justify-content-between\"><h6 class=\"mb-1\">")
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			var templ_7745c5c3_Var10 = []any{menuItem.Icon + " me-2"}
			templ_7745c5c3_Err = templ.RenderCSSItems(ctx, templ_7745c5c3_Buffer, templ_7745c5c3_Var10...)
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 13, "<i class=\"")
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			var templ_7745c5c3_Var11 string
			templ_7745c5c3_Var11, templ_7745c5c3_Err = templ.JoinStringErrs(templ.CSSClasses(templ_7745c5c3_Var10).String())
			if templ_7745c5c3_Err != nil {
				return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 1, Col: 0}
			}
			_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var11))
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 14, "\"></i> ")
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			var templ_7745c5c3_Var12 string
			templ_7745c5c3_Var12, templ_7745c5c3_Err = templ.JoinStringErrs(menuItem.DisplayName)
			if templ_7745c5c3_Err != nil {
				return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 151, Col: 65}
			}
			_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var12))
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 15, "</h6>")
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			if menuItem.IsEnabled {
				templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 16, "<span class=\"badge bg-success\">Enabled</span>")
				if templ_7745c5c3_Err != nil {
					return templ_7745c5c3_Err
				}
			} else {
				templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 17, "<span class=\"badge bg-secondary\">Disabled</span>")
				if templ_7745c5c3_Err != nil {
					return templ_7745c5c3_Err
				}
			}
			templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 18, "</div><p class=\"mb-1 small text-muted\">")
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			var templ_7745c5c3_Var13 string
			templ_7745c5c3_Var13, templ_7745c5c3_Err = templ.JoinStringErrs(menuItem.Description)
			if templ_7745c5c3_Err != nil {
				return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 159, Col: 90}
			}
			_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var13))
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
			templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 19, "</p></a>")
			if templ_7745c5c3_Err != nil {
				return templ_7745c5c3_Err
			}
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 20, "</div></div></div></div></div><!-- Statistics Overview --><div class=\"row mt-4\"><div class=\"col-12\"><div class=\"card\"><div class=\"card-header\"><h5 class=\"mb-0\">System Statistics</h5></div><div class=\"card-body\"><div class=\"row\"><div class=\"col-md-3\"><div class=\"text-center\"><h6 class=\"text-muted\">Last Scan</h6><p class=\"mb-0\">")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var14 string
		templ_7745c5c3_Var14, templ_7745c5c3_Err = templ.JoinStringErrs(data.LastScanTime.Format("2006-01-02 15:04:05"))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 180, Col: 100}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var14))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 21, "</p></div></div><div class=\"col-md-3\"><div class=\"text-center\"><h6 class=\"text-muted\">Next Scan</h6><p class=\"mb-0\">")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var15 string
		templ_7745c5c3_Var15, templ_7745c5c3_Err = templ.JoinStringErrs(data.NextScanTime.Format("2006-01-02 15:04:05"))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 186, Col: 100}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var15))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 22, "</p></div></div><div class=\"col-md-3\"><div class=\"text-center\"><h6 class=\"text-muted\">Total Tasks</h6><p class=\"mb-0\">")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var16 string
		templ_7745c5c3_Var16, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%d", data.SystemStats.TotalTasks))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 192, Col: 99}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var16))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 23, "</p></div></div><div class=\"col-md-3\"><div class=\"text-center\"><h6 class=\"text-muted\">Active Workers</h6><p class=\"mb-0\">")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		var templ_7745c5c3_Var17 string
		templ_7745c5c3_Var17, templ_7745c5c3_Err = templ.JoinStringErrs(fmt.Sprintf("%d", data.SystemStats.ActiveWorkers))
		if templ_7745c5c3_Err != nil {
			return templ.Error{Err: templ_7745c5c3_Err, FileName: `view/app/maintenance_config.templ`, Line: 198, Col: 102}
		}
		_, templ_7745c5c3_Err = templ_7745c5c3_Buffer.WriteString(templ.EscapeString(templ_7745c5c3_Var17))
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		templ_7745c5c3_Err = templruntime.WriteString(templ_7745c5c3_Buffer, 24, "</p></div></div></div></div></div></div></div></div><script>\n        function saveConfiguration() {\n            // First, get current configuration to preserve existing values\n            fetch('/api/maintenance/config')\n                .then(response => response.json())\n                .then(currentConfig => {\n                    // Update only the fields from the form\n                    const updatedConfig = {\n                        ...currentConfig.config, // Preserve existing config\n                        enabled: document.getElementById('enabled').checked,\n                        scan_interval_seconds: parseInt(document.getElementById('scanInterval').value) * 60, // Convert to seconds\n                        worker_timeout_seconds: parseInt(document.getElementById('workerTimeout').value) * 60, // Convert to seconds\n                        task_timeout_seconds: parseInt(document.getElementById('taskTimeout').value) * 3600, // Convert to seconds\n                        retry_delay_seconds: parseInt(document.getElementById('retryDelay').value) * 60, // Convert to seconds\n                        max_retries: parseInt(document.getElementById('maxRetries').value),\n                        task_retention_seconds: parseInt(document.getElementById('taskRetention').value) * 24 * 3600, // Convert to seconds\n                        policy: {\n                            ...currentConfig.config.policy, // Preserve existing policy\n                            global_max_concurrent: parseInt(document.getElementById('globalMaxConcurrent').value)\n                        }\n                    };\n\n                    // Send the updated configuration\n                    return fetch('/api/maintenance/config', {\n                        method: 'PUT',\n                        headers: {\n                            'Content-Type': 'application/json',\n                        },\n                        body: JSON.stringify(updatedConfig)\n                    });\n                })\n                .then(response => response.json())\n                .then(data => {\n                    if (data.success) {\n                        alert('Configuration saved successfully');\n                        location.reload(); // Reload to show updated values\n                    } else {\n                        alert('Failed to save configuration: ' + (data.error || 'Unknown error'));\n                    }\n                })\n                .catch(error => {\n                    alert('Error: ' + error.message);\n                });\n        }\n\n        function resetToDefaults() {\n            if (confirm('Are you sure you want to reset to default configuration? This will overwrite your current settings.')) {\n                // Reset form to defaults (matching DefaultMaintenanceConfig values)\n                document.getElementById('enabled').checked = false;\n                document.getElementById('scanInterval').value = '30';\n                document.getElementById('workerTimeout').value = '5';\n                document.getElementById('taskTimeout').value = '2';\n                document.getElementById('globalMaxConcurrent').value = '4';\n                document.getElementById('maxRetries').value = '3';\n                document.getElementById('retryDelay').value = '15';\n                document.getElementById('taskRetention').value = '7';\n            }\n        }\n    </script>")
		if templ_7745c5c3_Err != nil {
			return templ_7745c5c3_Err
		}
		return nil
	})
}

var _ = templruntime.GeneratedTemplate
