package layout

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"

	// Import task packages to trigger their auto-registration
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

// MenuItemData represents a menu item
type MenuItemData struct {
	Name        string
	URL         string
	Icon        string
	Description string
}

// GetConfigurationMenuItems returns the dynamic configuration menu items
func GetConfigurationMenuItems() []*MenuItemData {
	var menuItems []*MenuItemData

	// Add system configuration item
	menuItems = append(menuItems, &MenuItemData{
		Name:        "System",
		URL:         "/maintenance/config",
		Icon:        "fas fa-cogs",
		Description: "System-level configuration",
	})

	// Get all registered task types and add them as submenu items
	registeredTypes := maintenance.GetRegisteredMaintenanceTaskTypes()

	for _, taskType := range registeredTypes {
		menuItem := &MenuItemData{
			Name:        maintenance.GetTaskDisplayName(taskType),
			URL:         "/maintenance/config/" + string(taskType),
			Icon:        maintenance.GetTaskIcon(taskType),
			Description: maintenance.GetTaskDescription(taskType),
		}

		menuItems = append(menuItems, menuItem)
	}

	return menuItems
}
