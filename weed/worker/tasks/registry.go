package tasks

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// RegisterFunc is a function that registers a task with a given registry
type RegisterFunc func(registry *TaskRegistry)

var (
	globalRegistrations []RegisterFunc
	registrationMutex   sync.Mutex
)

// AutoRegister adds a task registration function to be called later
func AutoRegister(registerFunc RegisterFunc) {
	registrationMutex.Lock()
	defer registrationMutex.Unlock()
	globalRegistrations = append(globalRegistrations, registerFunc)
}

// RegisterAll calls all registered task registration functions
func RegisterAll(registry *TaskRegistry) {
	registrationMutex.Lock()
	defer registrationMutex.Unlock()

	glog.V(1).Infof("Auto-registering %d task types", len(globalRegistrations))

	for _, registerFunc := range globalRegistrations {
		registerFunc(registry)
	}

	glog.V(1).Infof("Successfully registered %d task types", len(globalRegistrations))
}
