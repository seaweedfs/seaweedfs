package handlers

import (
	"math"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// ClusterHandlers contains all the HTTP handlers for cluster management
type ClusterHandlers struct {
	adminServer *dash.AdminServer
}

// NewClusterHandlers creates a new instance of ClusterHandlers
func NewClusterHandlers(adminServer *dash.AdminServer) *ClusterHandlers {
	return &ClusterHandlers{
		adminServer: adminServer,
	}
}

// ShowClusterVolumeServers renders the cluster volume servers page
func (h *ClusterHandlers) ShowClusterVolumeServers(w http.ResponseWriter, r *http.Request) {
	// Get cluster volume servers data
	volumeServersData, err := h.adminServer.GetClusterVolumeServers()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get cluster volume servers: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	volumeServersData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	volumeServersComponent := app.ClusterVolumeServers(*volumeServersData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, volumeServersComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowClusterVolumes renders the cluster volumes page
func (h *ClusterHandlers) ShowClusterVolumes(w http.ResponseWriter, r *http.Request) {
	// Get pagination and sorting parameters from query string
	page := 1
	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	pageSize := 100
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if parsed, err := strconv.Atoi(ps); err == nil && parsed > 0 && parsed <= 1000 {
			pageSize = parsed
		}
	}

	sortBy := r.URL.Query().Get("sortBy")
	if sortBy == "" {
		sortBy = "id"
	}
	sortOrder := r.URL.Query().Get("sortOrder")
	if sortOrder == "" {
		sortOrder = "asc"
	}
	collection := r.URL.Query().Get("collection") // Optional collection filter

	// Get cluster volumes data
	volumesData, err := h.adminServer.GetClusterVolumes(page, pageSize, sortBy, sortOrder, collection)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get cluster volumes: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	volumesData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	volumesComponent := app.ClusterVolumes(*volumesData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, volumesComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowVolumeDetails renders the volume details page
func (h *ClusterHandlers) ShowVolumeDetails(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	volumeIDStr := vars["id"]
	server := vars["server"]

	if volumeIDStr == "" {
		writeJSONError(w, http.StatusBadRequest, "Volume ID is required")
		return
	}

	if server == "" {
		writeJSONError(w, http.StatusBadRequest, "Server is required")
		return
	}

	volumeID, err := strconv.Atoi(volumeIDStr)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid volume ID")
		return
	}

	// Get volume details
	volumeDetails, err := h.adminServer.GetVolumeDetails(volumeID, server)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get volume details: "+err.Error())
		return
	}

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	volumeDetailsComponent := app.VolumeDetails(*volumeDetails)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, volumeDetailsComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowClusterCollections renders the cluster collections page
func (h *ClusterHandlers) ShowClusterCollections(w http.ResponseWriter, r *http.Request) {
	// Get cluster collections data
	collectionsData, err := h.adminServer.GetClusterCollections()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get cluster collections: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	collectionsData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	collectionsComponent := app.ClusterCollections(*collectionsData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, collectionsComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowCollectionDetails renders the collection detail page
func (h *ClusterHandlers) ShowCollectionDetails(w http.ResponseWriter, r *http.Request) {
	collectionName := mux.Vars(r)["name"]
	if collectionName == "" {
		writeJSONError(w, http.StatusBadRequest, "Collection name is required")
		return
	}

	// Parse query parameters
	query := r.URL.Query()
	page, _ := strconv.Atoi(defaultQuery(query.Get("page"), "1"))
	pageSize, _ := strconv.Atoi(defaultQuery(query.Get("page_size"), "25"))
	sortBy := defaultQuery(query.Get("sort_by"), "volume_id")
	sortOrder := defaultQuery(query.Get("sort_order"), "asc")

	// Get collection details data (volumes and EC volumes)
	collectionDetailsData, err := h.adminServer.GetCollectionDetails(collectionName, page, pageSize, sortBy, sortOrder)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get collection details: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	collectionDetailsData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	collectionDetailsComponent := app.CollectionDetails(*collectionDetailsData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, collectionDetailsComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowClusterEcShards handles the cluster EC shards page (individual shards view)
func (h *ClusterHandlers) ShowClusterEcShards(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	query := r.URL.Query()
	page, _ := strconv.Atoi(defaultQuery(query.Get("page"), "1"))
	pageSize, _ := strconv.Atoi(defaultQuery(query.Get("page_size"), "100"))
	sortBy := defaultQuery(query.Get("sort_by"), "volume_id")
	sortOrder := defaultQuery(query.Get("sort_order"), "asc")
	collection := defaultQuery(query.Get("collection"), "")

	// Get data from admin server
	data, err := h.adminServer.GetClusterEcVolumes(page, pageSize, sortBy, sortOrder, collection)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	data.Username = username

	// Render template
	w.Header().Set("Content-Type", "text/html")
	ecVolumesComponent := app.ClusterEcVolumes(*data)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, ecVolumesComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
}

// ShowEcVolumeDetails renders the EC volume details page
func (h *ClusterHandlers) ShowEcVolumeDetails(w http.ResponseWriter, r *http.Request) {
	volumeIDStr := mux.Vars(r)["id"]

	if volumeIDStr == "" {
		writeJSONError(w, http.StatusBadRequest, "Volume ID is required")
		return
	}

	volumeID, err := strconv.Atoi(volumeIDStr)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid volume ID")
		return
	}

	// Check that volumeID is within uint32 range
	if volumeID < 0 || uint64(volumeID) > math.MaxUint32 {
		writeJSONError(w, http.StatusBadRequest, "Volume ID out of range")
		return
	}

	// Parse sorting parameters
	query := r.URL.Query()
	sortBy := defaultQuery(query.Get("sort_by"), "shard_id")
	sortOrder := defaultQuery(query.Get("sort_order"), "asc")

	// Get EC volume details
	ecVolumeDetails, err := h.adminServer.GetEcVolumeDetails(uint32(volumeID), sortBy, sortOrder)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get EC volume details: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	ecVolumeDetails.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	ecVolumeDetailsComponent := app.EcVolumeDetails(*ecVolumeDetails)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, ecVolumeDetailsComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowClusterMasters renders the cluster masters page
func (h *ClusterHandlers) ShowClusterMasters(w http.ResponseWriter, r *http.Request) {
	// Get cluster masters data
	mastersData, err := h.adminServer.GetClusterMasters()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get cluster masters: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	mastersData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	mastersComponent := app.ClusterMasters(*mastersData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, mastersComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowClusterFilers renders the cluster filers page
func (h *ClusterHandlers) ShowClusterFilers(w http.ResponseWriter, r *http.Request) {
	// Get cluster filers data
	filersData, err := h.adminServer.GetClusterFilers()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get cluster filers: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	filersData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	filersComponent := app.ClusterFilers(*filersData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, filersComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowClusterBrokers renders the cluster message brokers page
func (h *ClusterHandlers) ShowClusterBrokers(w http.ResponseWriter, r *http.Request) {
	// Get cluster brokers data
	brokersData, err := h.adminServer.GetClusterBrokers()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get cluster brokers: "+err.Error())
		return
	}

	// Set username
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	brokersData.Username = username

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	brokersComponent := app.ClusterBrokers(*brokersData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, brokersComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// GetClusterTopology returns the cluster topology as JSON
func (h *ClusterHandlers) GetClusterTopology(w http.ResponseWriter, r *http.Request) {
	topology, err := h.adminServer.GetClusterTopology()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, topology)
}

// GetMasters returns master node information
func (h *ClusterHandlers) GetMasters(w http.ResponseWriter, r *http.Request) {
	// Simple master info
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"masters": []map[string]string{{"address": "localhost:9333"}},
	})
}

// GetVolumeServers returns volume server information
func (h *ClusterHandlers) GetVolumeServers(w http.ResponseWriter, r *http.Request) {
	topology, err := h.adminServer.GetClusterTopology()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"volume_servers": topology.VolumeServers})
}

// VacuumVolume handles volume vacuum requests via API
func (h *ClusterHandlers) VacuumVolume(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	volumeIDStr := vars["id"]
	server := vars["server"]

	if volumeIDStr == "" {
		writeJSONError(w, http.StatusBadRequest, "Volume ID is required")
		return
	}

	volumeID, err := strconv.Atoi(volumeIDStr)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "Invalid volume ID")
		return
	}

	// Perform vacuum operation
	err = h.adminServer.VacuumVolume(volumeID, server)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "Failed to vacuum volume: " + err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":   "Volume vacuum started successfully",
		"volume_id": volumeID,
		"server":    server,
	})
}
