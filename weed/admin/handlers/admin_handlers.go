package handlers

import (
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// AdminHandlers contains all the HTTP handlers for the admin interface
type AdminHandlers struct {
	adminServer            *dash.AdminServer
	sessionStore           sessions.Store
	authHandlers           *AuthHandlers
	clusterHandlers        *ClusterHandlers
	fileBrowserHandlers    *FileBrowserHandlers
	userHandlers           *UserHandlers
	policyHandlers         *PolicyHandlers
	pluginHandlers         *PluginHandlers
	mqHandlers             *MessageQueueHandlers
	serviceAccountHandlers *ServiceAccountHandlers
}

// NewAdminHandlers creates a new instance of AdminHandlers
func NewAdminHandlers(adminServer *dash.AdminServer, store sessions.Store) *AdminHandlers {
	authHandlers := NewAuthHandlers(adminServer, store)
	clusterHandlers := NewClusterHandlers(adminServer)
	fileBrowserHandlers := NewFileBrowserHandlers(adminServer)
	userHandlers := NewUserHandlers(adminServer)
	policyHandlers := NewPolicyHandlers(adminServer)
	pluginHandlers := NewPluginHandlers(adminServer)
	mqHandlers := NewMessageQueueHandlers(adminServer)
	serviceAccountHandlers := NewServiceAccountHandlers(adminServer)
	return &AdminHandlers{
		adminServer:            adminServer,
		sessionStore:           store,
		authHandlers:           authHandlers,
		clusterHandlers:        clusterHandlers,
		fileBrowserHandlers:    fileBrowserHandlers,
		userHandlers:           userHandlers,
		policyHandlers:         policyHandlers,
		pluginHandlers:         pluginHandlers,
		mqHandlers:             mqHandlers,
		serviceAccountHandlers: serviceAccountHandlers,
	}
}

// SetupRoutes configures all the routes for the admin interface
func (h *AdminHandlers) SetupRoutes(r *mux.Router, authRequired bool, adminUser, adminPassword, readOnlyUser, readOnlyPassword string, enableUI bool) {
	// Health check (no auth required)
	r.HandleFunc("/health", h.HealthCheck).Methods(http.MethodGet)

	// Prometheus metrics endpoint (no auth required)
	r.Handle("/metrics", promhttp.HandlerFor(stats.Gather, promhttp.HandlerOpts{})).Methods(http.MethodGet)

	// Favicon route (no auth required) - redirect to static version
	r.HandleFunc("/favicon.ico", func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "/static/favicon.ico", http.StatusMovedPermanently)
	}).Methods(http.MethodGet)

	// Skip UI routes if UI is not enabled
	if !enableUI {
		return
	}

	if authRequired {
		// Authentication routes (no auth required)
		r.HandleFunc("/login", h.authHandlers.ShowLogin).Methods(http.MethodGet)
		r.Handle("/login", h.authHandlers.HandleLogin(adminUser, adminPassword, readOnlyUser, readOnlyPassword)).Methods(http.MethodPost)
		r.HandleFunc("/logout", h.authHandlers.HandleLogout).Methods(http.MethodGet)

		protected := r.NewRoute().Subrouter()
		protected.Use(dash.RequireAuth(h.sessionStore))
		h.registerUIRoutes(protected)

		api := r.PathPrefix("/api").Subrouter()
		api.Use(dash.RequireAuthAPI(h.sessionStore))
		h.registerAPIRoutes(api, true)
		return
	}

	// No authentication required - all routes are public
	h.registerUIRoutes(r)
	api := r.PathPrefix("/api").Subrouter()
	h.registerAPIRoutes(api, false)
}

func (h *AdminHandlers) registerUIRoutes(r *mux.Router) {
	// Main admin interface routes
	r.HandleFunc("/", h.ShowDashboard).Methods(http.MethodGet)
	r.HandleFunc("/admin", h.ShowDashboard).Methods(http.MethodGet)

	// Object Store management routes
	r.HandleFunc("/object-store/buckets", h.ShowS3Buckets).Methods(http.MethodGet)
	r.HandleFunc("/object-store/buckets/{bucket}", h.ShowBucketDetails).Methods(http.MethodGet)
	r.HandleFunc("/object-store/users", h.userHandlers.ShowObjectStoreUsers).Methods(http.MethodGet)
	r.HandleFunc("/object-store/policies", h.policyHandlers.ShowPolicies).Methods(http.MethodGet)
	r.HandleFunc("/object-store/service-accounts", h.serviceAccountHandlers.ShowServiceAccounts).Methods(http.MethodGet)
	r.HandleFunc("/object-store/s3tables/buckets", h.ShowS3TablesBuckets).Methods(http.MethodGet)
	r.HandleFunc("/object-store/s3tables/buckets/{bucket}/namespaces", h.ShowS3TablesNamespaces).Methods(http.MethodGet)
	r.HandleFunc("/object-store/s3tables/buckets/{bucket}/namespaces/{namespace}/tables", h.ShowS3TablesTables).Methods(http.MethodGet)
	r.HandleFunc("/object-store/s3tables/buckets/{bucket}/namespaces/{namespace}/tables/{table}", h.ShowS3TablesTableDetails).Methods(http.MethodGet)
	r.HandleFunc("/object-store/iceberg", h.ShowIcebergCatalog).Methods(http.MethodGet)
	r.HandleFunc("/object-store/iceberg/{catalog}/namespaces", h.ShowIcebergNamespaces).Methods(http.MethodGet)
	r.HandleFunc("/object-store/iceberg/{catalog}/namespaces/{namespace}/tables", h.ShowIcebergTables).Methods(http.MethodGet)
	r.HandleFunc("/object-store/iceberg/{catalog}/namespaces/{namespace}/tables/{table}", h.ShowIcebergTableDetails).Methods(http.MethodGet)

	// File browser routes
	r.HandleFunc("/files", h.fileBrowserHandlers.ShowFileBrowser).Methods(http.MethodGet)

	// Cluster management routes
	r.HandleFunc("/cluster/masters", h.clusterHandlers.ShowClusterMasters).Methods(http.MethodGet)
	r.HandleFunc("/cluster/filers", h.clusterHandlers.ShowClusterFilers).Methods(http.MethodGet)
	r.HandleFunc("/cluster/volume-servers", h.clusterHandlers.ShowClusterVolumeServers).Methods(http.MethodGet)

	// Storage management routes
	r.HandleFunc("/storage/volumes", h.clusterHandlers.ShowClusterVolumes).Methods(http.MethodGet)
	r.HandleFunc("/storage/volumes/{id}/{server}", h.clusterHandlers.ShowVolumeDetails).Methods(http.MethodGet)
	r.HandleFunc("/storage/collections", h.clusterHandlers.ShowClusterCollections).Methods(http.MethodGet)
	r.HandleFunc("/storage/collections/{name}", h.clusterHandlers.ShowCollectionDetails).Methods(http.MethodGet)
	r.HandleFunc("/storage/ec-shards", h.clusterHandlers.ShowClusterEcShards).Methods(http.MethodGet)
	r.HandleFunc("/storage/ec-volumes/{id}", h.clusterHandlers.ShowEcVolumeDetails).Methods(http.MethodGet)

	// Message Queue management routes
	r.HandleFunc("/mq/brokers", h.mqHandlers.ShowBrokers).Methods(http.MethodGet)
	r.HandleFunc("/mq/topics", h.mqHandlers.ShowTopics).Methods(http.MethodGet)
	r.HandleFunc("/mq/topics/{namespace}/{topic}", h.mqHandlers.ShowTopicDetails).Methods(http.MethodGet)

	// Plugin pages
	r.HandleFunc("/plugin", h.pluginHandlers.ShowPlugin).Methods(http.MethodGet)
	r.HandleFunc("/plugin/configuration", h.pluginHandlers.ShowPluginConfiguration).Methods(http.MethodGet)
	r.HandleFunc("/plugin/queue", h.pluginHandlers.ShowPluginQueue).Methods(http.MethodGet)
	r.HandleFunc("/plugin/detection", h.pluginHandlers.ShowPluginDetection).Methods(http.MethodGet)
	r.HandleFunc("/plugin/execution", h.pluginHandlers.ShowPluginExecution).Methods(http.MethodGet)
	r.HandleFunc("/plugin/monitoring", h.pluginHandlers.ShowPluginMonitoring).Methods(http.MethodGet)
}

func (h *AdminHandlers) registerAPIRoutes(api *mux.Router, enforceWrite bool) {
	wrapWrite := func(handler http.HandlerFunc) http.Handler {
		if !enforceWrite {
			return handler
		}
		return dash.RequireWriteAccess()(handler)
	}

	api.HandleFunc("/cluster/topology", h.clusterHandlers.GetClusterTopology).Methods(http.MethodGet)
	api.HandleFunc("/cluster/masters", h.clusterHandlers.GetMasters).Methods(http.MethodGet)
	api.HandleFunc("/cluster/volumes", h.clusterHandlers.GetVolumeServers).Methods(http.MethodGet)
	api.HandleFunc("/admin", h.adminServer.ShowAdmin).Methods(http.MethodGet)
	api.HandleFunc("/config", h.adminServer.GetConfigInfo).Methods(http.MethodGet)

	s3Api := api.PathPrefix("/s3").Subrouter()
	s3Api.HandleFunc("/buckets", h.adminServer.ListBucketsAPI).Methods(http.MethodGet)
	s3Api.Handle("/buckets", wrapWrite(h.adminServer.CreateBucket)).Methods(http.MethodPost)
	s3Api.Handle("/buckets/{bucket}", wrapWrite(h.adminServer.DeleteBucket)).Methods(http.MethodDelete)
	s3Api.HandleFunc("/buckets/{bucket}", h.adminServer.ShowBucketDetails).Methods(http.MethodGet)
	s3Api.Handle("/buckets/{bucket}/quota", wrapWrite(h.adminServer.UpdateBucketQuota)).Methods(http.MethodPut)
	s3Api.Handle("/buckets/{bucket}/owner", wrapWrite(h.adminServer.UpdateBucketOwner)).Methods(http.MethodPut)

	usersApi := api.PathPrefix("/users").Subrouter()
	usersApi.HandleFunc("", h.userHandlers.GetUsers).Methods(http.MethodGet)
	usersApi.Handle("", wrapWrite(h.userHandlers.CreateUser)).Methods(http.MethodPost)
	usersApi.HandleFunc("/{username}", h.userHandlers.GetUserDetails).Methods(http.MethodGet)
	usersApi.Handle("/{username}", wrapWrite(h.userHandlers.UpdateUser)).Methods(http.MethodPut)
	usersApi.Handle("/{username}", wrapWrite(h.userHandlers.DeleteUser)).Methods(http.MethodDelete)
	usersApi.Handle("/{username}/access-keys", wrapWrite(h.userHandlers.CreateAccessKey)).Methods(http.MethodPost)
	usersApi.Handle("/{username}/access-keys/{accessKeyId}", wrapWrite(h.userHandlers.DeleteAccessKey)).Methods(http.MethodDelete)
	usersApi.Handle("/{username}/access-keys/{accessKeyId}/status", wrapWrite(h.userHandlers.UpdateAccessKeyStatus)).Methods(http.MethodPut)
	usersApi.HandleFunc("/{username}/policies", h.userHandlers.GetUserPolicies).Methods(http.MethodGet)
	usersApi.Handle("/{username}/policies", wrapWrite(h.userHandlers.UpdateUserPolicies)).Methods(http.MethodPut)

	saApi := api.PathPrefix("/service-accounts").Subrouter()
	saApi.HandleFunc("", h.serviceAccountHandlers.GetServiceAccounts).Methods(http.MethodGet)
	saApi.Handle("", wrapWrite(h.serviceAccountHandlers.CreateServiceAccount)).Methods(http.MethodPost)
	saApi.HandleFunc("/{id}", h.serviceAccountHandlers.GetServiceAccountDetails).Methods(http.MethodGet)
	saApi.Handle("/{id}", wrapWrite(h.serviceAccountHandlers.UpdateServiceAccount)).Methods(http.MethodPut)
	saApi.Handle("/{id}", wrapWrite(h.serviceAccountHandlers.DeleteServiceAccount)).Methods(http.MethodDelete)

	policyApi := api.PathPrefix("/object-store/policies").Subrouter()
	policyApi.HandleFunc("", h.policyHandlers.GetPolicies).Methods(http.MethodGet)
	policyApi.Handle("", wrapWrite(h.policyHandlers.CreatePolicy)).Methods(http.MethodPost)
	policyApi.HandleFunc("/{name}", h.policyHandlers.GetPolicy).Methods(http.MethodGet)
	policyApi.Handle("/{name}", wrapWrite(h.policyHandlers.UpdatePolicy)).Methods(http.MethodPut)
	policyApi.Handle("/{name}", wrapWrite(h.policyHandlers.DeletePolicy)).Methods(http.MethodDelete)
	policyApi.HandleFunc("/validate", h.policyHandlers.ValidatePolicy).Methods(http.MethodPost)

	s3TablesApi := api.PathPrefix("/s3tables").Subrouter()
	s3TablesApi.HandleFunc("/buckets", h.adminServer.ListS3TablesBucketsAPI).Methods(http.MethodGet)
	s3TablesApi.Handle("/buckets", wrapWrite(h.adminServer.CreateS3TablesBucket)).Methods(http.MethodPost)
	s3TablesApi.Handle("/buckets", wrapWrite(h.adminServer.DeleteS3TablesBucket)).Methods(http.MethodDelete)
	s3TablesApi.HandleFunc("/namespaces", h.adminServer.ListS3TablesNamespacesAPI).Methods(http.MethodGet)
	s3TablesApi.Handle("/namespaces", wrapWrite(h.adminServer.CreateS3TablesNamespace)).Methods(http.MethodPost)
	s3TablesApi.Handle("/namespaces", wrapWrite(h.adminServer.DeleteS3TablesNamespace)).Methods(http.MethodDelete)
	s3TablesApi.HandleFunc("/tables", h.adminServer.ListS3TablesTablesAPI).Methods(http.MethodGet)
	s3TablesApi.Handle("/tables", wrapWrite(h.adminServer.CreateS3TablesTable)).Methods(http.MethodPost)
	s3TablesApi.Handle("/tables", wrapWrite(h.adminServer.DeleteS3TablesTable)).Methods(http.MethodDelete)
	s3TablesApi.Handle("/bucket-policy", wrapWrite(h.adminServer.PutS3TablesBucketPolicy)).Methods(http.MethodPut)
	s3TablesApi.HandleFunc("/bucket-policy", h.adminServer.GetS3TablesBucketPolicy).Methods(http.MethodGet)
	s3TablesApi.Handle("/bucket-policy", wrapWrite(h.adminServer.DeleteS3TablesBucketPolicy)).Methods(http.MethodDelete)
	s3TablesApi.Handle("/table-policy", wrapWrite(h.adminServer.PutS3TablesTablePolicy)).Methods(http.MethodPut)
	s3TablesApi.HandleFunc("/table-policy", h.adminServer.GetS3TablesTablePolicy).Methods(http.MethodGet)
	s3TablesApi.Handle("/table-policy", wrapWrite(h.adminServer.DeleteS3TablesTablePolicy)).Methods(http.MethodDelete)
	s3TablesApi.Handle("/tags", wrapWrite(h.adminServer.TagS3TablesResource)).Methods(http.MethodPut)
	s3TablesApi.HandleFunc("/tags", h.adminServer.ListS3TablesTags).Methods(http.MethodGet)
	s3TablesApi.Handle("/tags", wrapWrite(h.adminServer.UntagS3TablesResource)).Methods(http.MethodDelete)

	filesApi := api.PathPrefix("/files").Subrouter()
	filesApi.Handle("/delete", wrapWrite(h.fileBrowserHandlers.DeleteFile)).Methods(http.MethodDelete)
	filesApi.Handle("/delete-multiple", wrapWrite(h.fileBrowserHandlers.DeleteMultipleFiles)).Methods(http.MethodDelete)
	filesApi.Handle("/create-folder", wrapWrite(h.fileBrowserHandlers.CreateFolder)).Methods(http.MethodPost)
	filesApi.Handle("/upload", wrapWrite(h.fileBrowserHandlers.UploadFile)).Methods(http.MethodPost)
	filesApi.HandleFunc("/download", h.fileBrowserHandlers.DownloadFile).Methods(http.MethodGet)
	filesApi.HandleFunc("/view", h.fileBrowserHandlers.ViewFile).Methods(http.MethodGet)
	filesApi.HandleFunc("/properties", h.fileBrowserHandlers.GetFileProperties).Methods(http.MethodGet)

	volumeApi := api.PathPrefix("/volumes").Subrouter()
	volumeApi.Handle("/{id}/{server}/vacuum", wrapWrite(h.clusterHandlers.VacuumVolume)).Methods(http.MethodPost)

	pluginApi := api.PathPrefix("/plugin").Subrouter()
	pluginApi.HandleFunc("/status", h.adminServer.GetPluginStatusAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/workers", h.adminServer.GetPluginWorkersAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/job-types", h.adminServer.GetPluginJobTypesAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/jobs", h.adminServer.GetPluginJobsAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/jobs/{jobId}", h.adminServer.GetPluginJobAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/jobs/{jobId}/detail", h.adminServer.GetPluginJobDetailAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/activities", h.adminServer.GetPluginActivitiesAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/scheduler-states", h.adminServer.GetPluginSchedulerStatesAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/job-types/{jobType}/descriptor", h.adminServer.GetPluginJobTypeDescriptorAPI).Methods(http.MethodGet)
	pluginApi.HandleFunc("/job-types/{jobType}/schema", h.adminServer.RequestPluginJobTypeSchemaAPI).Methods(http.MethodPost)
	pluginApi.HandleFunc("/job-types/{jobType}/config", h.adminServer.GetPluginJobTypeConfigAPI).Methods(http.MethodGet)
	pluginApi.Handle("/job-types/{jobType}/config", wrapWrite(h.adminServer.UpdatePluginJobTypeConfigAPI)).Methods(http.MethodPut)
	pluginApi.HandleFunc("/job-types/{jobType}/runs", h.adminServer.GetPluginRunHistoryAPI).Methods(http.MethodGet)
	pluginApi.Handle("/job-types/{jobType}/detect", wrapWrite(h.adminServer.TriggerPluginDetectionAPI)).Methods(http.MethodPost)
	pluginApi.Handle("/job-types/{jobType}/run", wrapWrite(h.adminServer.RunPluginJobTypeAPI)).Methods(http.MethodPost)
	pluginApi.Handle("/jobs/execute", wrapWrite(h.adminServer.ExecutePluginJobAPI)).Methods(http.MethodPost)

	mqApi := api.PathPrefix("/mq").Subrouter()
	mqApi.HandleFunc("/topics/{namespace}/{topic}", h.mqHandlers.GetTopicDetailsAPI).Methods(http.MethodGet)
	mqApi.Handle("/topics/create", wrapWrite(h.mqHandlers.CreateTopicAPI)).Methods(http.MethodPost)
	mqApi.Handle("/topics/retention/update", wrapWrite(h.mqHandlers.UpdateTopicRetentionAPI)).Methods(http.MethodPost)
	mqApi.Handle("/retention/purge", wrapWrite(h.adminServer.TriggerTopicRetentionPurgeAPI)).Methods(http.MethodPost)
}

// HealthCheck returns the health status of the admin interface
func (h *AdminHandlers) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"health": "ok"})
}

// ShowDashboard renders the main admin dashboard
func (h *AdminHandlers) ShowDashboard(w http.ResponseWriter, r *http.Request) {
	// Get admin data from the server
	adminData := h.getAdminData(r)

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	adminComponent := app.Admin(adminData)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, adminComponent)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowS3Buckets renders the Object Store buckets management page
func (h *AdminHandlers) ShowS3Buckets(w http.ResponseWriter, r *http.Request) {
	// Get Object Store buckets data from the server
	s3Data := h.getS3BucketsData(r)

	// Render HTML template
	w.Header().Set("Content-Type", "text/html")
	s3Component := app.S3Buckets(s3Data)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, s3Component)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}
}

// ShowS3TablesBuckets renders the S3 Tables buckets page
func (h *AdminHandlers) ShowS3TablesBuckets(w http.ResponseWriter, r *http.Request) {
	username := h.getUsername(r)

	data, err := h.adminServer.GetS3TablesBucketsData(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get S3 Tables buckets: "+err.Error())
		return
	}
	data.Username = username

	w.Header().Set("Content-Type", "text/html")
	component := app.S3TablesBuckets(data)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, component)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
	}
}

// ShowS3TablesNamespaces renders namespaces for a table bucket
func (h *AdminHandlers) ShowS3TablesNamespaces(w http.ResponseWriter, r *http.Request) {
	username := h.getUsername(r)

	bucketName := mux.Vars(r)["bucket"]
	arn, err := buildS3TablesBucketArn(bucketName)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	data, err := h.adminServer.GetS3TablesNamespacesData(r.Context(), arn)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get S3 Tables namespaces: "+err.Error())
		return
	}
	data.Username = username

	w.Header().Set("Content-Type", "text/html")
	component := app.S3TablesNamespaces(data)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, component)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
	}
}

// ShowS3TablesTables renders tables for a namespace
func (h *AdminHandlers) ShowS3TablesTables(w http.ResponseWriter, r *http.Request) {
	username := h.getUsername(r)

	bucketName := mux.Vars(r)["bucket"]
	namespace := mux.Vars(r)["namespace"]
	arn, err := buildS3TablesBucketArn(bucketName)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	data, err := h.adminServer.GetS3TablesTablesData(r.Context(), arn, namespace)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get S3 Tables tables: "+err.Error())
		return
	}
	data.Username = username

	w.Header().Set("Content-Type", "text/html")
	component := app.S3TablesTables(data)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, component)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
	}
}

// ShowS3TablesTableDetails renders Iceberg table metadata and snapshot details on the merged S3 Tables path.
func (h *AdminHandlers) ShowS3TablesTableDetails(w http.ResponseWriter, r *http.Request) {
	bucketName := mux.Vars(r)["bucket"]
	namespace := mux.Vars(r)["namespace"]
	tableName := mux.Vars(r)["table"]
	arn, err := buildS3TablesBucketArn(bucketName)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	data, err := h.adminServer.GetIcebergTableDetailsData(r.Context(), bucketName, arn, namespace, tableName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get table details: "+err.Error())
		return
	}
	data.Username = h.getUsername(r)

	w.Header().Set("Content-Type", "text/html")
	component := app.IcebergTableDetails(data)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, component)
	if err := layoutComponent.Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
	}
}

func buildS3TablesBucketArn(bucketName string) (string, error) {
	return s3tables.BuildBucketARN(s3tables.DefaultRegion, s3_constants.AccountAdminId, bucketName)
}

// getUsername returns the username from context, defaulting to "admin" if not set
func (h *AdminHandlers) getUsername(r *http.Request) string {
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	return username
}

// ShowIcebergCatalog redirects legacy Iceberg catalog URL to the merged S3 Tables buckets page.
func (h *AdminHandlers) ShowIcebergCatalog(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/object-store/s3tables/buckets", http.StatusMovedPermanently)
}

// ShowIcebergNamespaces redirects legacy Iceberg namespaces URL to the merged S3 Tables namespaces page.
func (h *AdminHandlers) ShowIcebergNamespaces(w http.ResponseWriter, r *http.Request) {
	catalogName := mux.Vars(r)["catalog"]
	http.Redirect(w, r, "/object-store/s3tables/buckets/"+url.PathEscape(catalogName)+"/namespaces", http.StatusMovedPermanently)
}

// ShowIcebergTables redirects legacy Iceberg tables URL to the merged S3 Tables tables page.
func (h *AdminHandlers) ShowIcebergTables(w http.ResponseWriter, r *http.Request) {
	catalogName := mux.Vars(r)["catalog"]
	namespace := mux.Vars(r)["namespace"]
	http.Redirect(w, r, "/object-store/s3tables/buckets/"+url.PathEscape(catalogName)+"/namespaces/"+url.PathEscape(namespace)+"/tables", http.StatusMovedPermanently)
}

// ShowIcebergTableDetails redirects legacy Iceberg table details URL to the merged S3 Tables details page.
func (h *AdminHandlers) ShowIcebergTableDetails(w http.ResponseWriter, r *http.Request) {
	catalogName := mux.Vars(r)["catalog"]
	namespace := mux.Vars(r)["namespace"]
	tableName := mux.Vars(r)["table"]
	http.Redirect(w, r, "/object-store/s3tables/buckets/"+url.PathEscape(catalogName)+"/namespaces/"+url.PathEscape(namespace)+"/tables/"+url.PathEscape(tableName), http.StatusMovedPermanently)
}

// ShowBucketDetails returns detailed information about a specific bucket
func (h *AdminHandlers) ShowBucketDetails(w http.ResponseWriter, r *http.Request) {
	bucketName := mux.Vars(r)["bucket"]
	details, err := h.adminServer.GetBucketDetails(bucketName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to get bucket details: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, details)
}

// getS3BucketsData retrieves Object Store buckets data from the server
func (h *AdminHandlers) getS3BucketsData(r *http.Request) dash.S3BucketsData {
	username := dash.UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}

	// Get Object Store buckets data
	data, err := h.adminServer.GetS3BucketsData()
	if err != nil {
		// Return empty data on error
		return dash.S3BucketsData{
			Username:     username,
			Buckets:      []dash.S3Bucket{},
			TotalBuckets: 0,
			TotalSize:    0,
			LastUpdated:  time.Now(),
		}
	}

	data.Username = username
	return data
}

// getAdminData retrieves admin data from the server (now uses consolidated method)
func (h *AdminHandlers) getAdminData(r *http.Request) dash.AdminData {
	username := dash.UsernameFromContext(r.Context())

	// Use the consolidated GetAdminData method from AdminServer
	adminData, err := h.adminServer.GetAdminData(username)
	if err != nil {
		// Return default data when services are not available
		if username == "" {
			username = "admin"
		}

		masterNodes := []dash.MasterNode{
			{
				Address:  "localhost:9333",
				IsLeader: true,
			},
		}

		return dash.AdminData{
			Username:      username,
			TotalVolumes:  0,
			TotalFiles:    0,
			TotalSize:     0,
			MasterNodes:   masterNodes,
			VolumeServers: []dash.VolumeServer{},
			FilerNodes:    []dash.FilerNode{},
			DataCenters:   []dash.DataCenter{},
			LastUpdated:   time.Now(),
		}
	}

	return adminData
}

// Helper functions
