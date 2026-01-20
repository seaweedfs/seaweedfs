// SeaweedFS Dashboard IAM Logic
// Requires helpers.js to be loaded first

// Constants
const RELOAD_DELAY_SUCCESS_MS = 1000;
const RELOAD_DELAY_PARTIAL_FAILURE_MS = 2000;

// State tracks original values for diffing changes
let originalGroupMembers = [];
let originalGroupPolicies = [];
let currentStatsGroupName = "";

// Validate IAM Policy JSON structure
function validateIAMPolicy(policyText) {
    if (!policyText || policyText.trim() === "") {
        return { valid: false, error: "Policy document cannot be empty" };
    }

    let policy;
    try {
        policy = JSON.parse(policyText);
    } catch (e) {
        return { valid: false, error: "Invalid JSON format: " + e.message };
    }

    if (typeof policy !== 'object' || policy === null) {
        return { valid: false, error: "Policy must be a JSON object" };
    }

    // 1. Check Version
    if (!policy.Version) {
        return { valid: false, error: "Missing required field: 'Version'" };
    }
    if (typeof policy.Version !== 'string') {
        return { valid: false, error: "'Version' must be a string (e.g., \"2012-10-17\")" };
    }

    // 2. Check Statement
    if (!policy.Statement) {
        return { valid: false, error: "Missing required field: 'Statement'" };
    }
    if (!Array.isArray(policy.Statement)) {
        return { valid: false, error: "'Statement' must be an array of statement objects" };
    }
    if (policy.Statement.length === 0) {
        return { valid: false, error: "'Statement' array cannot be empty" };
    }

    // 3. Validate each Statement
    for (let i = 0; i < policy.Statement.length; i++) {
        const stmt = policy.Statement[i];
        const prefix = `Statement[${i}]: `;

        if (typeof stmt !== 'object' || stmt === null) {
            return { valid: false, error: prefix + "Must be an object" };
        }

        // Effect
        if (!stmt.Effect) {
            return { valid: false, error: prefix + "Missing required field 'Effect'" };
        }
        if (stmt.Effect !== "Allow" && stmt.Effect !== "Deny") {
            return { valid: false, error: prefix + "'Effect' must be either \"Allow\" or \"Deny\"" };
        }

        // Action (Required unless NotAction is present, but simplifies to Action for now)
        if (!stmt.Action && !stmt.NotAction) {
            return { valid: false, error: prefix + "Missing 'Action' (or 'NotAction')" };
        }

        if (stmt.Action) {
            if (typeof stmt.Action !== 'string' && !Array.isArray(stmt.Action)) {
                return { valid: false, error: prefix + "'Action' must be a string or an array of strings" };
            }
        }

        // Resource or Principal
        if (stmt.Resource) {
            if (typeof stmt.Resource !== 'string' && !Array.isArray(stmt.Resource)) {
                return { valid: false, error: prefix + "'Resource' must be a string or an array of strings" };
            }
        }

        if (stmt.Principal) {
            if (typeof stmt.Principal !== 'object' && typeof stmt.Principal !== 'string') {
                return { valid: false, error: prefix + "'Principal' format validation failed" };
            }
        }
    }

    return { valid: true };
}

// Attach to global Dashboard object if available
if (window.Dashboard) {
    window.Dashboard.validateIAMPolicy = validateIAMPolicy;
}

// ============================================================================
// USER MANAGEMENT FUNCTIONS
// ============================================================================

// Global variables for user management
// Use var to avoid "Identifier already declared" errors when HTMX re-executes script
var currentEditingUser = null;
var currentPolicy = null;
var currentAccessKeysUser = '';
var createActions = [];
var editActions = [];

// User Management Functions

async function fetchBuckets() {
    try {
        const response = await fetch('/api/s3/buckets');
        if (response.ok) {
            const data = await response.json();
            // Bucket fetching logic
        }
    } catch (error) {
        console.error('Error fetching buckets:', error);
    }
}

function removePermission(mode, action) {
    if (mode === 'create') {
        createActions = createActions.filter(a => a !== action);
    } else {
        editActions = editActions.filter(a => a !== action);
    }
    renderActions(mode);
}

function renderActions(mode) {
    const listEl = document.getElementById(mode + 'ActionsList');
    if (!listEl) return;

    const actions = mode === 'create' ? createActions : editActions;

    if (actions.length === 0) {
        listEl.innerHTML = '<small class="text-muted d-block text-center mt-2">No permissions added</small>';
        return;
    }

    listEl.innerHTML = '';
    actions.forEach(action => {
        const badge = document.createElement('div');
        badge.className = 'd-inline-block bg-light border rounded px-2 py-1 m-1';
        badge.innerHTML = `
            <span class="me-2">${escapeHtml(action)}</span>
            <button type="button" class="btn btn-sm btn-link text-danger p-0 ms-1" style="text-decoration: none;"
                    aria-label="Remove" onclick="removePermission('${mode}', '${escapeHtml(action)}')">
                <i class="fas fa-times"></i>
            </button>
        `;
        listEl.appendChild(badge);
    });
}

// Initialize IAM listeners using event delegation
document.addEventListener('DOMContentLoaded', function () {
    if (window.iamListenersInitialized) return;
    window.iamListenersInitialized = true;

    // Event delegation for user action buttons
    document.addEventListener('click', function (e) {
        const button = e.target.closest('[data-action]');
        if (!button) return;

        e.preventDefault();

        const action = button.getAttribute('data-action');
        const username = button.getAttribute('data-username');

        switch (action) {
            case 'show-user-details':
                if (typeof showUserDetails === 'function') showUserDetails(username);
                break;
            case 'edit-user':
                if (typeof editUser === 'function') editUser(username);
                break;
            case 'manage-access-keys':
                if (typeof manageAccessKeys === 'function') manageAccessKeys(username);
                break;
            case 'delete-user':
                if (typeof deleteUser === 'function') deleteUser(username);
                break;
            case 'export-users':
                if (typeof exportUsers === 'function') exportUsers();
                break;
            // Policy Actions
            case 'view-policy':
                const policyNameView = button.getAttribute('data-policy-name');
                if (typeof viewPolicy === 'function') viewPolicy(policyNameView);
                break;
            case 'edit-policy':
                const policyNameEdit = button.getAttribute('data-policy-name');
                if (typeof editPolicy === 'function') editPolicy(policyNameEdit);
                break;
            case 'delete-policy':
                const policyNameDelete = button.getAttribute('data-policy-name');
                if (typeof deletePolicy === 'function') deletePolicy(policyNameDelete);
                break;
            // Role Actions
            case 'create-role':
                if (typeof openCreateRoleModal === 'function') openCreateRoleModal();
                break;
            case 'preview-role':
                if (typeof previewRole === 'function') previewRole(button);
                break;
            case 'edit-role':
                if (typeof editRole === 'function') editRole(button);
                break;
            case 'delete-role':
                const roleNameDelete = button.getAttribute('data-role-name');
                if (typeof deleteRole === 'function') deleteRole(roleNameDelete);
                break;
            case 'save-role':
                if (typeof handleSaveRole === 'function') handleSaveRole();
                break;
            // User Modal Actions (Submit/Create)
            case 'create-user-submit':
                if (typeof handleCreateUser === 'function') handleCreateUser();
                break;

            case 'create-access-key':
                if (typeof createAccessKey === 'function') createAccessKey();
                break;
            // Policy Modal Actions
            case 'create-policy-submit':
                if (typeof handleCreatePolicy === 'function') handleCreatePolicy();
                break;
            case 'update-policy-submit':
                if (typeof handleUpdatePolicy === 'function') handleUpdatePolicy();
                break;
            case 'edit-policy-from-view':
                if (currentPolicy) {
                    const viewModal = bootstrap.Modal.getInstance(document.getElementById('viewPolicyModal'));
                    if (viewModal) viewModal.hide();
                    // Use policyName field (with fallback to name) - matches displayPolicyDetails logic
                    const policyName = currentPolicy.policyName || currentPolicy.name;
                    if (typeof editPolicy === 'function') editPolicy(policyName);
                }
                break;
            case 'insert-sample-policy':
                if (typeof insertSamplePolicy === 'function') insertSamplePolicy();
                break;
            case 'validate-policy-document':
                if (typeof validatePolicyDocument === 'function') validatePolicyDocument();
                break;
            case 'insert-sample-policy-edit':
                if (typeof insertSamplePolicyEdit === 'function') insertSamplePolicyEdit();
                break;
            case 'validate-edit-policy-document':
                if (typeof validateEditPolicyDocument === 'function') validateEditPolicyDocument();
                break;

        }
    });

    // We also listen on document for dynamic elements (delegation)
    document.addEventListener('show.bs.modal', function (event) {
        const target = event.target;

        // Reset create form actions on modal close/open
        if (target && target.id === 'createUserModal') {
            createActions = []; // Reset actions
            renderActions('create');

            // Fetch and render roles when modal opens
            if (typeof fetchAndRenderRoles === 'function') {
                fetchAndRenderRoles('createRolesList', 'create');
            }
        }

        // Edit User Modal
        if (target && target.id === 'editUserModal') {
            // Logic for edit user modal if any
        }

        // Group Modal
        if (target && target.id === 'groupModal') {
            populateGroupCreationModal().catch(err => {
                console.error('Failed to populate group modal:', err);
                showErrorMessage('Failed to load group creation form');
            });
        }
    }, true); // Capture phase/Bubble? Bootstrap events bubble.

    // Initial load
    fetchBuckets();
    loadSavedPolicies();
});

var availableRoles = [];

// Fetch roles from backend
async function fetchRoles() {
    try {
        const response = await fetch('/api/object-store/roles');
        if (response.ok) {
            const data = await response.json();
            availableRoles = data.roles || [];
            return availableRoles;
        } else {
            console.error('Failed to fetch roles');
            return [];
        }
    } catch (error) {
        console.error('Error fetching roles:', error);
        return [];
    }
}

// Fetch and render roles dropdown
async function fetchAndRenderRoles(containerId, context, selectedRoles = []) {
    const container = document.getElementById(containerId);
    if (!container) return;

    // Show loading state
    container.innerHTML = '<div class="text-center text-muted"><i class="fas fa-spinner fa-spin me-2"></i>Loading roles...</div>';

    const roles = await fetchRoles();

    if (roles.length === 0) {
        container.innerHTML = '<div class="text-muted small">No roles found. Create roles in the Roles tab first.</div>';
        return;
    }

    let html = `<select class="form-select" id="${context}_role_select" name="roles">`;
    html += '<option value="">Select a role...</option>';

    roles.forEach(role => {
        const isSelected = selectedRoles.includes(role.roleName);
        const selectedAttr = isSelected ? 'selected' : '';
        html += `<option value="${role.roleName}" ${selectedAttr}>${role.roleName}</option>`;
    });
    html += '</select>';
    container.innerHTML = html;
}

async function loadSavedPolicies() {
    try {
        const response = await fetch('/api/object-store/policies');
        if (response.ok) {
            const data = await response.json();
            const policies = data.policies || []; // Adjust based on API response structure

            ['createSavedPolicy', 'editSavedPolicy'].forEach(id => {
                const select = document.getElementById(id);
                if (select) {
                    // Clear existing options except first
                    select.innerHTML = '<option value="">Select Saved Policy...</option>';

                    policies.forEach(p => {
                        const option = document.createElement('option');
                        const pName = p.policyName || p.name;
                        option.value = pName;
                        option.textContent = pName;
                        select.appendChild(option);
                    });
                }
            });
        }
    } catch (e) {
        console.error('Failed to load policies', e);
    }
}

function addSavedPolicy(mode) {
    const selectId = mode === 'create' ? 'createSavedPolicy' : 'editSavedPolicy';
    const select = document.getElementById(selectId);
    if (!select) return;

    const policyName = select.value;
    if (!policyName) return;

    const actionString = 'policy:' + policyName;
    const targetArray = mode === 'create' ? createActions : editActions;
    let changed = false;

    if (!targetArray.includes(actionString)) {
        targetArray.push(actionString);
        changed = true;
    }

    if (changed) {
        renderActions(mode);
    }

    select.value = ""; // Reset
}

async function handleCreateUser() {
    const form = document.getElementById('createUserForm');
    const formData = new FormData(form);

    const userName = formData.get('username');

    if (!userName) {
        showErrorMessage('Username is required');
        return;
    }

    const userData = {
        userName: userName
    };

    try {
        const response = await fetch('/api/iam/users', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(userData)
        });

        if (response.ok) {
            const result = await response.json();
            showSuccessMessage('User created successfully');

            const createModalEl = document.getElementById('createUserModal');
            const createModal = bootstrap.Modal.getInstance(createModalEl);
            createModal.hide();

            form.reset();

            if (result.access_key) {
                showNewAccessKeyModal(result.access_key);
            } else {
                setTimeout(() => window.location.reload(), 1000);
            }
        } else {
            const error = await response.json();
            showErrorMessage('Failed to create user: ' + (error.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error creating user:', error);
        showErrorMessage('Failed to create user: ' + error.message);
    }
}

async function editUser(username) {
    showErrorMessage("Edit User is not supported implementation detail.");
}



async function deleteUser(username) {
    if (!confirm(`Are you sure you want to delete user "${username}"? This action cannot be undone.`)) {
        return;
    }

    try {
        const response = await fetch(`/api/iam/users/${username}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            showSuccessMessage('User deleted successfully');
            setTimeout(() => window.location.reload(), 1000);
        } else {
            const error = await response.json();
            showErrorMessage('Failed to delete user: ' + (error.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error deleting user:', error);
        showErrorMessage('Failed to delete user: ' + error.message);
    }
}

async function showUserDetails(username) {
    try {
        const response = await fetch(`/api/users/${username}`);
        if (response.ok) {
            const data = await response.json();
            // API now returns {user: ..., accessKeys: ...}
            const content = createUserDetailsContent(data.user, data.accessKeys);
            document.getElementById('userDetailsContent').innerHTML = content;
            const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('userDetailsModal'));
            modal.show();
        } else {
            showErrorMessage('Failed to load user details');
        }
    } catch (error) {
        console.error('Error loading user details:', error);
        showErrorMessage('Failed to load user details');
    }
}

function createUserDetailsContent(user, accessKeys) {
    if (!user) return '<p class="text-danger">Invalid user data</p>';

    var detailsHtml = '<div class="row">';
    detailsHtml += '<div class="col-md-6">';
    detailsHtml += '<h6 class="text-muted">Basic Information</h6>';
    detailsHtml += '<table class="table table-sm">';
    detailsHtml += '<tr><td><strong>Username:</strong></td><td>' + escapeHtml(user.userName) + '</td></tr>';
    detailsHtml += '<tr><td><strong>User ID:</strong></td><td><code>' + escapeHtml(user.arn) + '</code></td></tr>';
    detailsHtml += '<tr><td><strong>Created:</strong></td><td>' + new Date(user.createDate).toLocaleString() + '</td></tr>';
    detailsHtml += '</table>';
    detailsHtml += '</div>';

    detailsHtml += '<div class="col-md-6">';
    detailsHtml += '<h6 class="text-muted">Access Keys</h6>';
    if (accessKeys && accessKeys.length > 0) {
        detailsHtml += '<div class="list-group list-group-flush small mb-3">';
        accessKeys.forEach(function (key) {
            detailsHtml += `<div class="list-group-item px-0 py-1">
               <code class="text-muted">${key.accessKeyId}</code>
               <span class="badge ${key.status === 'Active' ? 'bg-success' : 'bg-danger'} ms-2">${key.status}</span>
               <div class="text-muted small">Created: ${new Date(key.createDate).toLocaleDateString()}</div>
           </div>`;
        });
        detailsHtml += '</div>';
    } else {
        detailsHtml += '<p class="text-muted">No access keys</p>';
    }
    detailsHtml += '</div>'; // col
    detailsHtml += '</div>'; // row

    return detailsHtml;
}

function createAccessKeysTable(accessKeys) {
    return `
        <div class="table-responsive">
            <table class="table table-sm">
                <thead>
                    <tr>
                        <th>Access Key</th>
                        <th>Created</th>
                    </tr>
                </thead>
                <tbody>
                    ${accessKeys.map(key => `
                        <tr>
                            <td><code>${key.access_key}</code></td>
                            <td>${new Date(key.created_at).toLocaleDateString()}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
}

async function manageAccessKeys(username) {
    currentAccessKeysUser = username;
    document.getElementById('accessKeysUsername').textContent = username;

    await loadAccessKeys(username);

    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('accessKeysModal'));
    modal.show();
}

async function loadAccessKeys(username) {
    try {
        const response = await fetch(`/api/iam/users/${username}/keys`);
        if (response.ok) {
            const data = await response.json();
            const content = createAccessKeysManagementContent(data.access_keys || []);
            document.getElementById('accessKeysContent').innerHTML = content;
        } else {
            document.getElementById('accessKeysContent').innerHTML = '<p class="text-muted">Failed to load access keys</p>';
        }
    } catch (error) {
        console.error('Error loading access keys:', error);
        document.getElementById('accessKeysContent').innerHTML = '<p class="text-muted">Error loading access keys</p>';
    }
}

function createAccessKeysManagementContent(accessKeys) {
    if (accessKeys.length === 0) {
        return '<p class="text-muted">No access keys found. Create one to get started.</p>';
    }

    return `
<div class="table-responsive">
    <table class="table table-hover">
        <thead>
            <tr>
                <th>Access Key</th>
                <th>Status</th>
                <th>Created</th>
                <th>Actions</th>
            </tr>
        </thead>
        <tbody>
            ${accessKeys.map(key => `
                <tr>
                    <td>
                        <code>${escapeHtml(key.accessKeyId)}</code>
                        <button class="btn btn-sm btn-outline-secondary ms-2" onclick="copyToClipboard('${escapeHtml(key.accessKeyId)}', this)">
                            <i class="fas fa-copy"></i>
                        </button>
                    </td>
                    <td>
                        <span class="badge ${key.status === 'Active' ? 'bg-success' : 'bg-danger'}">${escapeHtml(key.status)}</span>
                    </td>
                    <td>${new Date(key.createDate).toLocaleDateString()}</td>
                    <td>
                        ${key.status === 'Active' ?
            `<button class="btn btn-sm btn-outline-warning me-1" title="Deactivate" onclick="updateAccessKeyStatus('${escapeHtml(key.accessKeyId)}', 'Inactive')">
                                <i class="fas fa-ban"></i>
                            </button>` :
            `<button class="btn btn-sm btn-outline-success me-1" title="Activate" onclick="updateAccessKeyStatus('${escapeHtml(key.accessKeyId)}', 'Active')">
                                <i class="fas fa-check-circle"></i>
                            </button>`
        }
                        <button class="btn btn-sm btn-outline-danger" title="Delete" onclick="confirmDeleteAccessKey('${escapeHtml(key.accessKeyId)}')">
                            <i class="fas fa-trash"></i>
                        </button>
                    </td>
                </tr>
            `).join('')}
        </tbody>
    </table>
</div>
`;
}

async function createAccessKey() {
    if (!currentAccessKeysUser) {
        showErrorMessage('No user selected');
        return;
    }

    try {
        const response = await fetch(`/api/iam/users/${currentAccessKeysUser}/keys`, {
            method: 'POST'
        });

        if (response.ok) {
            const result = await response.json();
            showSuccessMessage('Access key created successfully');

            // Show the new access key
            showNewAccessKeyModal(result.access_key);

            // Reload access keys
            await loadAccessKeys(currentAccessKeysUser);
        } else {
            const error = await response.json();
            showErrorMessage('Failed to create access key: ' + (error.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error creating access key:', error);
        showErrorMessage('Failed to create access key: ' + error.message);
    }
}

async function updateAccessKeyStatus(accessKeyId, status) {
    if (!currentAccessKeysUser) return;

    try {
        const response = await fetch(`/api/iam/users/${currentAccessKeysUser}/keys/${accessKeyId}/status`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ status: status })
        });

        if (response.ok) {
            showSuccessMessage(`Access key ${status === 'Active' ? 'activated' : 'deactivated'} successfully`);
            await loadAccessKeys(currentAccessKeysUser);
        } else {
            const error = await response.json();
            showErrorMessage('Failed to update access key: ' + (error.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error updating access key:', error);
        showErrorMessage('Failed to update access key: ' + error.message);
    }
}

function confirmDeleteAccessKey(accessKeyId) {
    if (!confirm(`Are you sure you want to delete access key "${accessKeyId}"? This action cannot be undone.`)) {
        return;
    }
    deleteAccessKeyConfirmed(accessKeyId);
}

async function deleteAccessKeyConfirmed(accessKeyId) {
    try {
        const response = await fetch(`/api/iam/users/${currentAccessKeysUser}/keys/${accessKeyId}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            showSuccessMessage('Access key deleted successfully');

            // Reload access keys
            await loadAccessKeys(currentAccessKeysUser);
        } else {
            const error = await response.json();
            showErrorMessage('Failed to delete access key: ' + (error.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error deleting access key:', error);
        showErrorMessage('Failed to delete access key: ' + error.message);
    }
}

function showSecretKey(accessKey, secretKey) {
    const content = `
        <div class="alert alert-info">
            <i class="fas fa-info-circle me-2"></i>
            <strong>Access Key Details:</strong> These credentials provide access to your object storage. Keep them secure and don't share them.
        </div>
        <div class="mb-3">
            <label class="form-label"><strong>Access Key:</strong></label>
            <div class="input-group">
                <input type="text" class="form-control" value="${accessKey}" readonly>
                <button class="btn btn-outline-secondary" onclick="copyToClipboard('${accessKey}', this)">
                    <i class="fas fa-copy"></i>
                </button>
            </div>
        </div>
        <div class="mb-3">
            <label class="form-label"><strong>Secret Key:</strong></label>
            <div class="input-group">
                <input type="text" class="form-control" value="${secretKey}" readonly>
                <button class="btn btn-outline-secondary" onclick="copyToClipboard('${secretKey}', this)">
                    <i class="fas fa-copy"></i>
                </button>
            </div>
        </div>
    `;

    showModal('Access Key Details', content);
}

function showNewAccessKeyModal(accessKeyData) {
    const content = `
        <div class="alert alert-success">
            <i class="fas fa-check-circle me-2"></i>
            <strong>Success!</strong> Your new access key has been created.
        </div>
        <div class="alert alert-warning">
            <i class="fas fa-exclamation-triangle me-2"></i>
            <strong>Important:</strong> This is the only time the secret key will be displayed. Please save it securely now.
        </div>
        <div class="mb-3">
            <label class="form-label"><strong>Access Key ID:</strong></label>
            <div class="input-group">
                <input type="text" class="form-control" value="${escapeHtml(accessKeyData.accessKeyId)}" readonly>
                <button class="btn btn-outline-secondary" onclick="copyToClipboard('${escapeHtml(accessKeyData.accessKeyId)}', this)">
                    <i class="fas fa-copy"></i>
                </button>
            </div>
        </div>
        <div class="mb-3">
            <label class="form-label"><strong>Secret Access Key:</strong></label>
            <div class="input-group">
                <input type="text" class="form-control" value="${escapeHtml(accessKeyData.secretKey)}" readonly>
                <button class="btn btn-outline-secondary" onclick="copyToClipboard('${escapeHtml(accessKeyData.secretKey)}', this)">
                    <i class="fas fa-copy"></i>
                </button>
            </div>
        </div>
    `;

    showModal('New Access Key Created', content, () => {
        // Reload page to show new user or key when modal is closed
        window.location.reload();
    });
}

function showModal(title, content, onClose = null) {
    let modalEl = document.getElementById('globalIamModal');
    let modal;

    if (!modalEl) {
        const modalHtml = `
        <div class="modal fade" id="globalIamModal" tabindex="-1" role="dialog" aria-hidden="true">
            <div class="modal-dialog" role="document">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="globalIamModalTitle"></h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body" id="globalIamModalBody"></div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>`;
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        modalEl = document.getElementById('globalIamModal');
        modal = new bootstrap.Modal(modalEl);

        // Setup listener for cleanup/callbacks only once
        modalEl.addEventListener('hidden.bs.modal', function () {
            if (modalEl._onCloseCallback && typeof modalEl._onCloseCallback === 'function') {
                modalEl._onCloseCallback();
                modalEl._onCloseCallback = null; // Clear callback
            }
        });
    } else {
        modal = bootstrap.Modal.getOrCreateInstance(modalEl);
    }

    // Update Content
    document.getElementById('globalIamModalTitle').innerText = title;
    document.getElementById('globalIamModalBody').innerHTML = content;

    // Store callback on the element to be called on hidden event
    modalEl._onCloseCallback = onClose;

    modal.show();
}

// ============================================================================
// POLICY MANAGEMENT FUNCTIONS (Moved from policies.templ)
// ============================================================================

// View Policy
function viewPolicy(policyName) {
    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('viewPolicyModal'));
    modal.show();

    // Reset content to loading state
    document.getElementById('viewPolicyContent').innerHTML = `
        <div class="text-center">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="mt-2">Loading policy...</p>
        </div>
    `;

    // Fetch policy data
    fetch('/api/object-store/policies/' + encodeURIComponent(policyName))
        .then(response => {
            if (!response.ok) {
                throw new Error('Policy not found');
            }
            return response.json();
        })
        .then(policy => {
            currentPolicy = policy;
            displayPolicyDetails(policy);
        })
        .catch(error => {
            console.error('Error:', error);
            document.getElementById('viewPolicyContent').innerHTML = `
                <div class="alert alert-danger" role="alert">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Error loading policy: ${escapeHtml(error.message)}
                </div>
            `;
        });
}

function displayPolicyDetails(policy) {
    const content = document.getElementById('viewPolicyContent');

    // Handle document parsing if it's a string
    let doc = policy.document;
    if (typeof doc === 'string') {
        try {
            doc = JSON.parse(doc);
        } catch (e) {
            console.error("Failed to parse policy document", e);
            doc = {};
        }
    }
    const policyName = policy.policyName || policy.name || 'Unknown';

    let statementsHtml = '';
    if (doc && doc.Statement) {
        statementsHtml = doc.Statement.map((stmt, index) => `
            <div class="card mb-2">
                <div class="card-header py-2">
                    <h6 class="mb-0">Statement ${index + 1}</h6>
                </div>
                <div class="card-body py-2">
                    <div class="row">
                        <div class="col-md-6">
                            <strong>Effect:</strong> 
                            <span class="badge ${stmt.Effect === 'Allow' ? 'bg-success' : 'bg-danger'}">${escapeHtml(stmt.Effect)}</span>
                        </div>
                        <div class="col-md-6">
                            <strong>Actions:</strong> ${Array.isArray(stmt.Action) ? stmt.Action.map(escapeHtml).join(', ') : escapeHtml(stmt.Action)}
                        </div>
                    </div>
                    <div class="row mt-2">
                        <div class="col-12">
                            <strong>Resources:</strong> ${Array.isArray(stmt.Resource) ? stmt.Resource.map(escapeHtml).join(', ') : escapeHtml(stmt.Resource)}
                        </div>
                    </div>
                </div>
            </div>
        `).join('');
    }

    content.innerHTML = `
        <div class="row mb-3">
            <div class="col-md-6">
                <strong>Policy Name:</strong> ${escapeHtml(policyName)}
            </div>
            <div class="col-md-6">
                <strong>Version:</strong> <span class="badge bg-info">${escapeHtml(doc?.Version || 'Unknown')}</span>
            </div>
        </div>
        
        <div class="mb-3">
            <strong>Statements:</strong>
            <div class="mt-2">
                ${statementsHtml || '<p class="text-muted">No statements found</p>'}
            </div>
        </div>
        
        <div class="mb-3">
            <strong>Raw Policy Document:</strong>
            <pre class="bg-light p-3 border rounded mt-2"><code>${escapeHtml(JSON.stringify(doc, null, 2))}</code></pre>
        </div>
    `;
}

// Edit Policy
function editPolicy(policyName) {
    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('editPolicyModal'));
    modal.show();

    // Set policy name
    document.getElementById('editPolicyName').value = policyName;
    document.getElementById('editPolicyDocument').value = 'Loading...';

    // Fetch policy data
    fetch('/api/object-store/policies/' + encodeURIComponent(policyName))
        .then(response => {
            if (!response.ok) {
                throw new Error('Policy not found');
            }
            return response.json();
        })
        .then(policy => {
            currentPolicy = policy;
            let doc = policy.document;
            if (typeof doc === 'string') {
                try { doc = JSON.parse(doc); } catch (e) { }
            }
            document.getElementById('editPolicyDocument').value = JSON.stringify(doc, null, 2);
        })
        .catch(error => {
            console.error('Error:', error);
            showErrorMessage('Error loading policy for editing: ' + error.message);
            modal.hide();
        });
}

// Update Policy
function handleUpdatePolicy() {
    const policyName = document.getElementById('editPolicyName').value;
    const policyDocumentText = document.getElementById('editPolicyDocument').value;

    if (!policyName || !policyDocumentText) {
        showErrorMessage('Please fill in all required fields');
        return;
    }

    let policyDocument;
    try {
        policyDocument = JSON.parse(policyDocumentText);
    } catch (e) {
        showErrorMessage('Invalid JSON in policy document: ' + e.message);
        return;
    }

    const requestData = {
        document: policyDocument
    };

    fetch('/api/object-store/policies/' + encodeURIComponent(policyName), {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestData)
    })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showSuccessMessage('Policy updated successfully!');
                const modalEl = document.getElementById('editPolicyModal');
                const modal = bootstrap.Modal.getInstance(modalEl);
                modal.hide();
                setTimeout(() => window.location.reload(), 1000);
            } else {
                showErrorMessage('Error updating policy: ' + (data.error || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showErrorMessage('Error updating policy: ' + error.message);
        });
}

// Create Policy
function handleCreatePolicy() {
    const form = document.getElementById('createPolicyForm');
    const formData = new FormData(form);

    const policyName = formData.get('name');
    const policyDocumentText = formData.get('document');

    if (!policyName || !policyDocumentText) {
        showErrorMessage('Please fill in all required fields');
        return;
    }

    let policyDocument;
    try {
        policyDocument = JSON.parse(policyDocumentText);
    } catch (e) {
        showErrorMessage('Invalid JSON in policy document: ' + e.message);
        return;
    }

    const requestData = {
        name: policyName,
        document: policyDocument
    };

    fetch('/api/object-store/policies', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestData)
    })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showSuccessMessage('Policy created successfully!');
                const modalEl = document.getElementById('createPolicyModal');
                const modal = bootstrap.Modal.getInstance(modalEl);
                modal.hide();
                setTimeout(() => window.location.reload(), 1000);
            } else {
                showErrorMessage('Error creating policy: ' + (data.error || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showErrorMessage('Error creating policy: ' + error.message);
        });
}

// Delete Policy
function deletePolicy(policyName) {
    confirmAction('Are you sure you want to delete policy "' + policyName + '"?', function () {
        fetch('/api/object-store/policies/' + encodeURIComponent(policyName), {
            method: 'DELETE'
        })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showSuccessMessage('Policy deleted successfully!');
                    setTimeout(() => window.location.reload(), 1000);
                } else {
                    showErrorMessage('Error deleting policy: ' + (data.error || 'Unknown error'));
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showErrorMessage('Error deleting policy: ' + error.message);
            });
    });
}

// Helpers for Policy Forms
function insertSamplePolicy() {
    const samplePolicy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::my-bucket/*"
                ]
            }
        ]
    };
    document.getElementById('policyDocument').value = JSON.stringify(samplePolicy, null, 2);
}

function insertSamplePolicyEdit() {
    const samplePolicy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::my-bucket/*"
                ]
            }
        ]
    };
    document.getElementById('editPolicyDocument').value = JSON.stringify(samplePolicy, null, 2);
}

function validatePolicyDocument() {
    const policyText = document.getElementById('policyDocument').value;
    validatePolicyJSON(policyText);
}

function validateEditPolicyDocument() {
    const policyText = document.getElementById('editPolicyDocument').value;
    validatePolicyJSON(policyText);
}

function validatePolicyJSON(policyText) {
    // Only use the global validator if available, prevents 'Dashboard is not defined' if called too early
    if (window.Dashboard && window.Dashboard.validateIAMPolicy) {
        const result = window.Dashboard.validateIAMPolicy(policyText);
        if (result.valid) {
            showSuccessMessage('Policy document is valid JSON and has correct IAM structure!');
            return true;
        } else {
            showErrorMessage('Validation Error: ' + result.error);
            return false;
        }
    } else if (typeof validateIAMPolicy === 'function') {
        const result = validateIAMPolicy(policyText);
        if (result.valid) {
            showSuccessMessage('Policy document is valid JSON!');
            return true;
        } else {
            showErrorMessage('Validation Error: ' + result.error);
            return false;
        }
    }
}

// Handle 'Edit from View' button


// ============================================================================
// ROLE MANAGEMENT FUNCTIONS (Moved from roles.templ)
// ============================================================================

var availablePolicies = [];

function toJsonArray(arr) {
    return JSON.stringify(arr || []);
}

function fetchAndRenderPolicies(attachedPolicies) {
    const listContainer = document.getElementById("policiesListContainer");
    const loadingIndicator = document.getElementById("policiesLoading");

    if (!listContainer) return;

    listContainer.innerHTML = "";
    if (loadingIndicator) loadingIndicator.style.display = "block";

    // If we already have policies cached, use them
    if (availablePolicies.length > 0) {
        renderPoliciesList(availablePolicies, attachedPolicies);
        if (loadingIndicator) loadingIndicator.style.display = "none";
        return;
    }

    fetch("/api/object-store/policies")
        .then(res => res.json())
        .then(data => {
            availablePolicies = data.policies || [];
            renderPoliciesList(availablePolicies, attachedPolicies);
        })
        .catch(err => {
            listContainer.innerHTML = `<div class="text-danger small">Failed to load policies: ${escapeHtml(err.message)}</div>`;
        })
        .finally(() => {
            if (loadingIndicator) loadingIndicator.style.display = "none";
        });
}

function renderPoliciesList(policies, attachedPolicies) {
    const listContainer = document.getElementById("policiesListContainer");
    if (!listContainer) return;

    listContainer.innerHTML = "";

    if (policies.length === 0) {
        listContainer.innerHTML = `<div class="text-muted small text-center">No policies found.</div>`;
        updatePoliciesDropdownLabel();
        return;
    }

    // Normalize attached policies to set for O(1) lookup
    const attachedSet = new Set((attachedPolicies || []).map(p => {
        // Handle if p is ARN (arn:aws:iam:::policy/PolicyName)
        if (p && p.startsWith("arn:")) {
            return p.split("/").pop();
        }
        return p;
    }));

    policies.forEach(policy => {
        const pName = policy.policyName || policy.name;
        const isChecked = attachedSet.has(pName) ? "checked" : "";
        const div = document.createElement("div");
        div.className = "form-check py-2 px-4 hover-bg-light position-relative";

        div.innerHTML = `
            <input class="form-check-input policy-checkbox" type="checkbox" value="${escapeHtml(pName)}" id="policy_${escapeHtml(pName)}" ${isChecked} onchange="updatePoliciesDropdownLabel()" onclick="event.stopPropagation()">
            <label class="form-check-label w-100 stretched-link cursor-pointer" for="policy_${escapeHtml(pName)}" onclick="event.stopPropagation()">
                ${escapeHtml(pName)}
            </label>
        `;
        listContainer.appendChild(div);
    });
    updatePoliciesDropdownLabel();
}

// Expose this globally so the inline onchange still works if needed, 
// though ideally we'd delegate this too. For now keeping it simple.
window.updatePoliciesDropdownLabel = function () {
    const checkedCount = document.querySelectorAll(".policy-checkbox:checked").length;
    const btnSpan = document.querySelector("#policiesDropdownBtn span");
    if (btnSpan) {
        if (checkedCount > 0) {
            btnSpan.innerText = `${checkedCount} policies selected`;
            btnSpan.classList.add("fw-bold", "text-primary");
        } else {
            btnSpan.innerText = "Select Attached Policies...";
            btnSpan.classList.remove("fw-bold", "text-primary");
        }
    }
};

function openCreateRoleModal() {
    const label = document.getElementById("roleModalLabel");
    if (label) label.innerText = "Create Role";

    const nameInput = document.getElementById("roleName");
    if (nameInput) {
        nameInput.value = "";
        nameInput.disabled = false;
    }

    const descInput = document.getElementById("roleDescription");
    if (descInput) descInput.value = "";

    const durationInput = document.getElementById("maxSessionDuration");
    if (durationInput) durationInput.value = "3600";

    const trustInput = document.getElementById("trustPolicy");
    if (trustInput) {
        trustInput.value = JSON.stringify({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "AWS": "arn:aws:iam::seaweedfs:user/<user>" },
                "Action": "sts:AssumeRole"
            }]
        }, null, 2);
    }

    // Fetch and render empty attached policies
    fetchAndRenderPolicies([]);

    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('roleModal'));
    modal.show();
}

function editRole(btnOrElement) {
    // Support passing button element directly
    const btn = btnOrElement.tagName ? btnOrElement : null;
    if (!btn) return;

    const roleName = btn.getAttribute("data-role-name");
    const description = btn.getAttribute("data-role-desc");
    const duration = btn.getAttribute("data-role-duration");
    const trustPolicyJson = btn.getAttribute("data-role-trust");
    const attachedPoliciesStr = btn.getAttribute("data-role-policies");

    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('roleModal'));

    document.getElementById("roleModalLabel").innerText = "Edit Role";
    const nameInput = document.getElementById("roleName");
    nameInput.value = roleName;
    nameInput.disabled = true;
    document.getElementById("roleDescription").value = description;
    const durationValue = (duration && parseInt(duration, 10) > 0) ? duration : "3600";
    document.getElementById("maxSessionDuration").value = durationValue;

    // Try to format JSON
    try {
        const obj = JSON.parse(trustPolicyJson);
        document.getElementById("trustPolicy").value = JSON.stringify(obj, null, 2);
    } catch (e) {
        document.getElementById("trustPolicy").value = trustPolicyJson;
    }

    let attachedPolicies = [];
    try {
        attachedPolicies = JSON.parse(attachedPoliciesStr);
    } catch (e) {
        console.error("Failed to parse attached policies", e);
    }

    fetchAndRenderPolicies(attachedPolicies);

    modal.show();
}

// Exposed for inline onclick="validateTrustPolicy(event)" in role modal
window.validateTrustPolicy = function (event) {
    if (event) event.preventDefault();
    const trustPolicyStr = document.getElementById("trustPolicy").value;

    const result = Dashboard.validateIAMPolicy(trustPolicyStr);
    if (result.valid) {
        alert("Trust Policy is valid JSON and has correct IAM structure!");
        return true;
    } else {
        alert("Validation Error: " + result.error);
        return false;
    }
};

function handleSaveRole() {
    const roleName = document.getElementById("roleName").value;
    if (!roleName) {
        alert("Role Name is required");
        return;
    }

    const maxSessionDurationStr = document.getElementById("maxSessionDuration").value;
    const maxSessionDuration = parseInt(maxSessionDurationStr, 10);
    if (isNaN(maxSessionDuration) || maxSessionDuration < 3600 || maxSessionDuration > 43200) {
        alert("Max Session Duration must be between 3600 and 43200 seconds");
        return;
    }

    const policies = [];
    document.querySelectorAll(".policy-checkbox:checked").forEach(input => {
        policies.push(input.value);
    });

    // Validate JSON
    const trustPolicyStr = document.getElementById("trustPolicy").value;
    // Use centralized validator
    const validationResult = Dashboard.validateIAMPolicy(trustPolicyStr);
    if (!validationResult.valid) {
        alert("Invalid Trust Policy: " + validationResult.error);
        return;
    }

    const data = {
        roleName: roleName,
        description: document.getElementById("roleDescription").value,
        maxSessionDuration: maxSessionDuration,
        trustPolicyJson: trustPolicyStr,
        attachedPolicies: policies
    };

    // Determine if we are creating or updating
    // We check if the name input is disabled, which is set in editRole
    const isEdit = document.getElementById("roleName").disabled;
    const method = isEdit ? "PUT" : "POST";
    const url = isEdit ? "/api/object-store/roles/" + encodeURIComponent(roleName) : "/api/object-store/roles";

    fetch(url, {
        method: method,
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(data)
    })
        .then(response => {
            if (response.ok) return;
            return response.text().then(text => { throw new Error(text) });
        })
        .then(() => {
            const modalEl = document.getElementById('roleModal');
            const modal = bootstrap.Modal.getInstance(modalEl);

            if (modal) {
                modalEl.addEventListener('hidden.bs.modal', function () {
                    location.reload();
                }, { once: true });
                modal.hide();
            } else {
                location.reload();
            }
        })
        .catch(err => {
            alert("Error saving role: " + err.message);
        });
}

function deleteRole(name) {
    if (confirm("Are you sure you want to delete role " + name + "?")) {
        fetch("/api/object-store/roles/" + encodeURIComponent(name), {
            method: "DELETE"
        })
            .then(response => {
                if (response.ok) return;
                return response.text().then(text => { throw new Error(text) });
            })
            .then(() => {
                location.reload();
            })
            .catch(err => {
                alert("Error deleting role: " + err.message);
            });
    }
}

function previewRole(btnOrElement) {
    const btn = btnOrElement.tagName ? btnOrElement : null;
    if (!btn) return;

    const roleName = btn.getAttribute("data-role-name");
    const roleArn = btn.getAttribute("data-role-arn");
    const description = btn.getAttribute("data-role-desc");
    const duration = btn.getAttribute("data-role-duration");
    const trustPolicyJson = btn.getAttribute("data-role-trust");
    const attachedPoliciesStr = btn.getAttribute("data-role-policies");

    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('rolePreviewModal'));

    document.getElementById("previewRoleName").innerText = roleName;
    document.getElementById("previewRoleArn").innerText = roleArn;
    document.getElementById("previewRoleDescription").innerText = description || "-";
    const durationDisplay = (duration && parseInt(duration, 10) > 0) ? duration + "s" : "Default";
    document.getElementById("previewRoleDuration").innerText = durationDisplay;

    try {
        const obj = JSON.parse(trustPolicyJson);
        document.getElementById("previewTrustPolicy").innerText = JSON.stringify(obj, null, 2);
    } catch (e) {
        document.getElementById("previewTrustPolicy").innerText = trustPolicyJson;
    }

    const container = document.getElementById("previewAttachedPolicies");
    container.innerHTML = "";

    try {
        const attachedPolicies = JSON.parse(attachedPoliciesStr);
        if (attachedPolicies && Array.isArray(attachedPolicies) && attachedPolicies.length > 0) {
            attachedPolicies.forEach(p => {
                const span = document.createElement("span");
                span.className = "badge bg-secondary me-1";
                span.innerText = p;
                container.appendChild(span);
            });
        } else {
            container.innerText = "-";
        }
    } catch (e) {
        container.innerText = "-";
    }

    modal.show();
}

// ============================================================================
// GROUP MANAGEMENT FUNCTIONS
// ============================================================================

// Group Actions
document.addEventListener('click', function (e) {
    const button = e.target.closest('[data-action]');
    if (!button) return;
    const action = button.getAttribute('data-action');

    switch (action) {
        case 'create-group':
            const createGroupModalEl = document.getElementById('groupModal');
            if (createGroupModalEl) {
                const createModal = bootstrap.Modal.getOrCreateInstance(createGroupModalEl);
                createModal.show();
            }
            break;
        case 'save-group':
            handleCreateGroup();
            break;
        case 'delete-group':
            const groupName = button.getAttribute('data-group-name');
            confirmDeleteGroup(groupName);
            break;
        case 'view-group':
            const viewGroupName = button.getAttribute('data-group-name');
            const viewGroupId = button.getAttribute('data-group-id');
            showGroupDetails(viewGroupName, viewGroupId);
            break;
        case 'add-member':
            handleAddMemberToGroup();
            break;
        case 'attach-policy':
            handleAttachPolicyToGroup();
            break;
    }
});

async function handleCreateGroup() {
    const nameInput = document.getElementById('groupName');
    const groupName = nameInput.value.trim();
    if (!groupName) {
        showErrorMessage('Group name is required');
        return;
    }

    const userSelect = document.getElementById('groupUsers');
    const policySelect = document.getElementById('groupPolicies');
    const selectedUsers = userSelect
        ? Array.from(userSelect.selectedOptions)
            .filter(opt => !opt.disabled && opt.value)
            .map(opt => opt.value)
        : [];
    const selectedPolicies = policySelect
        ? Array.from(policySelect.selectedOptions)
            .filter(opt => !opt.disabled && opt.value)
            .map(opt => opt.value)
        : [];

    // Show loading state
    const submitBtn = document.querySelector('#groupModal button[data-action="save-group"]');
    if (!submitBtn) {
        throw new Error('Submit button not found');
    }
    const originalText = submitBtn.innerText;
    submitBtn.innerText = 'Creating...';
    submitBtn.disabled = true;

    try {
        // 1. Create Group
        const response = await fetch('/api/iam/groups', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ groupName: groupName })
        });

        if (!response.ok) {
            const err = await response.json();
            throw new Error(err.error || 'Failed to create group');
        }

        const errors = [];

        // 2. Add Members
        if (selectedUsers.length > 0) {
            const userPromises = selectedUsers.map(username =>
                fetch(`/api/iam/groups/${encodeURIComponent(groupName)}/members/${encodeURIComponent(username)}`, { method: 'POST' })
                    .then(res => {
                        if (!res.ok) {
                            return res.json().then(err => {
                                errors.push(`Failed to add user ${username}: ${err.error || 'Unknown error'}`);
                            }).catch(() => {
                                errors.push(`Failed to add user ${username}: HTTP ${res.status}`);
                            });
                        }
                    })
                    .catch(err => {
                        errors.push(`Failed to add user ${username}: ${err.message}`);
                    })
            );
            await Promise.all(userPromises);
        }

        // 3. Attach Policies
        if (selectedPolicies.length > 0) {
            const policyPromises = selectedPolicies.map(policyArn =>
                fetch(`/api/iam/groups/${encodeURIComponent(groupName)}/policies`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ policyArn: policyArn })
                }).then(res => {
                    if (!res.ok) {
                        return res.json().then(err => {
                            errors.push(`Failed to attach policy ${policyArn}: ${err.error || 'Unknown error'}`);
                        }).catch(() => {
                            errors.push(`Failed to attach policy ${policyArn}: HTTP ${res.status}`);
                        });
                    }
                })
                    .catch(err => {
                        errors.push(`Failed to attach policy ${policyArn}: ${err.message}`);
                    })
            );
            await Promise.all(policyPromises);
        }

        if (errors.length > 0) {
            showErrorMessage(`Group created but some actions failed:\n${errors.join('\n')}`);
            setTimeout(() => window.location.reload(), RELOAD_DELAY_PARTIAL_FAILURE_MS);
        } else {
            showSuccessMessage('Group created successfully');
            const createModalEl = document.getElementById('groupModal');
            const createModal = bootstrap.Modal.getInstance(createModalEl);
            if (createModal) {
                createModal.hide();
            }
            setTimeout(() => window.location.reload(), RELOAD_DELAY_SUCCESS_MS);
        }

    } catch (e) {
        showErrorMessage(e.message);
        submitBtn.innerText = originalText;
        submitBtn.disabled = false;
    }
}

async function populateGroupCreationModal() {
    const userSelect = document.getElementById('groupUsers');
    const policySelect = document.getElementById('groupPolicies');

    if (!userSelect || !policySelect) return;

    // Reset
    userSelect.innerHTML = '<option value="" disabled>Loading...</option>';
    policySelect.innerHTML = '<option value="" disabled>Loading...</option>';

    try {
        // Fetch Users
        const usersRes = await fetch('/api/iam/users');
        if (usersRes.ok) {
            const data = await usersRes.json();
            const users = data.users || [];
            if (users.length === 0) {
                userSelect.innerHTML = '<option value="" disabled>No users found</option>';
            } else {
                userSelect.innerHTML = users.map(u => `<option value="${escapeHtml(u.userName)}">${escapeHtml(u.userName)}</option>`).join('');
            }
        } else {
            userSelect.innerHTML = '<option value="" disabled>Error loading users</option>';
        }

        // Fetch Policies
        const policiesRes = await fetch('/api/object-store/policies');
        if (policiesRes.ok) {
            const data = await policiesRes.json();
            const policies = data.policies || [];
            if (policies.length === 0) {
                policySelect.innerHTML = '<option value="" disabled>No policies found</option>';
            } else {
                policySelect.innerHTML = policies.map(p => {
                    const name = p.policyName || p.name;
                    // Use ARN as value for Groups as backend expects it
                    const value = p.arn || p.Arn || "";
                    if (!value) {
                        console.warn(`Policy missing ARN: ${name}`);
                        return '';
                    }
                    return `<option value="${escapeHtml(value)}">${escapeHtml(name)}</option>`;
                }).filter(opt => opt).join('');
            }
        } else {
            policySelect.innerHTML = '<option value="" disabled>Error loading policies</option>';
        }

    } catch (e) {
        console.error("Error populating group modal:", e);
        userSelect.innerHTML = '<option value="" disabled>Error</option>';
        policySelect.innerHTML = '<option value="" disabled>Error</option>';
    }
}

async function populateGroupEditMultiSelects(groupName) {
    const userSelect = document.getElementById('editGroupMembers');
    const policySelect = document.getElementById('editGroupPolicies');
    const groupNameEl = document.getElementById('detailsGroupName');
    const groupIdEl = document.getElementById('detailsGroupId');

    if (!userSelect || !policySelect) return;

    // Reset and Load State
    userSelect.innerHTML = '<option disabled>Loading...</option>';
    policySelect.innerHTML = '<option disabled>Loading...</option>';

    // Fetch Group Details to get current members/policies
    let currentMembers = [];
    let currentPolicies = [];

    try {
        const groupRes = await fetch(`/api/iam/groups/${groupName}`);
        if (groupRes.ok) {
            const groupWrap = await groupRes.json();
            // GetGroup API response structure: wrap might be directly group or include users
            // Based on previous code, members/policies might be top level or inside.
            // Let's assume standard response handling, but check structure.
            // The previous loadGroupMembers used group.members directly.

            // NOTE: The previous code implies fetch returns JSON with members/policies lists.
            // Adjust based on observation if needed. Assuming: { members: [], attachedPolicies: [], GroupId: ... }

            currentMembers = groupWrap.members || [];
            currentPolicies = groupWrap.attachedPolicies || [];

            if (groupNameEl) groupNameEl.innerText = groupWrap.GroupName || groupName;
            if (groupIdEl) groupIdEl.innerText = groupWrap.GroupId || "";

            originalGroupMembers = [...currentMembers];
            originalGroupPolicies = [...currentPolicies];
        }
    } catch (e) {
        console.error("Failed to fetch group details:", e);
    }

    try {
        // Fetch All Users
        fetch('/api/iam/users')
            .then(res => res.json())
            .then(data => {
                const users = data.users || [];
                userSelect.innerHTML = users.map(u => {
                    const isSelected = currentMembers.includes(u.userName) ? 'selected' : '';
                    return `<option value="${escapeHtml(u.userName)}" ${isSelected}>${escapeHtml(u.userName)}</option>`;
                }).join('');
            }).catch(e => {
                console.error(e);
                userSelect.innerHTML = '<option disabled>Error loading users</option>';
            });

        // Fetch All Policies
        fetch('/api/object-store/policies')
            .then(res => res.json())
            .then(data => {
                const policies = data.policies || [];
                policySelect.innerHTML = policies.map(p => {
                    const name = p.policyName || p.name;
                    const value = p.arn || p.Arn;
                    if (!value) return '';
                    const isSelected = currentPolicies.includes(value) ? 'selected' : '';
                    return `<option value="${escapeHtml(value)}" ${isSelected}>${escapeHtml(name)}</option>`;
                }).join('');
            }).catch(e => {
                console.error(e);
                policySelect.innerHTML = '<option disabled>Error loading policies</option>';
            });

    } catch (e) {
        console.error("Error populating edit multi-selects:", e);
    }
}

function confirmDeleteGroup(groupName) {
    if (typeof confirmAction === 'function') {
        confirmAction(
            `Are you sure you want to delete group "${groupName}"?`,
            () => deleteGroupConfirmed(groupName)
        );
    } else {
        if (confirm(`Are you sure you want to delete group "${groupName}"?`)) {
            deleteGroupConfirmed(groupName);
        }
    }
}

async function deleteGroupConfirmed(groupName) {
    try {
        const response = await fetch(`/api/iam/groups/${groupName}`, { method: 'DELETE' });
        if (response.ok) {
            window.location.reload();
        } else {
            const err = await response.json();
            showErrorMessage(err.error || 'Failed to delete group');
        }
    } catch (e) {
        showErrorMessage(e.message);
    }
}

async function showGroupDetails(groupName, groupId) {
    currentStatsGroupName = groupName; // Store for global access

    // Open modal immediately
    const modalEl = document.getElementById('groupDetailsModal'); // Note: Previous ID was groupDetailsModal?? check templ
    // Actually, looking at previous code, the View button targeted 'groupDetailsModal' or reused 'groupModal'?
    // Step 268 showed Create Modal id="groupModal". NOT groupDetailsModal.
    // Wait, Step 168 showed "Group Details" modal content inside... wait.
    // Ah, the templ file has TWO modals? Or reused?
    // Let's assume the ID used in templates is 'groupDetailsModal' for the details (Wait, templ lines 135+).
    // Let's assume it works as before but call our population function.

    if (modalEl) {
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        modal.show();
    } else {
        // Maybe it's reused? But the template has distinct structures. 
        // I will trust the ID 'groupDetailsModal' exists if it was working before.
        // Actually, let's verify if the ID is correct. 
        // In Step 268 snippet, I only saw 'groupModal'. Let's assume there is a 'groupDetailsModal' further down.
        // If not, I might have an issue. But the JS was selecting it.
        const targetModal = document.getElementById('groupDetailsModal') || document.getElementById('groupModal'); // Fallback? No, groupModal is create.
        if (targetModal) {
            const modal = bootstrap.Modal.getOrCreateInstance(targetModal);
            modal.show();
        }
    }

    populateGroupEditMultiSelects(groupName);
}

async function handleSaveGroupDetails() {
    const btn = document.querySelector('button[data-action="save-group-details"]');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Saving...';
    }

    const userSelect = document.getElementById('editGroupMembers');
    const policySelect = document.getElementById('editGroupPolicies');

    const selectedMembers = userSelect ? Array.from(userSelect.selectedOptions).map(o => o.value) : [];
    const selectedPolicies = policySelect ? Array.from(policySelect.selectedOptions).map(o => o.value) : [];

    const toAddMembers = selectedMembers.filter(m => !originalGroupMembers.includes(m));
    const toRemoveMembers = originalGroupMembers.filter(m => !selectedMembers.includes(m));

    const toAttachPolicies = selectedPolicies.filter(p => !originalGroupPolicies.includes(p));
    const toDetachPolicies = originalGroupPolicies.filter(p => !selectedPolicies.includes(p));

    let errors = [];

    // Process Member Changes
    for (const m of toAddMembers) {
        try {
            const res = await fetch(`/api/iam/groups/${currentStatsGroupName}/members/${m}`, { method: 'POST' });
            if (!res.ok) errors.push(`Failed to add member ${m}`);
        } catch (e) { errors.push(`Error adding member ${m}: ${e.message}`); }
    }
    for (const m of toRemoveMembers) {
        try {
            const res = await fetch(`/api/iam/groups/${currentStatsGroupName}/members/${m}`, { method: 'DELETE' });
            if (!res.ok) errors.push(`Failed to remove member ${m}`);
        } catch (e) { errors.push(`Error removing member ${m}: ${e.message}`); }
    }

    // Process Policy Changes
    for (const p of toAttachPolicies) {
        try {
            const res = await fetch(`/api/iam/groups/${currentStatsGroupName}/policies`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ policyArn: p })
            });
            if (!res.ok) errors.push(`Failed to attach policy`);
        } catch (e) { errors.push(`Error attaching policy: ${e.message}`); }
    }
    for (const p of toDetachPolicies) {
        try {
            const res = await fetch(`/api/iam/groups/${currentStatsGroupName}/policies?policyArn=${encodeURIComponent(p)}`, { method: 'DELETE' });
            if (!res.ok) errors.push(`Failed to detach policy`);
        } catch (e) { errors.push(`Error detaching policy: ${e.message}`); }
    }

    if (errors.length > 0) {
        showErrorMessage(`Some updates failed:\n${errors.join('\n')}`);
        setTimeout(() => window.location.reload(), RELOAD_DELAY_PARTIAL_FAILURE_MS);
    } else {
        window.location.reload();
    }
}

async function loadGroupMembers(groupName) {
    const list = document.getElementById('groupMembersList');
    if (!list) return;
    list.innerHTML = '<div class="text-center"><i class="fas fa-spinner fa-spin"></i> Loading...</div>';

    try {
        const response = await fetch(`/api/iam/groups/${groupName}`);
        if (response.ok) {
            const group = await response.json();
            if (group.members && group.members.length > 0) {
                list.innerHTML = group.members.map(m => `
                    <div class="list-group-item d-flex justify-content-between align-items-center">
                        <span>${escapeHtml(m)}</span>
                        <button class="btn btn-sm btn-outline-danger" onclick="removeMemberFromGroup('${escapeHtml(groupName)}', '${escapeHtml(m)}')">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                `).join('');
            } else {
                list.innerHTML = '<div class="text-muted p-2">No members found</div>';
            }
        }
    } catch (e) {
        list.innerText = "Error loading members";
    }
}

async function loadGroupPolicies(groupName) {
    const list = document.getElementById('groupPoliciesList');
    if (!list) return;
    list.innerHTML = '<div class="text-center"><i class="fas fa-spinner fa-spin"></i> Loading...</div>';

    try {
        const response = await fetch(`/api/iam/groups/${groupName}`); // Reuse fetch
        if (response.ok) {
            const group = await response.json();
            if (group.attachedPolicies && group.attachedPolicies.length > 0) {
                list.innerHTML = group.attachedPolicies.map(p => `
                    <div class="list-group-item d-flex justify-content-between align-items-center">
                        <span>${escapeHtml(p)}</span>
                        <button class="btn btn-sm btn-outline-danger" onclick="detachPolicyFromGroup('${escapeHtml(groupName)}', '${escapeHtml(p)}')">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                `).join('');
            } else {
                list.innerHTML = '<div class="text-muted p-2">No policies attached</div>';
            }
        }
    } catch (e) {
        list.innerText = "Error loading policies";
    }
}

async function handleAddMemberToGroup() {
    const input = document.getElementById('addUserStats');
    const username = input.value.trim();
    if (!username || !currentStatsGroupName) return;

    try {
        const response = await fetch(`/api/iam/groups/${currentStatsGroupName}/members/${username}`, { method: 'POST' });
        if (response.ok) {
            input.value = '';
            loadGroupMembers(currentStatsGroupName);
        } else {
            const err = await response.json();
            showErrorMessage(err.error || 'Failed to add member');
        }
    } catch (e) {
        showErrorMessage(e.message);
    }
}

window.removeMemberFromGroup = async function (groupName, username) {
    try {
        const response = await fetch(`/api/iam/groups/${groupName}/members/${username}`, { method: 'DELETE' });
        if (response.ok) {
            loadGroupMembers(groupName);
        } else {
            const err = await response.json();
            showErrorMessage(err.error || 'Failed to remove member');
        }
    } catch (e) {
        showErrorMessage(e.message);
    }
};

async function handleAttachPolicyToGroup() {
    const input = document.getElementById('attachPolicyName');
    const policyArn = input.value.trim();
    if (!policyArn || !currentStatsGroupName) return;

    try {
        const response = await fetch(`/api/iam/groups/${currentStatsGroupName}/policies`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ policyArn: policyArn })
        });
        if (response.ok) {
            input.value = '';
            loadGroupPolicies(currentStatsGroupName);
        } else {
            const err = await response.json();
            showErrorMessage(err.error || 'Failed to attach policy');
        }
    } catch (e) {
        showErrorMessage(e.message);
    }
}

window.detachPolicyFromGroup = async function (groupName, policyArn) {
    try {
        const response = await fetch(`/api/iam/groups/${groupName}/policies?policyArn=${encodeURIComponent(policyArn)}`, { method: 'DELETE' });
        if (response.ok) {
            loadGroupPolicies(groupName);
        } else {
            const err = await response.json();
            showErrorMessage(err.error || 'Failed to detach policy');
        }
    } catch (e) {
        showErrorMessage(e.message);
    }
};

// Initialize event listeners for IAM User Management
let iamEventListenerAttached = false;
document.addEventListener('DOMContentLoaded', function () {
    // Guard against multiple registrations
    if (iamEventListenerAttached) return;
    iamEventListenerAttached = true;

    // Use event delegation for all IAM user action buttons
    document.addEventListener('click', function (e) {
        const target = e.target.closest('[data-action]');
        if (!target) return;

        const action = target.dataset.action;
        const username = target.dataset.username;

        switch (action) {
            case 'view-user':
                if (username) showUserDetails(username);
                break;
            case 'manage-access-keys':
                if (username) manageAccessKeys(username);
                break;
            case 'delete-user':
                if (username) deleteUser(username);
                break;
            case 'create-user-submit':
                handleCreateUser();
                break;
            case 'create-access-key':
                createAccessKey();
                break;
            case 'save-group-details':
                handleSaveGroupDetails();
                break;
        }
    });
});
