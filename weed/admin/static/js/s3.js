// SeaweedFS Dashboard S3 Logic
// Requires helpers.js to be loaded first

// Initialize S3 listeners using event delegation
document.addEventListener('DOMContentLoaded', function () {
    // Event delegation for S3 action buttons
    document.addEventListener('click', function (e) {
        const target = e.target;

        // Handle "Create Bucket" Submit
        if (target.matches('[data-action="create-bucket-submit"]')) {
            handleCreateBucketSubmit();
            return;
        }

        // Handle "Delete Bucket" Button (Open Modal)
        const deleteBtn = target.closest('[data-action="delete-bucket"]');
        if (deleteBtn) {
            confirmDeleteBucket(deleteBtn.dataset.bucketName);
            return;
        }

        // Handle "Delete Bucket" Confirm (In Modal)
        if (target.matches('[data-action="delete-bucket-confirm"]')) {
            handleDeleteBucketConfirm();
            return;
        }

        // Handle "Manage Quota" Button (Open Modal)
        const quotaBtn = target.closest('[data-action="manage-quota"]');
        if (quotaBtn) {
            openQuotaModal(quotaBtn);
            return;
        }

        // Handle "Update Quota" Submit
        if (target.matches('[data-action="update-quota-submit"]')) {
            handleQuotaSubmit();
            return;
        }

        // Handle "View Details" Button (Open Modal)
        const detailsBtn = target.closest('[data-action="view-bucket-details"]');
        if (detailsBtn) {
            openBucketDetailsModal(detailsBtn.dataset.bucketName);
            return;
        }

        // Handle "Export List"
        if (target.matches('[data-action="export-buckets"]') || target.closest('[data-action="export-buckets"]')) {
            e.preventDefault(); // Prevent default link behavior
            exportBucketList();
            return;
        }
    });

    // Delegated Change Listeners for Toggles (Create Bucket Form)
    document.addEventListener('change', function (e) {
        const target = e.target;

        // Quota Toggle
        if (target.id === 'enableQuota') {
            document.getElementById('quotaSettings').style.display = target.checked ? 'block' : 'none';
        }

        // Object Lock Toggle
        if (target.id === 'enableObjectLock') {
            const versioningCheckbox = document.getElementById('enableVersioning');
            const defaultRetentionSettings = document.getElementById('defaultRetentionSettings');
            const setDefaultRetentionCheckbox = document.getElementById('setDefaultRetention');

            document.getElementById('objectLockSettings').style.display = target.checked ? 'block' : 'none';
            if (target.checked) {
                if (versioningCheckbox) {
                    versioningCheckbox.checked = true;
                    versioningCheckbox.disabled = true;
                }
            } else {
                if (versioningCheckbox) versioningCheckbox.disabled = false;
                // Reset default retention
                if (setDefaultRetentionCheckbox) setDefaultRetentionCheckbox.checked = false;
                if (defaultRetentionSettings) defaultRetentionSettings.style.display = 'none';
            }
        }

        // Default Retention Toggle
        if (target.id === 'setDefaultRetention') {
            document.getElementById('defaultRetentionSettings').style.display = target.checked ? 'block' : 'none';
        }

        // Quota Modal Enabled Toggle
        if (target.id === 'quotaEnabled') {
            document.getElementById('quotaSizeSettings').style.display = target.checked ? 'block' : 'none';
        }
    });

    // Cleanup modals on hidden
    ['createBucketModal', 'deleteBucketModal', 'manageQuotaModal', 'bucketDetailsModal'].forEach(modalId => {
        const modalEl = document.getElementById(modalId);
        if (modalEl) {
            modalEl.addEventListener('hidden.bs.modal', function () {
                // Ensure no backdrops remain
                document.querySelectorAll('.modal-backdrop').forEach(backdrop => backdrop.remove());
                document.body.classList.remove('modal-open');
                document.body.style.removeProperty('padding-right');
            });
        }
    });

    // Bucket Name Validation
    const bucketNameInput = document.getElementById('bucketName');
    if (bucketNameInput) {
        bucketNameInput.addEventListener('input', validateBucketName);
    }
});

function validateBucketName(e) {
    const input = e.target;
    const value = input.value;
    const feedback = input.nextElementSibling; // Assuming a feedback div exists or we create/use customized logic

    // S3 Bucket Naming Rules
    // 1. Length 3-63
    // 2. Lowercase letters, numbers, dots, hyphens
    // 3. Start/end with letter or number
    // 4. Not IP address format (simplified check)

    let isValid = true;
    let message = '';

    if (value.length < 3 || value.length > 63) {
        isValid = false;
        message = 'Bucket name must be between 3 and 63 characters.';
    } else if (!/^[a-z0-9.-]+$/.test(value)) {
        isValid = false;
        message = 'Only lowercase letters, numbers, dots, and hyphens are allowed.';
    } else if (/^[.-]|[.-]$/.test(value)) {
        isValid = false;
        message = 'Bucket name must start and end with a letter or number.';
    } else if (value.includes('..')) {
        isValid = false;
        message = 'Bucket name cannot contain consecutive periods.';
    } else if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(value)) {
        isValid = false;
        message = 'Bucket name cannot be formatted as an IP address.';
    }

    if (isValid) {
        input.classList.remove('is-invalid');
        input.classList.add('is-valid');
    } else {
        input.classList.remove('is-valid');
        input.classList.add('is-invalid');
        // If there's a feedback element, update it
        if (feedback && feedback.classList.contains('invalid-feedback')) {
            feedback.textContent = message;
        }
    }
}

// Global state for this module
var bucketToDelete = '';
var currentBucketToUpdate = '';

// ----------------------------------------------------------------------------
// HANDLERS
// ----------------------------------------------------------------------------

function handleCreateBucketSubmit() {
    const form = document.getElementById('createBucketForm');
    if (!form) return;

    const formData = new FormData(form);
    const quotaCheckbox = document.getElementById('enableQuota');
    const versioningCheckbox = document.getElementById('enableVersioning');
    const objectLockCheckbox = document.getElementById('enableObjectLock');
    const setDefaultRetentionCheckbox = document.getElementById('setDefaultRetention');

    const bucketData = {
        name: formData.get('name'),
        region: formData.get('region') || '',
        quota_size: quotaCheckbox && quotaCheckbox.checked ? parseInt(formData.get('quota_size')) || 0 : 0,
        quota_unit: formData.get('quota_unit') || 'MB',
        quota_enabled: quotaCheckbox ? quotaCheckbox.checked : false,
        versioning_enabled: versioningCheckbox ? versioningCheckbox.checked : false,
        object_lock_enabled: objectLockCheckbox ? objectLockCheckbox.checked : false,
        object_lock_mode: formData.get('object_lock_mode') || 'GOVERNANCE',
        set_default_retention: setDefaultRetentionCheckbox ? setDefaultRetentionCheckbox.checked : false,
        object_lock_duration: (setDefaultRetentionCheckbox && setDefaultRetentionCheckbox.checked) ? parseInt(formData.get('object_lock_duration')) || 30 : 0
    };

    // Validate object lock settings
    if (bucketData.object_lock_enabled && bucketData.set_default_retention && bucketData.object_lock_duration <= 0) {
        alert('Please enter a valid retention duration for object lock.');
        return;
    }

    fetch('/api/s3/buckets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(bucketData)
    })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                alert('Error creating bucket: ' + data.error);
            } else {
                alert('Bucket created successfully!');
                const modal = bootstrap.Modal.getInstance(document.getElementById('createBucketModal'));
                if (modal) modal.hide();
                setTimeout(() => location.reload(), 500);
            }
        })
        .catch(error => {
            console.error('Error:', error);
            alert('Error creating bucket: ' + error.message);
        });
}

function confirmDeleteBucket(bucketName) {
    bucketToDelete = bucketName;
    const nameSpan = document.getElementById('deleteBucketName');
    if (nameSpan) nameSpan.textContent = bucketName;

    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('deleteBucketModal'));
    modal.show();
}

function handleDeleteBucketConfirm() {
    if (!bucketToDelete) return;

    fetch(`/api/s3/buckets/${bucketToDelete}`, {
        method: 'DELETE'
    })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                alert('Error deleting bucket: ' + data.error);
            } else {
                alert('Bucket deleted successfully!');
                const modal = bootstrap.Modal.getInstance(document.getElementById('deleteBucketModal'));
                if (modal) modal.hide();
                setTimeout(() => location.reload(), 500);
            }
        })
        .catch(error => {
            console.error('Error:', error);
            alert('Error deleting bucket: ' + error.message);
        });

    bucketToDelete = '';
}

function openQuotaModal(btn) {
    const bucketName = btn.dataset.bucketName;
    const currentQuotaMB = parseInt(btn.dataset.currentQuota) || 0;
    const quotaEnabled = btn.dataset.quotaEnabled === 'true';

    currentBucketToUpdate = bucketName;

    document.getElementById('quotaBucketName').value = bucketName;
    document.getElementById('quotaEnabled').checked = quotaEnabled;

    // Convert quota to appropriate unit and set values
    const quotaBytes = currentQuotaMB * 1024 * 1024; // Convert MB to bytes
    const { size, unit } = convertBytesToBestUnit(quotaBytes);

    document.getElementById('quotaSizeMB').value = size;
    const unitSelect = document.getElementById('quotaUnitMB');
    if (unitSelect) unitSelect.value = unit;

    document.getElementById('quotaSizeSettings').style.display = quotaEnabled ? 'block' : 'none';

    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('manageQuotaModal'));
    modal.show();
}

function convertBytesToBestUnit(bytes) {
    if (bytes === 0) {
        return { size: 0, unit: 'MB' };
    }
    // Check if it's a clean TB value
    if (bytes >= 1024 * 1024 * 1024 * 1024 && bytes % (1024 * 1024 * 1024 * 1024) === 0) {
        return { size: bytes / (1024 * 1024 * 1024 * 1024), unit: 'TB' };
    }
    // Check if it's a clean GB value
    if (bytes >= 1024 * 1024 * 1024 && bytes % (1024 * 1024 * 1024) === 0) {
        return { size: bytes / (1024 * 1024 * 1024), unit: 'GB' };
    }
    // Default to MB
    return { size: bytes / (1024 * 1024), unit: 'MB' };
}

function handleQuotaSubmit() {
    const form = document.getElementById('quotaForm');
    if (!form) return;

    const formData = new FormData(form);
    const enabled = document.getElementById('quotaEnabled').checked;
    const data = {
        quota_size: enabled ? parseInt(formData.get('quota_size')) || 0 : 0,
        quota_unit: formData.get('quota_unit') || 'MB',
        quota_enabled: enabled
    };

    fetch(`/api/s3/buckets/${currentBucketToUpdate}/quota`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                alert('Error updating quota: ' + data.error);
            } else {
                alert('Quota updated successfully!');
                const modal = bootstrap.Modal.getInstance(document.getElementById('manageQuotaModal'));
                if (modal) modal.hide();
                setTimeout(() => location.reload(), 500);
            }
        })
        .catch(error => {
            console.error('Error:', error);
            alert('Error updating quota: ' + error.message);
        });
}

function openBucketDetailsModal(bucketName) {
    document.getElementById('bucketDetailsModalLabel').innerHTML =
        '<i class="fas fa-cube me-2"></i>Bucket Details - ' + bucketName;

    document.getElementById('bucketDetailsContent').innerHTML =
        '<div class="text-center py-4">' +
        '<div class="spinner-border text-primary" role="status">' +
        '<span class="visually-hidden">Loading...</span>' +
        '</div>' +
        '<div class="mt-2">Loading bucket details...</div>' +
        '</div>';

    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('bucketDetailsModal'));
    modal.show();

    fetch('/api/s3/buckets/' + bucketName)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                document.getElementById('bucketDetailsContent').innerHTML =
                    '<div class="alert alert-danger">' +
                    '<i class="fas fa-exclamation-triangle me-2"></i>' +
                    'Error loading bucket details: ' + data.error +
                    '</div>';
            } else {
                displayBucketDetails(data);
            }
        })
        .catch(error => {
            console.error('Error fetching bucket details:', error);
            document.getElementById('bucketDetailsContent').innerHTML =
                '<div class="alert alert-danger">' +
                '<i class="fas fa-exclamation-triangle me-2"></i>' +
                'Error loading bucket details: ' + error.message +
                '</div>';
        });
}

function displayBucketDetails(data) {
    const bucket = data.bucket;
    const objects = data.objects || [];

    // Helper function to format bytes
    function formatBytes(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    let content = `
        <div class="row mb-4">
            <div class="col-md-6">
                <div class="card h-100">
                    <div class="card-header font-weight-bold">Properties</div>
                    <div class="card-body">
                        <ul class="list-group list-group-flush">
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Created
                                <span>${new Date(bucket.created_at).toLocaleString()}</span>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Size
                                <span>${formatBytes(bucket.size || 0)}</span>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Object Count
                                <span>${bucket.object_count || 0}</span>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card h-100">
                    <div class="card-header font-weight-bold">Settings</div>
                    <div class="card-body">
                        <ul class="list-group list-group-flush">
                             <li class="list-group-item d-flex justify-content-between align-items-center">
                                Versioning
                                ${bucket.versioning_enabled
            ? '<span class="badge bg-success">Enabled</span>'
            : '<span class="badge bg-secondary">Disabled</span>'}
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Object Lock
                                ${bucket.object_lock_enabled
            ? '<span class="badge bg-warning">Enabled</span>'
            : '<span class="badge bg-secondary">Disabled</span>'}
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center">
                                Quota
                                ${bucket.quota_enabled
            ? `<span class="badge bg-info">${formatBytes(bucket.quota)}</span>`
            : '<span class="badge bg-secondary">Disabled</span>'}
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    `;

    document.getElementById('bucketDetailsContent').innerHTML = content;
}

function exportBucketList() {
    const table = document.getElementById('bucketsTable');
    if (!table) return;

    const rows = Array.from(table.querySelectorAll('tbody tr'));
    const data = rows.map(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length < 5) return null;

        return {
            name: cells[0].textContent.trim(),
            created: cells[1].textContent.trim(),
            objects: cells[2].textContent.trim(),
            size: cells[3].textContent.trim(),
            quota: cells[4].textContent.trim()
        };
    }).filter(item => item !== null);

    const csv = [
        ['Name', 'Created', 'Objects', 'Size', 'Quota'].join(','),
        ...data.map(row => [
            row.name,
            row.created,
            row.objects,
            row.size,
            row.quota
        ].join(','))
    ].join('\n');

    const filename = `seaweedfs-buckets-${new Date().toISOString().split('T')[0]}.csv`;
    if (window.Dashboard && window.Dashboard.downloadCSV) {
        window.Dashboard.downloadCSV(csv, filename);
    } else {
        // Fallback or use simple download
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.setAttribute('hidden', '');
        a.setAttribute('href', url);
        a.setAttribute('download', filename);
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }
}
