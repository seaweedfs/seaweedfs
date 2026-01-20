// SeaweedFS Dashboard JavaScript

// Global variables


// Initialize dashboard when DOM is loaded
// Initialize dashboard logic - runs on load and after HTMX swaps
function initApp() {
    initializeDashboard();
    initializeContentEventHandlers();
    setupFormValidation();
    setupFileManagerEventHandlers();

    // Initialize delete button visibility on file browser page
    if (window.location.pathname === '/files') {
        updateDeleteSelectedButton();
    }
}

// Global initialization - runs ONCE
function initializeGlobal() {
    initApp();
    initializeGlobalEventHandlers();
    setupHTMXListeners();
    setupAutoRefresh();
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', initializeGlobal);

// Re-initialize after HTMX page swaps (SPA behavior)
document.addEventListener('htmx:afterSwap', function (evt) {
    // Global cleanup for modals
    document.querySelectorAll('.modal-backdrop').forEach(backdrop => backdrop.remove());
    document.body.classList.remove('modal-open');
    document.body.style.removeProperty('padding-right');

    // Re-initialize app logic for the new content
    initApp();
});

function initializeDashboard() {
    // Initialize tooltips
    initializeTooltips();

    // Set active navigation
    setActiveNavigation();
}

// HTMX event listeners
function setupHTMXListeners() {
    // Show loading indicator on requests
    document.body.addEventListener('htmx:beforeRequest', function (evt) {
        showLoadingIndicator();
    });

    // Hide loading indicator on completion
    document.body.addEventListener('htmx:afterRequest', function (evt) {
        hideLoadingIndicator();
    });

    // Handle errors
    document.body.addEventListener('htmx:responseError', function (evt) {
        handleHTMXError(evt);
    });
}

// Initialize Bootstrap tooltips
function initializeTooltips() {
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

// Set up auto-refresh for dashboard data
function setupAutoRefresh() {
    if (window.autoRefreshInterval) return; // Prevent duplicates

    // Refresh dashboard data every 30 seconds
    window.autoRefreshInterval = setInterval(function () {
        if (window.location.pathname === '/dashboard') {
            const dashboardContent = document.getElementById('dashboard-content');
            if (dashboardContent) {
                htmx.trigger('#dashboard-content', 'refresh');
            }
        }
    }, 30000);
}

// Set active navigation item
function setActiveNavigation() {
    const currentPath = window.location.pathname;
    const navLinks = document.querySelectorAll('.sidebar .nav-link');

    navLinks.forEach(function (link) {
        const href = link.getAttribute('href');
        let isActive = false;

        if (href === currentPath) {
            isActive = true;
        } else if (currentPath === '/' && href === '/admin') {
            isActive = true;
        } else if (currentPath.startsWith('/s3/') && href === '/s3/buckets') {
            isActive = true;
        }
        // Note: Removed the problematic cluster condition that was highlighting all submenu items

        if (isActive) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });
}

// Set up submenu behavior
function setupSubmenuBehavior() {
    const currentPath = window.location.pathname;
    // console.log('Setting up submenu behavior for path:', currentPath);

    const expandSubmenu = (submenuId) => {
        const submenu = document.getElementById(submenuId);
        if (submenu) {
            // console.log(`Expanding submenu: ${submenuId}`);
            // Use Bootstrap API to show
            new bootstrap.Collapse(submenu, { toggle: false }).show();

            // Ensure parent button state is correct
            const toggleButton = document.querySelector(`[data-bs-target="#${submenuId}"]`);
            if (toggleButton) {
                toggleButton.classList.remove('collapsed');
                toggleButton.setAttribute('aria-expanded', 'true');
            }
        }
    };

    // If we're on a cluster page, expand the cluster submenu
    if (currentPath.startsWith('/cluster/')) {
        expandSubmenu('clusterSubmenu');
    }

    // If we're on an object store page, expand the object store submenu
    if (currentPath.startsWith('/object-store/')) {
        expandSubmenu('objectStoreSubmenu');
    }

    // If we're on a maintenance page, expand the maintenance submenu
    if (currentPath.startsWith('/maintenance')) {
        expandSubmenu('maintenanceSubmenu');
    }
}






// Utility functions have been moved to helpers.js

// Global error handler
window.addEventListener('error', function (e) {
    console.error('Global error:', e.error);
    showErrorMessage('An unexpected error occurred.');
});

// IAM Validation moved to iam.js

// Initialize global event handlers (delegated)
function initializeGlobalEventHandlers() {
    // Global event handlers have been delegated to specific modules (s3.js, iam.js)
    // or handled via HTMX interactions.

    // Delegated event handling for file checkboxes (handles dynamically added rows)
    document.addEventListener('change', function (e) {
        if (e.target.classList.contains('file-checkbox')) {
            updateDeleteSelectedButton();
            updateSelectAllCheckbox();
        }
    });

    // Delegated event handling for file table action buttons
    document.addEventListener('click', function (e) {
        const button = e.target.closest('[data-action]');
        if (!button) return;

        const action = button.getAttribute('data-action');
        const path = button.getAttribute('data-path');

        if (!path) return;

        switch (action) {
            case 'download':
                downloadFile(path);
                break;
            case 'view':
                viewFile(path);
                break;
            case 'properties':
                showProperties(path);
                break;
            case 'delete':
                if (confirm('Are you sure you want to delete "' + path + '"?')) {
                    deleteFile(path);
                }
                break;
        }
    });

    // Prevent default global drag behaviors
    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        document.body.addEventListener(eventName, preventDefaults, false);
    });
}

// Initialize content-specific event handlers (run on load and after swap)
function initializeContentEventHandlers() {

}

// Setup form validation
function setupFormValidation() {
    // Form validation is now handled in specific modules (s3.js, iam.js)
}

// S3 Bucket Management Functions have been moved to s3.js
// S3 Bucket Management Functions have been moved to s3.js



// Dashboard refresh functionality
function refreshDashboard() {
    location.reload();
}

// Cluster management functions

// Export volume servers data as CSV
function exportVolumeServers() {
    const table = document.getElementById('hostsTable');
    if (!table) {
        showErrorMessage('No volume servers data to export');
        return;
    }

    let csv = 'Server ID,Address,Data Center,Rack,Volumes,Capacity,Usage\n';

    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 7) {
            const rowData = [
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim(),
                cells[4].textContent.trim(),
                cells[5].textContent.trim(),
                cells[6].textContent.trim()
            ];
            csv += rowData.join(',') + '\n';
        }
    });

    downloadCSV(csv, 'seaweedfs-volume-servers.csv');
}

// Export volumes data as CSV
function exportVolumes() {
    const table = document.getElementById('volumesTable');
    if (!table) {
        showErrorMessage('No volumes data to export');
        return;
    }

    // Get headers from the table (dynamically handles conditional columns)
    const headerCells = table.querySelectorAll('thead th');
    const headers = [];
    headerCells.forEach((cell, index) => {
        // Skip the Actions column (last column)
        if (index < headerCells.length - 1) {
            headers.push(cell.textContent.trim());
        }
    });

    let csv = headers.join(',') + '\n';

    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        const rowData = [];
        // Export all cells except the Actions column (last column)
        for (let i = 0; i < cells.length - 1; i++) {
            rowData.push(`"${cells[i].textContent.trim().replace(/"/g, '""')}"`);
        }
        csv += rowData.join(',') + '\n';
    });

    downloadCSV(csv, 'seaweedfs-volumes.csv');
}

// Export collections data as CSV
function exportCollections() {
    const table = document.getElementById('collectionsTable');
    if (!table) {
        showAlert('error', 'Collections table not found');
        return;
    }

    const headers = ['Collection Name', 'Volumes', 'Files', 'Size', 'Disk Types'];
    const rows = [];

    // Get table rows
    const tableRows = table.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 5) {
            rows.push([
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim(),
                formatDiskTypes(cells[4].textContent.trim())
            ]);
        }
    });

    // Generate CSV
    const csvContent = [headers, ...rows]
        .map(row => row.map(cell => `"${cell}"`).join(','))
        .join('\n');

    // Download
    const filename = `seaweedfs-collections-${new Date().toISOString().split('T')[0]}.csv`;
    downloadCSV(csvContent, filename);
}

// Export Masters to CSV
function exportMasters() {
    const table = document.getElementById('mastersTable');
    if (!table) {
        showAlert('error', 'Masters table not found');
        return;
    }

    const headers = ['Address', 'Role', 'Suffrage'];
    const rows = [];

    // Get table rows
    const tableRows = table.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 3) {
            rows.push([
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim()
            ]);
        }
    });

    // Generate CSV
    const csvContent = [headers, ...rows]
        .map(row => row.map(cell => `"${cell}"`).join(','))
        .join('\n');

    // Download
    const filename = `seaweedfs-masters-${new Date().toISOString().split('T')[0]}.csv`;
    downloadCSV(csvContent, filename);
}

// Export Filers to CSV
function exportFilers() {
    const table = document.getElementById('filersTable');
    if (!table) {
        showAlert('error', 'Filers table not found');
        return;
    }

    const headers = ['Address', 'Version', 'Data Center', 'Rack', 'Created At'];
    const rows = [];

    // Get table rows
    const tableRows = table.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 5) {
            rows.push([
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim(),
                cells[4].textContent.trim()
            ]);
        }
    });

    // Generate CSV
    const csvContent = [headers, ...rows]
        .map(row => row.map(cell => `"${cell}"`).join(','))
        .join('\n');

    // Download
    const filename = `seaweedfs-filers-${new Date().toISOString().split('T')[0]}.csv`;
    downloadCSV(csvContent, filename);
}

// Export Users to CSV
function exportUsers() {
    const table = document.getElementById('usersTable');
    if (!table) {
        showAlert('error', 'Users table not found');
        return;
    }

    const rows = table.querySelectorAll('tbody tr');
    if (rows.length === 0) {
        showErrorMessage('No users to export');
        return;
    }

    let csvContent = 'Username,Email,Access Key,Status,Created,Last Login\n';

    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 6) {
            const username = cells[0].textContent.trim();
            const email = cells[1].textContent.trim();
            const accessKey = cells[2].textContent.trim();
            const status = cells[3].textContent.trim();
            const created = cells[4].textContent.trim();
            const lastLogin = cells[5].textContent.trim();

            csvContent += `"${username}","${email}","${accessKey}","${status}","${created}","${lastLogin}"\n`;
        }
    });

    downloadCSV(csvContent, 'seaweedfs-users.csv');
}

// Confirm delete collection
function confirmDeleteCollection(button) {
    const collectionName = button.getAttribute('data-collection-name');
    document.getElementById('deleteCollectionName').textContent = collectionName;

    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('deleteCollectionModal'));
    modal.show();

    // Set up confirm button
    document.getElementById('confirmDeleteCollection').onclick = function () {
        deleteCollection(collectionName);
    };
}

// Delete collection
async function deleteCollection(collectionName) {
    try {
        const response = await fetch(`/api/collections/${collectionName}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
            }
        });

        if (response.ok) {
            showSuccessMessage(`Collection "${collectionName}" deleted successfully`);
            // Hide modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('deleteCollectionModal'));
            modal.hide();
            // Refresh page
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            const error = await response.json();
            showErrorMessage(`Failed to delete collection: ${error.error || 'Unknown error'}`);
        }
    } catch (error) {
        console.error('Error deleting collection:', error);
        showErrorMessage('Failed to delete collection. Please try again.');
    }
}





// File Browser Functions

// Toggle select all checkboxes
function toggleSelectAll() {
    const selectAll = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.file-checkbox');

    checkboxes.forEach(checkbox => {
        checkbox.checked = selectAll.checked;
    });

    updateDeleteSelectedButton();
}

// Update visibility of delete selected button based on selection
function updateDeleteSelectedButton() {
    const checkboxes = document.querySelectorAll('.file-checkbox:checked');
    const deleteBtn = document.getElementById('deleteSelectedBtn');

    if (deleteBtn) {
        if (checkboxes.length > 0) {
            deleteBtn.style.display = 'inline-block';
            deleteBtn.innerHTML = `<i class="fas fa-trash me-1"></i>Delete Selected (${checkboxes.length})`;
        } else {
            deleteBtn.style.display = 'none';
        }
    }
}

// Update select all checkbox state based on individual selections
function updateSelectAllCheckbox() {
    const selectAll = document.getElementById('selectAll');
    const allCheckboxes = document.querySelectorAll('.file-checkbox');
    const checkedCheckboxes = document.querySelectorAll('.file-checkbox:checked');

    if (selectAll && allCheckboxes.length > 0) {
        if (checkedCheckboxes.length === 0) {
            selectAll.checked = false;
            selectAll.indeterminate = false;
        } else if (checkedCheckboxes.length === allCheckboxes.length) {
            selectAll.checked = true;
            selectAll.indeterminate = false;
        } else {
            selectAll.checked = false;
            selectAll.indeterminate = true;
        }
    }
}

// Get selected file paths
function getSelectedFilePaths() {
    const checkboxes = document.querySelectorAll('.file-checkbox:checked');
    return Array.from(checkboxes).map(cb => cb.value);
}

// Confirm delete selected files
function confirmDeleteSelected() {
    const selectedPaths = getSelectedFilePaths();

    if (selectedPaths.length === 0) {
        showAlert('warning', 'No files selected');
        return;
    }

    const fileNames = selectedPaths.map(path => path.split('/').pop()).join(', ');
    const message = selectedPaths.length === 1
        ? `Are you sure you want to delete "${fileNames}"?`
        : `Are you sure you want to delete ${selectedPaths.length} selected items?\n\n${fileNames.substring(0, 200)}${fileNames.length > 200 ? '...' : ''}`;

    if (confirm(message)) {
        deleteSelectedFiles(selectedPaths);
    }
}

// Delete multiple selected files
async function deleteSelectedFiles(filePaths) {
    if (!filePaths || filePaths.length === 0) {
        showAlert('warning', 'No files selected');
        return;
    }

    // Disable the delete button during operation
    const deleteBtn = document.getElementById('deleteSelectedBtn');
    const originalText = deleteBtn.innerHTML;
    deleteBtn.disabled = true;
    deleteBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Deleting...';

    try {
        const response = await fetch('/api/files/delete-multiple', {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ paths: filePaths })
        });

        if (response.ok) {
            const result = await response.json();

            if (result.deleted > 0) {
                if (result.failed === 0) {
                    showAlert('success', `Successfully deleted ${result.deleted} item(s)`);
                } else {
                    showAlert('warning', `Deleted ${result.deleted} item(s), failed to delete ${result.failed} item(s)`);
                    if (result.errors && result.errors.length > 0) {
                        console.warn('Deletion errors:', result.errors);
                    }
                }

                // Reload the page to update the file list
                setTimeout(() => {
                    window.location.reload();
                }, 1000);
            } else {
                let errorMessage = result.message || 'Failed to delete all selected items';
                if (result.errors && result.errors.length > 0) {
                    errorMessage += ': ' + result.errors.join(', ');
                }
                showAlert('error', errorMessage);
            }
        } else {
            const error = await response.json();
            showAlert('error', `Failed to delete files: ${error.error || 'Unknown error'}`);
        }
    } catch (error) {
        console.error('Delete error:', error);
        showAlert('error', 'Failed to delete files');
    } finally {
        // Re-enable the button
        deleteBtn.disabled = false;
        deleteBtn.innerHTML = originalText;
    }
}

// Create new folder
function createFolder() {
    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('createFolderModal'));
    modal.show();
}

// Upload file
function uploadFile() {
    const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('uploadFileModal'));
    modal.show();
}

// Submit create folder form
async function submitCreateFolder() {
    const folderName = document.getElementById('folderName').value.trim();
    const currentPath = document.getElementById('currentPath').value;

    if (!folderName) {
        showErrorMessage('Please enter a folder name');
        return;
    }

    // Validate folder name
    if (folderName.includes('/') || folderName.includes('\\')) {
        showErrorMessage('Folder names cannot contain / or \\ characters');
        return;
    }

    // Additional validation for reserved names
    const reservedNames = ['.', '..', 'CON', 'PRN', 'AUX', 'NUL'];
    if (reservedNames.includes(folderName.toUpperCase())) {
        showErrorMessage('This folder name is reserved and cannot be used');
        return;
    }

    // Disable the button to prevent double submission
    const submitButton = document.querySelector('#createFolderModal .btn-primary');
    const originalText = submitButton.innerHTML;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Creating...';

    try {
        const response = await fetch('/api/files/create-folder', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                path: currentPath,
                folder_name: folderName
            })
        });

        if (response.ok) {
            showSuccessMessage(`Folder "${folderName}" created successfully`);
            // Hide modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('createFolderModal'));
            modal.hide();
            // Clear form
            document.getElementById('folderName').value = '';
            // Refresh page
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            const error = await response.json();
            showErrorMessage(`Failed to create folder: ${error.error || 'Unknown error'}`);
        }
    } catch (error) {
        console.error('Create folder error:', error);
        showErrorMessage('Failed to create folder. Please try again.');
    } finally {
        // Re-enable the button
        submitButton.disabled = false;
        submitButton.innerHTML = originalText;
    }
}

// Submit upload file form
async function submitUploadFile() {
    const fileInput = document.getElementById('fileInput');
    const currentPath = document.getElementById('uploadPath').value;

    if (!fileInput.files || fileInput.files.length === 0) {
        showErrorMessage('Please select at least one file to upload');
        return;
    }

    const files = Array.from(fileInput.files);
    const totalSize = files.reduce((sum, file) => sum + file.size, 0);

    // Validate total file size (limit to 500MB for admin interface)
    const maxSize = 500 * 1024 * 1024; // 500MB total
    if (totalSize > maxSize) {
        showErrorMessage('Total file size exceeds 500MB limit. Please select fewer or smaller files.');
        return;
    }

    // Individual file size validation removed - no limit per file

    const formData = new FormData();
    files.forEach(file => {
        formData.append('files', file);
    });
    formData.append('path', currentPath);

    // Show progress bar and disable button
    const progressContainer = document.getElementById('uploadProgress');
    const progressBar = progressContainer.querySelector('.progress-bar');
    const uploadStatus = document.getElementById('uploadStatus');
    const submitButton = document.querySelector('#uploadFileModal .btn-primary');
    const originalText = submitButton.innerHTML;

    progressContainer.style.display = 'block';
    progressBar.style.width = '0%';
    progressBar.setAttribute('aria-valuenow', '0');
    progressBar.textContent = '0%';
    uploadStatus.textContent = `Uploading ${files.length} file(s)...`;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Uploading...';

    try {
        const xhr = new XMLHttpRequest();

        // Handle progress
        xhr.upload.addEventListener('progress', function (e) {
            if (e.lengthComputable) {
                const percentComplete = Math.round((e.loaded / e.total) * 100);
                progressBar.style.width = percentComplete + '%';
                progressBar.setAttribute('aria-valuenow', percentComplete);
                progressBar.textContent = percentComplete + '%';
                uploadStatus.textContent = `Uploading ${files.length} file(s)... ${percentComplete}%`;
            }
        });

        // Handle completion
        xhr.addEventListener('load', function () {
            if (xhr.status === 200) {
                try {
                    const response = JSON.parse(xhr.responseText);

                    if (response.uploaded > 0) {
                        if (response.failed === 0) {
                            showSuccessMessage(`Successfully uploaded ${response.uploaded} file(s)`);
                        } else {
                            showSuccessMessage(response.message);
                            // Show details of failed uploads
                            if (response.errors && response.errors.length > 0) {
                                console.warn('Upload errors:', response.errors);
                            }
                        }

                        // Hide modal and refresh page
                        const modal = bootstrap.Modal.getInstance(document.getElementById('uploadFileModal'));
                        modal.hide();
                        setTimeout(() => {
                            window.location.reload();
                        }, 1000);
                    } else {
                        let errorMessage = response.message || 'All file uploads failed';
                        if (response.errors && response.errors.length > 0) {
                            errorMessage += ': ' + response.errors.join(', ');
                        }
                        showErrorMessage(errorMessage);
                    }
                } catch (e) {
                    showErrorMessage('Upload completed but response format was unexpected');
                }
                progressContainer.style.display = 'none';
            } else {
                let errorMessage = 'Unknown error';
                try {
                    const error = JSON.parse(xhr.responseText);
                    errorMessage = error.error || error.message || errorMessage;
                } catch (e) {
                    errorMessage = `Server returned status ${xhr.status}`;
                }
                showErrorMessage(`Failed to upload files: ${errorMessage}`);
                progressContainer.style.display = 'none';
            }
        });

        // Handle errors
        xhr.addEventListener('error', function () {
            showErrorMessage('Failed to upload files. Please check your connection and try again.');
            progressContainer.style.display = 'none';
        });

        // Handle abort
        xhr.addEventListener('abort', function () {
            showErrorMessage('File upload was cancelled.');
            progressContainer.style.display = 'none';
        });

        // Send request
        xhr.open('POST', '/api/files/upload');
        xhr.send(formData);

    } catch (error) {
        console.error('Upload error:', error);
        showErrorMessage('Failed to upload files. Please try again.');
        progressContainer.style.display = 'none';
    } finally {
        // Re-enable the button
        submitButton.disabled = false;
        submitButton.innerHTML = originalText;
    }
}

// Export file list to CSV
function exportFileList() {
    const table = document.getElementById('fileTable');
    if (!table) {
        showAlert('error', 'File table not found');
        return;
    }

    const headers = ['Name', 'Size', 'Type', 'Modified', 'Permissions'];
    const rows = [];

    // Get table rows
    const tableRows = table.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 6) {
            rows.push([
                cells[1].textContent.trim(), // Name
                cells[2].textContent.trim(), // Size
                cells[3].textContent.trim(), // Type
                cells[4].textContent.trim(), // Modified
                cells[5].textContent.trim()  // Permissions
            ]);
        }
    });

    // Generate CSV
    const csvContent = [headers, ...rows]
        .map(row => row.map(cell => `"${cell}"`).join(','))
        .join('\n');

    // Download
    const filename = `seaweedfs-files-${new Date().toISOString().split('T')[0]}.csv`;
    downloadCSV(csvContent, filename);
}

// Download file
function downloadFile(filePath) {
    // Create download link using admin API
    const downloadUrl = `/api/files/download?path=${encodeURIComponent(filePath)}`;
    window.open(downloadUrl, '_blank');
}

// View file
async function viewFile(filePath) {
    try {
        const response = await fetch(`/api/files/view?path=${encodeURIComponent(filePath)}&t=${new Date().getTime()}`);

        if (!response.ok) {
            const error = await response.json();
            showAlert('error', `Failed to view file: ${error.error || 'Unknown error'}`);
            return;
        }

        const data = await response.json();
        showFileViewer(data);

    } catch (error) {
        console.error('View file error:', error);
        showAlert('error', 'Failed to view file');
    }
}

// Show file properties
async function showProperties(filePath) {
    try {
        const response = await fetch(`/api/files/properties?path=${encodeURIComponent(filePath)}`);

        if (!response.ok) {
            const error = await response.json();
            showAlert('error', `Failed to get file properties: ${error.error || 'Unknown error'}`);
            return;
        }

        const properties = await response.json();
        showPropertiesModal(properties);

    } catch (error) {
        console.error('Properties error:', error);
        showAlert('error', 'Failed to get file properties');
    }
}

// Confirm delete file/folder
function confirmDelete(filePath) {
    if (confirm(`Are you sure you want to delete "${filePath}"?`)) {
        deleteFile(filePath);
    }
}

// Delete file/folder
async function deleteFile(filePath) {
    try {
        const response = await fetch('/api/files/delete', {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ path: filePath })
        });

        if (response.ok) {
            showAlert('success', `Successfully deleted "${filePath}"`);
            // Reload the page to update the file list
            window.location.reload();
        } else {
            const error = await response.json();
            showAlert('error', `Failed to delete file: ${error.error || 'Unknown error'}`);
        }
    } catch (error) {
        console.error('Delete error:', error);
        showAlert('error', 'Failed to delete file');
    }
}

// Setup file manager specific event handlers
function setupFileManagerEventHandlers() {
    // Handle Enter key in folder name input
    const folderNameInput = document.getElementById('folderName');
    if (folderNameInput) {
        folderNameInput.addEventListener('keypress', function (e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                submitCreateFolder();
            }
        });
    }

    // Handle file selection change to show preview
    const fileInput = document.getElementById('fileInput');
    if (fileInput) {
        fileInput.addEventListener('change', function (e) {
            updateFileListPreview();
        });
    }

    // Delegated event handling for file checkboxes and actions has been moved to initializeGlobalEventHandlers
    // to prevent duplicate listeners on page navigation/HTMX swaps.



    // Setup drag and drop for file uploads
    setupDragAndDrop();

    // Clear form when modals are hidden
    const createFolderModal = document.getElementById('createFolderModal');
    if (createFolderModal) {
        createFolderModal.addEventListener('hidden.bs.modal', function () {
            document.getElementById('folderName').value = '';
        });
    }

    const uploadFileModal = document.getElementById('uploadFileModal');
    if (uploadFileModal) {
        uploadFileModal.addEventListener('hidden.bs.modal', function () {
            const fileInput = document.getElementById('fileInput');
            const progressContainer = document.getElementById('uploadProgress');
            const fileListPreview = document.getElementById('fileListPreview');
            fileInput.value = '';
            progressContainer.style.display = 'none';
            fileListPreview.style.display = 'none';
        });
    }
}

// Setup drag and drop functionality
function setupDragAndDrop() {
    const dropZone = document.querySelector('.card-body'); // Main file listing area
    const uploadModal = document.getElementById('uploadFileModal');

    if (!dropZone || !uploadModal) return;

    // Prevent default drag behaviors on drop zone
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, preventDefaults, false);
        // document.body listeners moved to initializeGlobalEventHandlers
    });

    // Highlight drop zone when item is dragged over it
    ['dragenter', 'dragover'].forEach(eventName => {
        dropZone.addEventListener(eventName, highlight, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, unhighlight, false);
    });

    // Handle dropped files
    dropZone.addEventListener('drop', handleDrop, false);

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    function highlight(e) {
        dropZone.classList.add('drag-over');
        // Add some visual feedback
        if (!dropZone.querySelector('.drag-overlay')) {
            const overlay = document.createElement('div');
            overlay.className = 'drag-overlay';
            overlay.innerHTML = `
                <div class="text-center p-5">
                    <i class="fas fa-cloud-upload-alt fa-3x text-primary mb-3"></i>
                    <h5>Drop files here to upload</h5>
                    <p class="text-muted">Release to upload files to this directory</p>
                </div>
            `;
            overlay.style.cssText = `
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: rgba(255, 255, 255, 0.9);
                border: 2px dashed #007bff;
                border-radius: 0.375rem;
                z-index: 1000;
                display: flex;
                align-items: center;
                justify-content: center;
            `;
            dropZone.style.position = 'relative';
            dropZone.appendChild(overlay);
        }
    }

    function unhighlight(e) {
        dropZone.classList.remove('drag-over');
        const overlay = dropZone.querySelector('.drag-overlay');
        if (overlay) {
            overlay.remove();
        }
    }

    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;

        if (files.length > 0) {
            // Open upload modal and set files
            const fileInput = document.getElementById('fileInput');
            if (fileInput) {
                // Create a new FileList-like object
                const fileArray = Array.from(files);

                // Set files to input (this is a bit tricky with file inputs)
                const dataTransfer = new DataTransfer();
                fileArray.forEach(file => dataTransfer.items.add(file));
                fileInput.files = dataTransfer.files;

                // Update preview and show modal
                updateFileListPreview();
                const modal = bootstrap.Modal.getOrCreateInstance(uploadModal);
                modal.show();
            }
        }
    }
}

// Update file list preview when files are selected
function updateFileListPreview() {
    const fileInput = document.getElementById('fileInput');
    const fileListPreview = document.getElementById('fileListPreview');
    const selectedFilesList = document.getElementById('selectedFilesList');

    if (!fileInput.files || fileInput.files.length === 0) {
        fileListPreview.style.display = 'none';
        return;
    }

    const files = Array.from(fileInput.files);
    const totalSize = files.reduce((sum, file) => sum + file.size, 0);

    let html = `<div class="d-flex justify-content-between align-items-center mb-2">
        <strong>${files.length} file(s) selected</strong>
        <small class="text-muted">Total: ${formatBytes(totalSize)}</small>
    </div>`;

    files.forEach((file, index) => {
        const fileIcon = getFileIconByName(file.name);
        html += `<div class="d-flex justify-content-between align-items-center py-1 ${index > 0 ? 'border-top' : ''}">
            <div class="d-flex align-items-center">
                <i class="fas ${fileIcon} me-2 text-muted"></i>
                <span class="text-truncate" style="max-width: 200px;" title="${file.name}">${file.name}</span>
            </div>
            <small class="text-muted">${formatBytes(file.size)}</small>
        </div>`;
    });

    selectedFilesList.innerHTML = html;
    fileListPreview.style.display = 'block';
}

// Get file icon based on file name/extension
function getFileIconByName(fileName) {
    const ext = fileName.split('.').pop().toLowerCase();

    switch (ext) {
        case 'jpg':
        case 'jpeg':
        case 'png':
        case 'gif':
        case 'bmp':
        case 'svg':
            return 'fa-image';
        case 'mp4':
        case 'avi':
        case 'mov':
        case 'wmv':
        case 'flv':
            return 'fa-video';
        case 'mp3':
        case 'wav':
        case 'flac':
        case 'aac':
            return 'fa-music';
        case 'pdf':
            return 'fa-file-pdf';
        case 'doc':
        case 'docx':
            return 'fa-file-word';
        case 'xls':
        case 'xlsx':
            return 'fa-file-excel';
        case 'ppt':
        case 'pptx':
            return 'fa-file-powerpoint';
        case 'txt':
        case 'md':
            return 'fa-file-text';
        case 'zip':
        case 'rar':
        case '7z':
        case 'tar':
        case 'gz':
            return 'fa-file-archive';
        case 'js':
        case 'ts':
        case 'html':
        case 'css':
        case 'json':
        case 'xml':
            return 'fa-file-code';
        default:
            return 'fa-file';
    }
}

// Quota Management Functions

// Show quota management modal


// Convert bytes to the best unit (TB, GB, or MB)
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

// Handle quota update form submission


// Show file viewer modal
function showFileViewer(data) {
    const file = data.file;
    const content = data.content || '';
    const viewable = data.viewable !== false;

    let modalEl = document.getElementById('fileViewerModal');
    let modal;

    if (!modalEl) {
        // Create modal structure if it doesn't exist
        const modalHtml = `
        <div class="modal fade" id="fileViewerModal" tabindex="-1" aria-labelledby="fileViewerModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-xl">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="fileViewerModalLabel"></h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body" id="fileViewerModalBody"></div>
                    <div class="modal-footer" id="fileViewerModalFooter"></div>
                </div>
            </div>
        </div>`;
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        modalEl = document.getElementById('fileViewerModal');
        modal = new bootstrap.Modal(modalEl);
    } else {
        modal = bootstrap.Modal.getOrCreateInstance(modalEl);
    }

    // Update content
    document.getElementById('fileViewerModalLabel').innerHTML = `<i class="fas fa-eye me-2"></i>File Viewer: ${file.name}`;

    const bodyContent = viewable ? createFileViewerContent(file, content, data.parquet_data) : createNonViewableContent(data.reason || 'File cannot be viewed');
    document.getElementById('fileViewerModalBody').innerHTML = bodyContent;

    document.getElementById('fileViewerModalFooter').innerHTML = `
        <button type="button" class="btn btn-primary" onclick="downloadFile('${file.full_path}')">
            <i class="fas fa-download me-1"></i>Download
        </button>
        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
    `;

    modal.show();
}

// Create file viewer content based on file type
function createFileViewerContent(file, content, parquetData) {
    if (file.mime === 'application/vnd.apache.parquet' || file.name.toLowerCase().endsWith('.parquet')) {
        // If parquetData is missing/empty but we have an error message in content, show it
        if ((!parquetData || !parquetData.rows) && content && content.startsWith("Error")) {
            return `
            <div class="mb-3">
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    ${escapeHtml(content)}
                </div>
            </div>`;
        }
        return createParquetViewerContent(parquetData);
    } else if (file.mime.startsWith('image/')) {
        return `
            <div class="text-center">
                <img src="/api/files/download?path=${encodeURIComponent(file.full_path)}" 
                     class="img-fluid" alt="${file.name}" style="max-height: 500px;">
            </div>
        `;
    } else if (file.mime.startsWith('text/') || file.mime === 'application/json' || file.mime === 'application/javascript') {
        const language = getLanguageFromMime(file.mime, file.name);
        return `
            <div class="mb-3">
                <small class="text-muted">
                    <i class="fas fa-info-circle me-1"></i>
                    Size: ${formatBytes(file.size)} | Type: ${file.mime}
                </small>
            </div>
            <pre><code class="language-${language}" style="max-height: 400px; overflow-y: auto;">${escapeHtml(content)}</code></pre>
        `;
    } else if (file.mime === 'application/pdf') {
        return `
            <div class="text-center">
                <embed src="/api/files/download?path=${encodeURIComponent(file.full_path)}" 
                       type="application/pdf" width="100%" height="500px">
            </div>
        `;
    } else {
        return createNonViewableContent('This file type cannot be previewed in the browser.');
    }
}

// Create Parquet viewer content
function createParquetViewerContent(parquetData) {
    if (!parquetData || !parquetData.rows || parquetData.rows.length === 0) {
        return `
            <div class="text-center py-5">
                <i class="fas fa-table fa-3x text-muted mb-3"></i>
                <h5 class="text-muted">No data found</h5>
                <p class="text-muted">The Parquet file appears to be empty or could not be parsed.</p>
            </div>
        `;
    }

    // Headers
    let html = '<div class="table-responsive"><table class="table table-striped table-hover table-sm">';
    html += '<thead class="table-light"><tr>';

    // Use schema if available, otherwise get keys from first row
    const columns = parquetData.schema || (parquetData.rows.length > 0 ? Object.keys(parquetData.rows[0]) : []);

    columns.forEach(col => {
        html += `<th>${escapeHtml(col)}</th>`;
    });
    html += '</tr></thead><tbody>';

    // Rows
    parquetData.rows.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            let val = row[col];
            if (val === null || val === undefined) val = '';
            else if (typeof val === 'object') val = JSON.stringify(val);
            html += `<td>${escapeHtml(String(val))}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table></div>';

    if (parquetData.total) {
        html += `<div class="mt-2"><small class="text-muted"><i class="fas fa-info-circle me-1"></i>Showing ${parquetData.rows.length} of ${parquetData.total} rows</small></div>`;
    }

    return html;
}

// Create non-viewable content message
function createNonViewableContent(reason) {
    return `
        <div class="text-center py-5">
            <i class="fas fa-file fa-3x text-muted mb-3"></i>
            <h5 class="text-muted">Cannot preview file</h5>
            <p class="text-muted">${reason}</p>
        </div>
    `;
}

// Get language for syntax highlighting
function getLanguageFromMime(mime, filename) {
    // First check MIME type
    switch (mime) {
        case 'application/json': return 'json';
        case 'application/javascript': return 'javascript';
        case 'text/html': return 'html';
        case 'text/css': return 'css';
        case 'application/xml': return 'xml';
        case 'text/typescript': return 'typescript';
        case 'text/x-python': return 'python';
        case 'text/x-go': return 'go';
        case 'text/x-java': return 'java';
        case 'text/x-c': return 'c';
        case 'text/x-c++': return 'cpp';
        case 'text/x-c-header': return 'c';
        case 'text/x-shellscript': return 'bash';
        case 'text/x-php': return 'php';
        case 'text/x-ruby': return 'ruby';
        case 'text/x-perl': return 'perl';
        case 'text/x-rust': return 'rust';
        case 'text/x-swift': return 'swift';
        case 'text/x-kotlin': return 'kotlin';
        case 'text/x-scala': return 'scala';
        case 'text/x-dockerfile': return 'dockerfile';
        case 'text/yaml': return 'yaml';
        case 'text/csv': return 'csv';
        case 'text/sql': return 'sql';
        case 'text/markdown': return 'markdown';
    }

    // Fallback to file extension
    const ext = filename.split('.').pop().toLowerCase();
    switch (ext) {
        case 'js': case 'mjs': return 'javascript';
        case 'ts': return 'typescript';
        case 'py': return 'python';
        case 'go': return 'go';
        case 'java': return 'java';
        case 'cpp': case 'cc': case 'cxx': case 'c++': return 'cpp';
        case 'c': return 'c';
        case 'h': case 'hpp': return 'c';
        case 'sh': case 'bash': case 'zsh': case 'fish': return 'bash';
        case 'php': return 'php';
        case 'rb': return 'ruby';
        case 'pl': return 'perl';
        case 'rs': return 'rust';
        case 'swift': return 'swift';
        case 'kt': return 'kotlin';
        case 'scala': return 'scala';
        case 'yml': case 'yaml': return 'yaml';
        case 'md': case 'markdown': return 'markdown';
        case 'sql': return 'sql';
        case 'csv': return 'csv';
        case 'dockerfile': return 'dockerfile';
        case 'gitignore': case 'gitattributes': return 'text';
        case 'env': return 'bash';
        case 'cfg': case 'conf': case 'ini': case 'properties': return 'ini';
        default: return 'text';
    }
}

// Show properties modal
function showPropertiesModal(properties) {
    let modalEl = document.getElementById('propertiesModal');
    let modal;

    if (!modalEl) {
        const modalHtml = `
        <div class="modal fade" id="propertiesModal" tabindex="-1" aria-labelledby="propertiesModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="propertiesModalLabel"></h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body" id="propertiesModalBody"></div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>`;
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        modalEl = document.getElementById('propertiesModal');
        modal = new bootstrap.Modal(modalEl);
    } else {
        modal = bootstrap.Modal.getOrCreateInstance(modalEl);
    }

    document.getElementById('propertiesModalLabel').innerHTML = `<i class="fas fa-info-circle me-2"></i>Properties: ${properties.name}`;
    document.getElementById('propertiesModalBody').innerHTML = createPropertiesContent(properties);

    modal.show();
}

// Create properties content
function createPropertiesContent(properties) {
    let html = `
        <div class="row">
            <div class="col-md-6">
                <h6 class="text-primary"><i class="fas fa-file me-1"></i>Basic Information</h6>
                <table class="table table-sm">
                    <tr><td><strong>Name:</strong></td><td>${properties.name}</td></tr>
                    <tr><td><strong>Full Path:</strong></td><td><code>${properties.full_path}</code></td></tr>
                    <tr><td><strong>Type:</strong></td><td>${properties.is_directory ? 'Directory' : 'File'}</td></tr>
    `;

    if (!properties.is_directory) {
        html += `
                    <tr><td><strong>Size:</strong></td><td>${properties.size_formatted || formatBytes(properties.size || 0)}</td></tr>
                    <tr><td><strong>MIME Type:</strong></td><td>${properties.mime_type || 'Unknown'}</td></tr>
        `;
    }

    html += `
                </table>
            </div>
            <div class="col-md-6">
                <h6 class="text-primary"><i class="fas fa-clock me-1"></i>Timestamps</h6>
                <table class="table table-sm">
    `;

    if (properties.modified_time) {
        html += `<tr><td><strong>Modified:</strong></td><td>${properties.modified_time}</td></tr>`;
    }
    if (properties.created_time) {
        html += `<tr><td><strong>Created:</strong></td><td>${properties.created_time}</td></tr>`;
    }

    html += `
                </table>
                
                <h6 class="text-primary"><i class="fas fa-shield-alt me-1"></i>Permissions</h6>
                <table class="table table-sm">
                    <tr><td><strong>Mode:</strong></td><td><code>${properties.file_mode_formatted || properties.file_mode}</code></td></tr>
                    <tr><td><strong>UID:</strong></td><td>${properties.uid || 'N/A'}</td></tr>
                    <tr><td><strong>GID:</strong></td><td>${properties.gid || 'N/A'}</td></tr>
                </table>
            </div>
        </div>
    `;

    // Add TTL information if available
    if (properties.ttl_seconds && properties.ttl_seconds > 0) {
        html += `
            <div class="row mt-3">
                <div class="col-12">
                    <h6 class="text-primary"><i class="fas fa-hourglass-half me-1"></i>TTL (Time To Live)</h6>
                    <table class="table table-sm">
                        <tr><td><strong>TTL:</strong></td><td>${properties.ttl_formatted || properties.ttl_seconds + ' seconds'}</td></tr>
                    </table>
                </div>
            </div>
        `;
    }

    // Add chunk information if available
    if (properties.chunks && properties.chunks.length > 0) {
        html += `
            <div class="row mt-3">
                <div class="col-12">
                    <h6 class="text-primary"><i class="fas fa-puzzle-piece me-1"></i>Chunks (${properties.chunk_count})</h6>
                    <div class="table-responsive" style="max-height: 200px; overflow-y: auto;">
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th>File ID</th>
                                    <th>Offset</th>
                                    <th>Size</th>
                                    <th>ETag</th>
                                </tr>
                            </thead>
                            <tbody>
        `;

        properties.chunks.forEach(chunk => {
            html += `
                                <tr>
                                    <td><code class="small">${chunk.file_id}</code></td>
                                    <td>${formatBytes(chunk.offset)}</td>
                                    <td>${formatBytes(chunk.size)}</td>
                                    <td><code class="small">${chunk.e_tag || 'N/A'}</code></td>
                                </tr>
            `;
        });

        html += `
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }

    // Add extended attributes if available
    if (properties.extended && Object.keys(properties.extended).length > 0) {
        html += `
            <div class="row mt-3">
                <div class="col-12">
                    <h6 class="text-primary"><i class="fas fa-tags me-1"></i>Extended Attributes</h6>
                    <table class="table table-sm">
        `;

        Object.entries(properties.extended).forEach(([key, value]) => {
            html += `<tr><td><strong>${key}:</strong></td><td>${value}</td></tr>`;
        });

        html += `
                    </table>
                </div>
            </div>
        `;
    }

    return html;
}

