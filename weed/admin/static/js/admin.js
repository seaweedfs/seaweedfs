// SeaweedFS Dashboard JavaScript

// Global variables
let bucketToDelete = '';

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    initializeEventHandlers();
    setupFormValidation();
    setupFileManagerEventHandlers();
    
    // Initialize delete button visibility on file browser page
    if (window.location.pathname === '/files') {
        updateDeleteSelectedButton();
    }
});

function initializeDashboard() {
    // Set up HTMX event listeners
    setupHTMXListeners();
    
    // Initialize tooltips
    initializeTooltips();
    
    // Set up periodic refresh
    setupAutoRefresh();
    
    // Set active navigation
    setActiveNavigation();
    
    // Set up submenu behavior
    setupSubmenuBehavior();
}

// HTMX event listeners
function setupHTMXListeners() {
    // Show loading indicator on requests
    document.body.addEventListener('htmx:beforeRequest', function(evt) {
        showLoadingIndicator();
    });
    
    // Hide loading indicator on completion
    document.body.addEventListener('htmx:afterRequest', function(evt) {
        hideLoadingIndicator();
    });
    
    // Handle errors
    document.body.addEventListener('htmx:responseError', function(evt) {
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
    // Refresh dashboard data every 30 seconds
    setInterval(function() {
        if (window.location.pathname === '/dashboard') {
            htmx.trigger('#dashboard-content', 'refresh');
        }
    }, 30000);
}

// Set active navigation item
function setActiveNavigation() {
    const currentPath = window.location.pathname;
    const navLinks = document.querySelectorAll('.sidebar .nav-link');
    
    navLinks.forEach(function(link) {
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
    
    // If we're on a cluster page, expand the cluster submenu
    if (currentPath.startsWith('/cluster/')) {
        const clusterSubmenu = document.getElementById('clusterSubmenu');
        if (clusterSubmenu) {
            clusterSubmenu.classList.add('show');
            
            // Update the parent toggle button state
            const toggleButton = document.querySelector('[data-bs-target="#clusterSubmenu"]');
            if (toggleButton) {
                toggleButton.classList.remove('collapsed');
                toggleButton.setAttribute('aria-expanded', 'true');
            }
        }
    }
    
    // If we're on an object store page, expand the object store submenu
    if (currentPath.startsWith('/object-store/')) {
        const objectStoreSubmenu = document.getElementById('objectStoreSubmenu');
        if (objectStoreSubmenu) {
            objectStoreSubmenu.classList.add('show');
            
            // Update the parent toggle button state
            const toggleButton = document.querySelector('[data-bs-target="#objectStoreSubmenu"]');
            if (toggleButton) {
                toggleButton.classList.remove('collapsed');
                toggleButton.setAttribute('aria-expanded', 'true');
            }
        }
    }
    
    // Prevent submenu from collapsing when clicking on submenu items
    const clusterSubmenuLinks = document.querySelectorAll('#clusterSubmenu .nav-link');
    clusterSubmenuLinks.forEach(function(link) {
        link.addEventListener('click', function(e) {
            // Don't prevent the navigation, just stop the collapse behavior
            e.stopPropagation();
        });
    });
    
    const objectStoreSubmenuLinks = document.querySelectorAll('#objectStoreSubmenu .nav-link');
    objectStoreSubmenuLinks.forEach(function(link) {
        link.addEventListener('click', function(e) {
            // Don't prevent the navigation, just stop the collapse behavior
            e.stopPropagation();
        });
    });
    
    // Handle the main cluster toggle
    const clusterToggle = document.querySelector('[data-bs-target="#clusterSubmenu"]');
    if (clusterToggle) {
        clusterToggle.addEventListener('click', function(e) {
            e.preventDefault();
            
            const submenu = document.getElementById('clusterSubmenu');
            const isExpanded = submenu.classList.contains('show');
            
            if (isExpanded) {
                // Collapse
                submenu.classList.remove('show');
                this.classList.add('collapsed');
                this.setAttribute('aria-expanded', 'false');
            } else {
                // Expand
                submenu.classList.add('show');
                this.classList.remove('collapsed');
                this.setAttribute('aria-expanded', 'true');
            }
        });
    }
    
    // Handle the main object store toggle
    const objectStoreToggle = document.querySelector('[data-bs-target="#objectStoreSubmenu"]');
    if (objectStoreToggle) {
        objectStoreToggle.addEventListener('click', function(e) {
            e.preventDefault();
            
            const submenu = document.getElementById('objectStoreSubmenu');
            const isExpanded = submenu.classList.contains('show');
            
            if (isExpanded) {
                // Collapse
                submenu.classList.remove('show');
                this.classList.add('collapsed');
                this.setAttribute('aria-expanded', 'false');
            } else {
                // Expand
                submenu.classList.add('show');
                this.classList.remove('collapsed');
                this.setAttribute('aria-expanded', 'true');
            }
        });
    }
}

// Loading indicator functions
function showLoadingIndicator() {
    const indicator = document.getElementById('loading-indicator');
    if (indicator) {
        indicator.style.display = 'block';
    }
    
    // Add loading class to body
    document.body.classList.add('loading');
}

function hideLoadingIndicator() {
    const indicator = document.getElementById('loading-indicator');
    if (indicator) {
        indicator.style.display = 'none';
    }
    
    // Remove loading class from body
    document.body.classList.remove('loading');
}

// Handle HTMX errors
function handleHTMXError(evt) {
    console.error('HTMX Request Error:', evt.detail);
    
    // Show error toast or message
    showErrorMessage('Request failed. Please try again.');
    
    hideLoadingIndicator();
}

// Utility functions
function showErrorMessage(message) {
    // Create toast element
    const toast = document.createElement('div');
    toast.className = 'toast align-items-center text-white bg-danger border-0';
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');
    
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                <i class="fas fa-exclamation-triangle me-2"></i>
                ${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
        </div>
    `;
    
    // Add to toast container or create one
    let toastContainer = document.getElementById('toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toast-container';
        toastContainer.className = 'toast-container position-fixed top-0 end-0 p-3';
        toastContainer.style.zIndex = '1055';
        document.body.appendChild(toastContainer);
    }
    
    toastContainer.appendChild(toast);
    
    // Show toast
    const bsToast = new bootstrap.Toast(toast);
    bsToast.show();
    
    // Remove toast element after it's hidden
    toast.addEventListener('hidden.bs.toast', function() {
        toast.remove();
    });
}

function showSuccessMessage(message) {
    // Similar to showErrorMessage but with success styling
    const toast = document.createElement('div');
    toast.className = 'toast align-items-center text-white bg-success border-0';
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');
    
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                <i class="fas fa-check-circle me-2"></i>
                ${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
        </div>
    `;
    
    let toastContainer = document.getElementById('toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toast-container';
        toastContainer.className = 'toast-container position-fixed top-0 end-0 p-3';
        toastContainer.style.zIndex = '1055';
        document.body.appendChild(toastContainer);
    }
    
    toastContainer.appendChild(toast);
    
    const bsToast = new bootstrap.Toast(toast);
    bsToast.show();
    
    toast.addEventListener('hidden.bs.toast', function() {
        toast.remove();
    });
}

// Format bytes for display
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
    
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// Format numbers with commas
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Confirm action dialogs
function confirmAction(message, callback) {
    if (confirm(message)) {
        callback();
    }
}

// Global error handler
window.addEventListener('error', function(e) {
    console.error('Global error:', e.error);
    showErrorMessage('An unexpected error occurred.');
});

// Export functions for global use
window.Dashboard = {
    showErrorMessage,
    showSuccessMessage,
    formatBytes,
    formatNumber,
    confirmAction
}; 

// Initialize event handlers
function initializeEventHandlers() {
    // S3 Bucket Management
    const createBucketForm = document.getElementById('createBucketForm');
    if (createBucketForm) {
        createBucketForm.addEventListener('submit', handleCreateBucket);
    }

    // Delete bucket buttons
    document.addEventListener('click', function(e) {
        if (e.target.closest('.delete-bucket-btn')) {
            const button = e.target.closest('.delete-bucket-btn');
            const bucketName = button.getAttribute('data-bucket-name');
            confirmDeleteBucket(bucketName);
        }
    });
}

// Setup form validation
function setupFormValidation() {
    // Bucket name validation
    const bucketNameInput = document.getElementById('bucketName');
    if (bucketNameInput) {
        bucketNameInput.addEventListener('input', validateBucketName);
    }
}

// S3 Bucket Management Functions

// Handle create bucket form submission
async function handleCreateBucket(event) {
    event.preventDefault();
    
    const form = event.target;
    const formData = new FormData(form);
    const bucketData = {
        name: formData.get('name'),
        region: formData.get('region') || 'us-east-1'
    };

    try {
        const response = await fetch('/api/s3/buckets', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(bucketData)
        });

        const result = await response.json();

        if (response.ok) {
            // Success
            showAlert('success', `Bucket "${bucketData.name}" created successfully!`);
            
            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('createBucketModal'));
            modal.hide();
            
            // Reset form
            form.reset();
            
            // Refresh the page after a short delay
            setTimeout(() => {
                location.reload();
            }, 1500);
        } else {
            // Error
            showAlert('danger', result.error || 'Failed to create bucket');
        }
    } catch (error) {
        console.error('Error creating bucket:', error);
        showAlert('danger', 'Network error occurred while creating bucket');
    }
}

// Validate bucket name input
function validateBucketName(event) {
    const input = event.target;
    const value = input.value;
    const isValid = /^[a-z0-9.-]+$/.test(value) && value.length >= 3 && value.length <= 63;
    
    if (value.length > 0 && !isValid) {
        input.setCustomValidity('Bucket name must contain only lowercase letters, numbers, dots, and hyphens (3-63 characters)');
    } else {
        input.setCustomValidity('');
    }
}

// Confirm bucket deletion
function confirmDeleteBucket(bucketName) {
    bucketToDelete = bucketName;
    document.getElementById('deleteBucketName').textContent = bucketName;
    
    const modal = new bootstrap.Modal(document.getElementById('deleteBucketModal'));
    modal.show();
}

// Delete bucket
async function deleteBucket() {
    if (!bucketToDelete) {
        return;
    }

    try {
        const response = await fetch(`/api/s3/buckets/${bucketToDelete}`, {
            method: 'DELETE'
        });

        const result = await response.json();

        if (response.ok) {
            // Success
            showAlert('success', `Bucket "${bucketToDelete}" deleted successfully!`);
            
            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('deleteBucketModal'));
            modal.hide();
            
            // Refresh the page after a short delay
            setTimeout(() => {
                location.reload();
            }, 1500);
        } else {
            // Error
            showAlert('danger', result.error || 'Failed to delete bucket');
        }
    } catch (error) {
        console.error('Error deleting bucket:', error);
        showAlert('danger', 'Network error occurred while deleting bucket');
    }

    bucketToDelete = '';
}

// Refresh buckets list
function refreshBuckets() {
    location.reload();
}

// Export bucket list
function exportBucketList() {
    // Get table data
    const table = document.getElementById('bucketsTable');
    if (!table) return;

    const rows = Array.from(table.querySelectorAll('tbody tr'));
    const data = rows.map(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length < 5) return null; // Skip empty state row
        
        return {
            name: cells[0].textContent.trim(),
            created: cells[1].textContent.trim(),
            objects: cells[2].textContent.trim(),
            size: cells[3].textContent.trim(),
            status: cells[4].textContent.trim()
        };
    }).filter(item => item !== null);

    // Convert to CSV
    const csv = [
        ['Name', 'Created', 'Objects', 'Size', 'Status'].join(','),
        ...data.map(row => [
            row.name,
            row.created,
            row.objects,
            row.size,
            row.status
        ].join(','))
    ].join('\n');

    // Download CSV
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `seaweedfs-buckets-${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
}

// Show alert message
function showAlert(type, message) {
    // Remove existing alerts
    const existingAlerts = document.querySelectorAll('.alert-floating');
    existingAlerts.forEach(alert => alert.remove());

    // Create new alert
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show alert-floating`;
    alert.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        z-index: 9999;
        min-width: 300px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    `;
    
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;

    document.body.appendChild(alert);

    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (alert.parentNode) {
            alert.remove();
        }
    }, 5000);
}

// Format date for display
function formatDate(date) {
    return new Date(date).toLocaleString();
}

// Copy text to clipboard
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showAlert('success', 'Copied to clipboard!');
    }).catch(err => {
        console.error('Failed to copy text: ', err);
        showAlert('danger', 'Failed to copy to clipboard');
    });
}

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
    
    let csv = 'Server ID,Address,Data Center,Rack,Volumes,Capacity,Usage,Status\n';
    
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 8) {
            const rowData = [
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim(),
                cells[4].textContent.trim(),
                cells[5].textContent.trim(),
                cells[6].textContent.trim(),
                cells[7].textContent.trim()
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
    
    let csv = 'Volume ID,Server,Data Center,Rack,Collection,Size,File Count,Replication,Status\n';
    
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 9) {
            const rowData = [
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim(),
                cells[4].textContent.trim(),
                cells[5].textContent.trim(),
                cells[6].textContent.trim(),
                cells[7].textContent.trim(),
                cells[8].textContent.trim()
            ];
            csv += rowData.join(',') + '\n';
        }
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

    const headers = ['Collection Name', 'Data Center', 'Replication', 'Volume Count', 'TTL', 'Disk Type', 'Status'];
    const rows = [];

    // Get table rows
    const tableRows = table.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 7) {
            rows.push([
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim(),
                cells[4].textContent.trim(),
                cells[5].textContent.trim(),
                cells[6].textContent.trim()
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

    const headers = ['Address', 'Role', 'Suffrage', 'Status'];
    const rows = [];

    // Get table rows
    const tableRows = table.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 4) {
            rows.push([
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim()
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

    const headers = ['Address', 'Version', 'Data Center', 'Rack', 'Created At', 'Status'];
    const rows = [];

    // Get table rows
    const tableRows = table.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 6) {
            rows.push([
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim(),
                cells[4].textContent.trim(),
                cells[5].textContent.trim()
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

    const headers = ['Username', 'Email', 'Access Key', 'Status', 'Created', 'Last Login'];
    const rows = [];

    // Get table rows
    const tableRows = table.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 6) {
            rows.push([
                cells[0].textContent.trim(),
                cells[1].textContent.trim(),
                cells[2].textContent.trim(),
                cells[3].textContent.trim(),
                cells[4].textContent.trim(),
                cells[5].textContent.trim()
            ]);
        }
    });

    // Generate CSV
    const csvContent = [headers, ...rows]
        .map(row => row.map(cell => `"${cell}"`).join(','))
        .join('\n');

    // Download
    const filename = `seaweedfs-users-${new Date().toISOString().split('T')[0]}.csv`;
    downloadCSV(csvContent, filename);
}

// Confirm delete collection
function confirmDeleteCollection(button) {
    const collectionName = button.getAttribute('data-collection-name');
    document.getElementById('deleteCollectionName').textContent = collectionName;
    
    const modal = new bootstrap.Modal(document.getElementById('deleteCollectionModal'));
    modal.show();
    
    // Set up confirm button
    document.getElementById('confirmDeleteCollection').onclick = function() {
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

// Handle create collection form submission
document.addEventListener('DOMContentLoaded', function() {
    const createCollectionForm = document.getElementById('createCollectionForm');
    if (createCollectionForm) {
        createCollectionForm.addEventListener('submit', handleCreateCollection);
    }
});

async function handleCreateCollection(event) {
    event.preventDefault();
    
    const formData = new FormData(event.target);
    const collectionData = {
        name: formData.get('name'),
        replication: formData.get('replication'),
        ttl: formData.get('ttl'),
        diskType: formData.get('diskType')
    };
    
    try {
        const response = await fetch('/api/collections', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(collectionData)
        });
        
        if (response.ok) {
            showSuccessMessage(`Collection "${collectionData.name}" created successfully`);
            // Hide modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('createCollectionModal'));
            modal.hide();
            // Reset form
            event.target.reset();
            // Refresh page
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            const error = await response.json();
            showErrorMessage(`Failed to create collection: ${error.error || 'Unknown error'}`);
        }
    } catch (error) {
        console.error('Error creating collection:', error);
        showErrorMessage('Failed to create collection. Please try again.');
    }
}

// Download CSV utility function
function downloadCSV(csvContent, filename) {
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    
    if (link.download !== undefined) {
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', filename);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
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
    const modal = new bootstrap.Modal(document.getElementById('createFolderModal'));
    modal.show();
}

// Upload file
function uploadFile() {
    const modal = new bootstrap.Modal(document.getElementById('uploadFileModal'));
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
    
    // Validate individual file sizes
    const maxIndividualSize = 100 * 1024 * 1024; // 100MB per file
    const oversizedFiles = files.filter(file => file.size > maxIndividualSize);
    if (oversizedFiles.length > 0) {
        showErrorMessage(`Some files exceed 100MB limit: ${oversizedFiles.map(f => f.name).join(', ')}`);
        return;
    }
    
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
        xhr.upload.addEventListener('progress', function(e) {
            if (e.lengthComputable) {
                const percentComplete = Math.round((e.loaded / e.total) * 100);
                progressBar.style.width = percentComplete + '%';
                progressBar.setAttribute('aria-valuenow', percentComplete);
                progressBar.textContent = percentComplete + '%';
                uploadStatus.textContent = `Uploading ${files.length} file(s)... ${percentComplete}%`;
            }
        });
        
        // Handle completion
        xhr.addEventListener('load', function() {
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
        xhr.addEventListener('error', function() {
            showErrorMessage('Failed to upload files. Please check your connection and try again.');
            progressContainer.style.display = 'none';
        });
        
        // Handle abort
        xhr.addEventListener('abort', function() {
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
    // Create download link using filer direct access
    const downloadUrl = `/files/download?path=${encodeURIComponent(filePath)}`;
    window.open(downloadUrl, '_blank');
}

// View file
function viewFile(filePath) {
    // TODO: Implement file viewer functionality
    showAlert('info', `File viewer for ${filePath} will be implemented`);
}

// Show file properties
function showProperties(filePath) {
    // TODO: Implement file properties modal
    showAlert('info', `Properties for ${filePath} will be implemented`);
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
        folderNameInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                submitCreateFolder();
            }
        });
    }
    
    // Handle file selection change to show preview
    const fileInput = document.getElementById('fileInput');
    if (fileInput) {
        fileInput.addEventListener('change', function(e) {
            updateFileListPreview();
        });
    }
    
    // Setup checkbox event listeners for file selection
    const checkboxes = document.querySelectorAll('.file-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            updateDeleteSelectedButton();
            updateSelectAllCheckbox();
        });
    });
    
    // Setup drag and drop for file uploads
    setupDragAndDrop();
    
    // Clear form when modals are hidden
    const createFolderModal = document.getElementById('createFolderModal');
    if (createFolderModal) {
        createFolderModal.addEventListener('hidden.bs.modal', function() {
            document.getElementById('folderName').value = '';
        });
    }
    
    const uploadFileModal = document.getElementById('uploadFileModal');
    if (uploadFileModal) {
        uploadFileModal.addEventListener('hidden.bs.modal', function() {
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
    
    // Prevent default drag behaviors
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, preventDefaults, false);
        document.body.addEventListener(eventName, preventDefaults, false);
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
                const modal = new bootstrap.Modal(uploadModal);
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

 