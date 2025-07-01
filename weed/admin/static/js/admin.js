// SeaweedFS Dashboard JavaScript

// Global variables
let bucketToDelete = '';

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    initializeEventHandlers();
    setupFormValidation();
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
        } else if (currentPath.startsWith('/cluster/') && href.startsWith('/cluster/')) {
            isActive = true;
        }
        
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
    
    // Prevent submenu from collapsing when clicking on submenu items
    const submenuLinks = document.querySelectorAll('#clusterSubmenu .nav-link');
    submenuLinks.forEach(function(link) {
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
        if (cells.length < 6) return null; // Skip empty state row
        
        return {
            name: cells[0].textContent.trim(),
            created: cells[1].textContent.trim(),
            objects: cells[2].textContent.trim(),
            size: cells[3].textContent.trim(),
            region: cells[4].textContent.trim(),
            status: cells[5].textContent.trim()
        };
    }).filter(item => item !== null);

    // Convert to CSV
    const csv = [
        ['Name', 'Created', 'Objects', 'Size', 'Region', 'Status'].join(','),
        ...data.map(row => [
            row.name,
            row.created,
            row.objects,
            row.size,
            row.region,
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

// Export hosts data as CSV
function exportHosts() {
    const table = document.getElementById('hostsTable');
    if (!table) {
        showErrorMessage('No hosts data to export');
        return;
    }
    
    let csv = 'Host ID,Address,Data Center,Rack,Volumes,Capacity,Usage,Status\n';
    
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
    
    downloadCSV(csv, 'seaweedfs-hosts.csv');
}

// Export volumes data as CSV
function exportVolumes() {
    const table = document.getElementById('volumesTable');
    if (!table) {
        showErrorMessage('No volumes data to export');
        return;
    }
    
    let csv = 'Volume ID,Server,Data Center,Rack,Size,File Count,Replication,Status\n';
    
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
    
    downloadCSV(csv, 'seaweedfs-volumes.csv');
}

// Export collections data as CSV
function exportCollections() {
    const table = document.getElementById('collectionsTable');
    if (!table) {
        showErrorMessage('No collections data to export');
        return;
    }
    
    let csv = 'Collection Name,Data Center,Replication,Volumes,TTL,Disk Type,Status\n';
    
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
    
    downloadCSV(csv, 'seaweedfs-collections.csv');
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

// Helper function to download CSV
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
    } else {
        // Fallback for browsers that don't support download attribute
        window.open('data:text/csv;charset=utf-8,' + encodeURIComponent(csvContent));
    }
} 