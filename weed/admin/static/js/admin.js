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
        
        if (isActive) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });
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