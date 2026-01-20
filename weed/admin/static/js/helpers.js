// SeaweedFS Dashboard Utilities
// Shared helper functions

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

// Format date for display
function formatDate(date) {
    return new Date(date).toLocaleString();
}

// Helper function to format disk types for CSV export
function formatDiskTypes(diskTypesText) {
    // Remove any HTML tags and clean up the text
    return diskTypesText.replace(/<[^>]*>/g, '').replace(/\s+/g, ' ').trim();
}

// Utility functions for Toasts
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
    toast.addEventListener('hidden.bs.toast', function () {
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

    toast.addEventListener('hidden.bs.toast', function () {
        toast.remove();
    });
}

// Show alert message (Floating Alert style, sometimes used differently than Toast)
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

// Copy text to clipboard
function copyToClipboard(text, triggerElement) {
    // Strategy 1: If we have a trigger element with a sibling input containing the text, usage that input directly.
    if (triggerElement && triggerElement.previousElementSibling) {
        const sibling = triggerElement.previousElementSibling;
        if ((sibling.tagName === 'INPUT' || sibling.tagName === 'TEXTAREA') && sibling.value === text) {
            sibling.focus();
            sibling.select();
            try {
                var successful = document.execCommand('copy');
                if (successful) {
                    const shortText = text.length > 20 ? text.substring(0, 20) + '...' : text;
                    showAlert('success', `Copied "${shortText}" to clipboard!`);
                    return;
                }
            } catch (err) {
                console.warn('Direct input copy failed, falling back...');
            }
        }
    }

    // Strategy 2: Modern API (if secure context)
    if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(() => {
            showAlert('success', 'Copied to clipboard!');
        }).catch(err => {
            fallbackCopyTextToClipboard(text);
        });
    } else {
        // Strategy 3: Fallback textarea
        fallbackCopyTextToClipboard(text);
    }
}

function fallbackCopyTextToClipboard(text) {
    if (document.activeElement instanceof HTMLElement) {
        document.activeElement.blur();
    }

    var textArea = document.createElement("textarea");
    textArea.value = text;

    // Ensure element is visible but unobtrusive for execCommand
    textArea.style.position = "fixed";
    textArea.style.left = "-9999px";
    textArea.style.top = "0";

    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    try {
        var successful = document.execCommand('copy');
        if (successful) {
            showAlert('success', 'Copied to clipboard!');
        } else {
            showAlert('danger', 'Failed to copy to clipboard.');
        }
    } catch (err) {
        console.error('Fallback: Oops, unable to copy', err);
        showAlert('danger', 'Failed to copy to clipboard.');
    }

    document.body.removeChild(textArea);
}

// Confirm action dialogs
function confirmAction(message, callback) {
    if (confirm(message)) {
        callback();
    }
}

// Loading indicator functions
function showLoadingIndicator() {
    const indicator = document.getElementById('loading-indicator');
    if (indicator) {
        indicator.style.display = 'block';
    }
    document.body.classList.add('loading');
}

function hideLoadingIndicator() {
    const indicator = document.getElementById('loading-indicator');
    if (indicator) {
        indicator.style.display = 'none';
    }
    document.body.classList.remove('loading');
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
        window.URL.revokeObjectURL(url);
    }
}

// Export functions for global use
window.Dashboard = {
    showErrorMessage,
    showSuccessMessage,
    showAlert,
    formatBytes,
    formatNumber,
    formatDate,
    formatDiskTypes,
    confirmAction,
    copyToClipboard,
    validateIAMPolicy: null, // Will be assigned by iam.js if loaded
    handleHTMXError, // exposed for admin.js
    escapeHtml,
    showLoadingIndicator,
    hideLoadingIndicator,
    downloadCSV,
    formatPermissions
};

// Utility function to escape HTML
function escapeHtml(text) {
    if (typeof text !== 'string') return text;
    var map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, function (m) { return map[m]; });
}






// Handle HTMX errors (moved from admin.js)
function handleHTMXError(evt) {
    console.warn('HTMX Request Error:', evt.detail);

    let message = 'Request failed. Please check your connection and try again.';

    if (evt.detail.xhr) {
        const xhr = evt.detail.xhr;
        const status = xhr.status;

        // Try to extract a specific error message from JSON response
        try {
            if (xhr.responseText && xhr.getResponseHeader('Content-Type') && xhr.getResponseHeader('Content-Type').includes('application/json')) {
                const response = JSON.parse(xhr.responseText);
                if (response.error) message = response.error;
                else if (response.message) message = response.message;
            } else if (xhr.statusText) {
                message = `Request failed: ${status} ${xhr.statusText}`;
            }
        } catch (e) {
            // If JSON parsing fails, fall back to status
            if (xhr.statusText) message = `Request failed: ${status} ${xhr.statusText}`;
        }
    } else if (evt.detail.error) {
        // Network errors etc
        message = evt.detail.error;
    }

    // Show error toast
    showErrorMessage(message);

    // If loading indicator function is available globally or we can find the element manually
    const indicator = document.getElementById('loading-indicator');
    if (indicator) {
        indicator.style.display = 'none';
    }
    document.body.classList.remove('loading');
}

// Helper function to format permissions in rwxrwxrwx format
function formatPermissions(mode, isDirectory) {
    // Check if mode is already in rwxrwxrwx format (e.g., "drwxr-xr-x" or "-rw-r--r--")
    if (mode && (mode.startsWith('d') || mode.startsWith('-') || mode.startsWith('l')) && mode.length === 10) {
        return mode; // Already formatted
    }

    // Convert to number - could be octal string or decimal
    let permissions;
    if (typeof mode === 'string') {
        // Try parsing as octal first, then decimal
        if (mode.startsWith('0') && mode.length <= 4) {
            permissions = parseInt(mode, 8);
        } else {
            permissions = parseInt(mode, 10);
        }
    } else {
        permissions = parseInt(mode, 10);
    }

    if (isNaN(permissions)) {
        return isDirectory ? 'drwxr-xr-x' : '-rw-r--r--'; // Default fallback
    }

    // Handle Go's os.ModeDir conversion
    // Go's os.ModeDir is 0x80000000 (2147483648), but Unix S_IFDIR is 0o40000 (16384)
    let fileType = '-';

    // Check for Go's os.ModeDir flag
    if (permissions & 0x80000000) {
        fileType = 'd';
    }
    // Check for standard Unix file type bits
    else if ((permissions & 0xF000) === 0x4000) { // S_IFDIR (0o40000)
        fileType = 'd';
    } else if ((permissions & 0xF000) === 0x8000) { // S_IFREG (0o100000)
        fileType = '-';
    } else if ((permissions & 0xF000) === 0xA000) { // S_IFLNK (0o120000)
        fileType = 'l';
    } else if ((permissions & 0xF000) === 0x2000) { // S_IFCHR (0o020000)
        fileType = 'c';
    } else if ((permissions & 0xF000) === 0x6000) { // S_IFBLK (0o060000)
        fileType = 'b';
    } else if ((permissions & 0xF000) === 0x1000) { // S_IFIFO (0o010000)
        fileType = 'p';
    } else if ((permissions & 0xF000) === 0xC000) { // S_IFSOCK (0o140000)
        fileType = 's';
    }
    // Fallback to isDirectory parameter if file type detection fails
    else if (isDirectory) {
        fileType = 'd';
    }

    // Permission bits (always use the lower 12 bits for permissions)
    const owner = (permissions >> 6) & 7;
    const group = (permissions >> 3) & 7;
    const others = permissions & 7;

    // Convert number to rwx format
    function numToRwx(num) {
        const r = (num & 4) ? 'r' : '-';
        const w = (num & 2) ? 'w' : '-';
        const x = (num & 1) ? 'x' : '-';
        return r + w + x;
    }

    return fileType + numToRwx(owner) + numToRwx(group) + numToRwx(others);
}

