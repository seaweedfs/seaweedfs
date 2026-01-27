/**
 * Modal Alerts - Bootstrap Modal replacement for native alert() and confirm()
 * Fixes Chrome auto-dismiss issue with native dialogs
 * 
 * Usage:
 *   showAlert('Message', 'success');
 *   showConfirm('Delete this?', function() { });
 */

(function () {
    'use strict';

    // Create and inject modal HTML into page if not already present
    function ensureModalsExist() {
        if (document.getElementById('globalAlertModal')) {
            return; // Already exists
        }

        const modalsHTML = `
            <!-- Global Alert Modal -->
            <div class="modal fade" id="globalAlertModal" tabindex="-1" aria-labelledby="globalAlertModalLabel" aria-hidden="true">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header" id="globalAlertModalHeader">
                            <h5 class="modal-title" id="globalAlertModalLabel">
                                <i class="fas fa-info-circle me-2" id="globalAlertModalIcon"></i>
                                <span id="globalAlertModalTitle">Notice</span>
                            </h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body" id="globalAlertModalBody">
                            <!-- Message will be inserted here -->
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-primary" data-bs-dismiss="modal">OK</button>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Global Confirm Modal -->
            <div class="modal fade" id="globalConfirmModal" tabindex="-1" aria-labelledby="globalConfirmModalLabel" aria-hidden="true">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header bg-warning">
                            <h5 class="modal-title" id="globalConfirmModalLabel">
                                <i class="fas fa-question-circle me-2"></i><span id="globalConfirmModalTitleText">Confirm Action</span>
                            </h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body" id="globalConfirmModalBody">
                            <!-- Message will be inserted here -->
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal" id="globalConfirmCancelBtn">Cancel</button>
                            <button type="button" class="btn btn-primary" id="globalConfirmOkBtn">OK</button>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Global Delete Confirm Modal -->
            <div class="modal fade" id="globalDeleteModal" tabindex="-1" aria-labelledby="globalDeleteModalLabel" aria-hidden="true">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header bg-danger text-white">
                            <h5 class="modal-title" id="globalDeleteModalLabel">
                                <i class="fas fa-exclamation-triangle me-2"></i>Confirm Delete
                            </h5>
                            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <p class="mb-2" id="globalDeleteModalMessage">Are you sure you want to delete this item?</p>
                            <p class="mb-0"><strong id="globalDeleteModalItemName"></strong></p>
                            <p class="text-muted small mt-2 mb-0">This action cannot be undone.</p>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                            <button type="button" class="btn btn-danger" id="globalDeleteConfirmBtn">
                                <i class="fas fa-trash me-1"></i>Delete
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        // Inject modals at end of body
        document.body.insertAdjacentHTML('beforeend', modalsHTML);
    }

    /**
     * Show an alert message using Bootstrap modal
     * @param {string} message - The message to display
     * @param {string|object} typeOrOptions - Type ('success', 'error', 'warning', 'info') or options object
     * @param {string} title - Optional custom title
     */
    window.showAlert = function (message, typeOrOptions, title) {
        ensureModalsExist();

        let type = 'info';
        let isHtml = false;

        if (typeof typeOrOptions === 'object' && typeOrOptions !== null) {
            type = typeOrOptions.type || 'info';
            isHtml = typeOrOptions.isHtml || false;
            title = typeOrOptions.title || title;
        } else if (typeof typeOrOptions === 'string') {
            type = typeOrOptions;
        }

        const modal = document.getElementById('globalAlertModal');
        const header = document.getElementById('globalAlertModalHeader');
        const titleEl = document.getElementById('globalAlertModalTitle');
        const bodyEl = document.getElementById('globalAlertModalBody');
        const iconEl = document.getElementById('globalAlertModalIcon');

        // Configuration for different types
        const types = {
            'success': {
                title: 'Success',
                icon: 'fa-check-circle',
                headerClass: 'bg-success text-white',
                btnClose: 'btn-close-white'
            },
            'error': {
                title: 'Error',
                icon: 'fa-exclamation-triangle',
                headerClass: 'bg-danger text-white',
                btnClose: 'btn-close-white'
            },
            'warning': {
                title: 'Warning',
                icon: 'fa-exclamation-circle',
                headerClass: 'bg-warning text-dark',
                btnClose: ''
            },
            'info': {
                title: 'Notice',
                icon: 'fa-info-circle',
                headerClass: 'bg-info text-white',
                btnClose: 'btn-close-white'
            }
        };

        const config = types[type] || types['info'];

        // Update header styling
        header.className = 'modal-header ' + config.headerClass;
        const closeBtn = header.querySelector('.btn-close');
        closeBtn.className = 'btn-close ' + config.btnClose;

        // Update icon
        iconEl.className = 'fas ' + config.icon + ' me-2';

        // Update title
        titleEl.textContent = title || config.title;

        // Update body - support HTML or text
        if (isHtml || message.includes('<p>') || message.includes('<ul>')) {
            bodyEl.innerHTML = message;
        } else {
            bodyEl.innerHTML = '<p class="mb-0">' + escapeHtml(message) + '</p>';
        }

        // Show modal
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();
    };

    /**
     * Show a confirmation dialog using Bootstrap modal
     * @param {string} message - The confirmation message
     * @param {function} onConfirm - Callback function if user confirms
     * @param {function|object} onCancelOrOptions - Optional callback or options object
     * @param {string} title - Optional custom title
     */
    window.showConfirm = function (message, onConfirm, onCancelOrOptions, title) {
        ensureModalsExist();

        let onCancel = null;
        let isHtml = false;

        if (typeof onCancelOrOptions === 'object' && onCancelOrOptions !== null) {
            onCancel = onCancelOrOptions.onCancel;
            isHtml = onCancelOrOptions.isHtml || false;
            title = onCancelOrOptions.title || null;
        } else {
            onCancel = onCancelOrOptions;
        }

        const modalEl = document.getElementById('globalConfirmModal');
        const bodyEl = document.getElementById('globalConfirmModalBody');
        const titleEl = document.getElementById('globalConfirmModalTitleText');
        const okBtn = document.getElementById('globalConfirmOkBtn');
        const cancelBtn = document.getElementById('globalConfirmCancelBtn');

        // Set title
        if (title) {
            titleEl.textContent = title;
        } else {
            titleEl.textContent = 'Confirm Action';
        }

        // Set message
        if (isHtml || message.includes('<p>') || message.includes('<ul>')) {
            bodyEl.innerHTML = message;
        } else {
            bodyEl.innerHTML = '<p class="mb-0">' + escapeHtml(message) + '</p>';
        }

        // Remove old event listeners by cloning buttons
        const newOkBtn = okBtn.cloneNode(true);
        const newCancelBtn = cancelBtn.cloneNode(true);
        okBtn.parentNode.replaceChild(newOkBtn, okBtn);
        cancelBtn.parentNode.replaceChild(newCancelBtn, cancelBtn);

        const modal = new bootstrap.Modal(modalEl);

        // Add event listeners
        newOkBtn.addEventListener('click', function () {
            modal.hide();
            if (typeof onConfirm === 'function') {
                onConfirm();
            }
        });

        newCancelBtn.addEventListener('click', function () {
            modal.hide();
            if (typeof onCancel === 'function') {
                onCancel();
            }
        });

        modal.show();
    };

    /**
     * Show a delete confirmation dialog
     * @param {string} itemName - Name of the item to delete
     * @param {function} onConfirm - Callback function if user confirms deletion
     * @param {string} message - Optional custom message (default: "Are you sure you want to delete this item?")
     */
    window.showDeleteConfirm = function (itemName, onConfirm, message) {
        ensureModalsExist();

        const modalEl = document.getElementById('globalDeleteModal');
        const messageEl = document.getElementById('globalDeleteModalMessage');
        const itemNameEl = document.getElementById('globalDeleteModalItemName');
        const confirmBtn = document.getElementById('globalDeleteConfirmBtn');

        // Set custom message if provided
        if (message) {
            messageEl.textContent = message;
        } else {
            messageEl.textContent = 'Are you sure you want to delete this item?';
        }

        // Set item name
        itemNameEl.textContent = itemName;

        // Remove old event listener by cloning button
        const newConfirmBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);

        const modal = new bootstrap.Modal(modalEl);

        // Add new event listener
        newConfirmBtn.addEventListener('click', function () {
            modal.hide();
            if (typeof onConfirm === 'function') {
                onConfirm();
            }
        });

        modal.show();
    };

    /**
     * Escape HTML to prevent XSS
     */
    function escapeHtml(text) {
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, function (m) { return map[m]; });
    }

    // Auto-initialize on DOMContentLoaded
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', ensureModalsExist);
    } else {
        ensureModalsExist();
    }

    /**
     * AUTOMATIC OVERRIDE of native alert()
     * This makes ALL existing alert() calls automatically use Bootstrap modals
     */
    window.alert = function (message) {
        // Auto-detect message type from content
        let type = 'info';
        const msg = String(message || '');
        const msgLower = msg.toLowerCase();

        // Refined type inference to avoid false positives
        if (msgLower.includes('success') || msgLower.includes('successfully') || msgLower.includes('created') || msgLower.includes('updated') || msgLower.includes('saved')) {
            // Avoid "not successful"
            if (!msgLower.includes('not success')) {
                type = 'success';
            }
        }

        if (type === 'info') {
            if (msgLower.includes('error') || msgLower.includes('failed') || msgLower.includes('invalid') || msgLower.includes('exception')) {
                type = 'error';
            } else if (msgLower.includes('warning') || msgLower.includes('required') || msgLower.includes('attention')) {
                type = 'warning';
            }
        }

        showAlert(msg, type);
    };

    console.log('Modal Alerts library loaded - native alert() overridden');
    console.log('For confirm(), use showConfirm() or showDeleteConfirm() instead of native confirm()');
})();
