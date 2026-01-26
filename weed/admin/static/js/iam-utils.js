/**
 * Shared IAM utility functions for the SeaweedFS Admin Dashboard.
 */

// Delete user function
async function deleteUser(username) {
    showDeleteConfirm(username, async function () {
        try {
            const encodedUsername = encodeURIComponent(username);
            const response = await fetch(`/api/users/${encodedUsername}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                showAlert('User deleted successfully', 'success');
                setTimeout(() => window.location.reload(), 1000);
            } else {
                const error = await response.json().catch(() => ({}));
                showAlert('Failed to delete user: ' + (error.error || 'Unknown error'), 'error');
            }
        } catch (error) {
            console.error('Error deleting user:', error);
            showAlert('Failed to delete user: ' + error.message, 'error');
        }
    }, 'Are you sure you want to delete this user? This action cannot be undone.');
}

// Delete access key function
async function deleteAccessKey(username, accessKey) {
    showDeleteConfirm(accessKey, async function () {
        try {
            const encodedUsername = encodeURIComponent(username);
            const encodedAccessKey = encodeURIComponent(accessKey);
            const response = await fetch(`/api/users/${encodedUsername}/access-keys/${encodedAccessKey}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                showAlert('Access key deleted successfully', 'success');
                // If refreshAccessKeysList exists (in object_store_users.templ), use it
                if (typeof refreshAccessKeysList === 'function') {
                    refreshAccessKeysList(username);
                } else {
                    setTimeout(() => window.location.reload(), 1000);
                }
            } else {
                const error = await response.json().catch(() => ({}));
                showAlert('Failed to delete access key: ' + (error.error || 'Unknown error'), 'error');
            }
        } catch (error) {
            console.error('Error deleting access key:', error);
            showAlert('Failed to delete access key: ' + error.message, 'error');
        }
    }, 'Are you sure you want to delete this access key?');
}
