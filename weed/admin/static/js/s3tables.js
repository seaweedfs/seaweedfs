/**
 * Shared S3 Tables functionality for the SeaweedFS Admin Dashboard.
 */

// Shared Modals
let s3tablesBucketDeleteModal = null;
let s3tablesBucketPolicyModal = null;
let s3tablesNamespaceDeleteModal = null;
let s3tablesTableDeleteModal = null;
let s3tablesTablePolicyModal = null;
let s3tablesTagsModal = null;

/**
 * Initialize S3 Tables Buckets Page
 */
function initS3TablesBuckets() {
    s3tablesBucketDeleteModal = new bootstrap.Modal(document.getElementById('deleteS3TablesBucketModal'));
    s3tablesBucketPolicyModal = new bootstrap.Modal(document.getElementById('s3tablesBucketPolicyModal'));
    s3tablesTagsModal = new bootstrap.Modal(document.getElementById('s3tablesTagsModal'));

    const ownerSelect = document.getElementById('s3tablesBucketOwner');
    if (ownerSelect) {
        document.getElementById('createS3TablesBucketModal').addEventListener('show.bs.modal', async function () {
            if (ownerSelect.options.length <= 1) {
                try {
                    const response = await fetch('/api/users');
                    const data = await response.json();
                    const users = data.users || [];
                    users.forEach(user => {
                        const option = document.createElement('option');
                        option.value = user.username;
                        option.textContent = user.username;
                        ownerSelect.appendChild(option);
                    });
                } catch (error) {
                    console.error('Error fetching users for owner dropdown:', error);
                    ownerSelect.innerHTML = '<option value="">No owner (admin-only access)</option>';
                    ownerSelect.selectedIndex = 0;
                }
            }
        });
    }

    document.querySelectorAll('.s3tables-delete-bucket-btn').forEach(button => {
        button.addEventListener('click', function () {
            document.getElementById('deleteS3TablesBucketName').textContent = this.dataset.bucketName || '';
            document.getElementById('deleteS3TablesBucketModal').dataset.bucketArn = this.dataset.bucketArn || '';
            s3tablesBucketDeleteModal.show();
        });
    });

    document.querySelectorAll('.s3tables-bucket-policy-btn').forEach(button => {
        button.addEventListener('click', function () {
            const bucketArn = this.dataset.bucketArn || '';
            document.getElementById('s3tablesBucketPolicyArn').value = bucketArn;
            loadS3TablesBucketPolicy(bucketArn);
            s3tablesBucketPolicyModal.show();
        });
    });

    document.querySelectorAll('.s3tables-tags-btn').forEach(button => {
        button.addEventListener('click', function () {
            const resourceArn = this.dataset.resourceArn || '';
            openS3TablesTags(resourceArn);
        });
    });

    const createForm = document.getElementById('createS3TablesBucketForm');
    if (createForm) {
        createForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const name = document.getElementById('s3tablesBucketName').value.trim();
            const owner = ownerSelect.value;
            const tagsInput = document.getElementById('s3tablesBucketTags').value.trim();
            const tags = parseTagsInput(tagsInput);
            if (tags === null) return;
            const payload = { name: name, tags: tags, owner: owner };

            try {
                const response = await fetch('/api/s3tables/buckets', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await response.json();
                if (!response.ok) {
                    alert(data.error || 'Failed to create bucket');
                    return;
                }
                alert('Bucket created successfully');
                location.reload();
            } catch (error) {
                alert('Failed to create bucket: ' + error.message);
            }
        });
    }

    const policyForm = document.getElementById('s3tablesBucketPolicyForm');
    if (policyForm) {
        policyForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const bucketArn = document.getElementById('s3tablesBucketPolicyArn').value;
            const policy = document.getElementById('s3tablesBucketPolicyText').value.trim();
            if (!policy) {
                alert('Policy JSON is required');
                return;
            }
            try {
                const response = await fetch('/api/s3tables/bucket-policy', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ bucket_arn: bucketArn, policy: policy })
                });
                const data = await response.json();
                if (!response.ok) {
                    alert(data.error || 'Failed to update policy');
                    return;
                }
                alert('Policy updated');
                s3tablesBucketPolicyModal.hide();
            } catch (error) {
                alert('Failed to update policy: ' + error.message);
            }
        });
    }

    const tagsForm = document.getElementById('s3tablesTagsForm');
    if (tagsForm) {
        tagsForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const resourceArn = document.getElementById('s3tablesTagsResourceArn').value;
            const tags = parseTagsInput(document.getElementById('s3tablesTagsInput').value.trim());
            if (tags === null || Object.keys(tags).length === 0) {
                alert('Please provide tags to update');
                return;
            }
            await updateS3TablesTags(resourceArn, tags);
        });
    }
}

/**
 * Initialize S3 Tables Tables Page
 */
function initS3TablesTables() {
    s3tablesTableDeleteModal = new bootstrap.Modal(document.getElementById('deleteS3TablesTableModal'));
    s3tablesTablePolicyModal = new bootstrap.Modal(document.getElementById('s3tablesTablePolicyModal'));
    s3tablesTagsModal = new bootstrap.Modal(document.getElementById('s3tablesTagsModal'));

    const dataContainer = document.getElementById('s3tables-tables-content');
    const dataBucketArn = dataContainer.dataset.bucketArn || '';
    const dataNamespace = dataContainer.dataset.namespace || '';

    document.querySelectorAll('.s3tables-delete-table-btn').forEach(button => {
        button.addEventListener('click', function () {
            document.getElementById('deleteS3TablesTableName').textContent = this.dataset.tableName || '';
            document.getElementById('deleteS3TablesTableModal').dataset.tableName = this.dataset.tableName || '';
            s3tablesTableDeleteModal.show();
        });
    });

    document.querySelectorAll('.s3tables-table-policy-btn').forEach(button => {
        button.addEventListener('click', function () {
            document.getElementById('s3tablesTablePolicyBucketArn').value = dataBucketArn;
            document.getElementById('s3tablesTablePolicyNamespace').value = dataNamespace;
            document.getElementById('s3tablesTablePolicyName').value = this.dataset.tableName || '';
            loadS3TablesTablePolicy(dataBucketArn, dataNamespace, this.dataset.tableName || '');
            s3tablesTablePolicyModal.show();
        });
    });

    document.querySelectorAll('.s3tables-tags-btn').forEach(button => {
        button.addEventListener('click', function () {
            const resourceArn = this.dataset.resourceArn || '';
            openS3TablesTags(resourceArn);
        });
    });

    const createForm = document.getElementById('createS3TablesTableForm');
    if (createForm) {
        createForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const name = document.getElementById('s3tablesTableName').value.trim();
            const format = document.getElementById('s3tablesTableFormat').value;
            const metadataText = document.getElementById('s3tablesTableMetadata').value.trim();
            const tagsInput = document.getElementById('s3tablesTableTags').value.trim();
            const tags = parseTagsInput(tagsInput);
            if (tags === null) return;
            let metadata = null;
            if (metadataText) {
                try {
                    metadata = JSON.parse(metadataText);
                } catch (error) {
                    alert('Invalid metadata JSON');
                    return;
                }
            }
            const payload = { bucket_arn: dataBucketArn, namespace: dataNamespace, name: name, format: format, tags: tags };
            if (metadata) {
                payload.metadata = metadata;
            }
            try {
                const response = await fetch('/api/s3tables/tables', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await response.json();
                if (!response.ok) {
                    alert(data.error || 'Failed to create table');
                    return;
                }
                alert('Table created');
                location.reload();
            } catch (error) {
                alert('Failed to create table: ' + error.message);
            }
        });
    }

    const policyForm = document.getElementById('s3tablesTablePolicyForm');
    if (policyForm) {
        policyForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const policy = document.getElementById('s3tablesTablePolicyText').value.trim();
            if (!policy) {
                alert('Policy JSON is required');
                return;
            }
            try {
                const response = await fetch('/api/s3tables/table-policy', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ bucket_arn: dataBucketArn, namespace: dataNamespace, name: document.getElementById('s3tablesTablePolicyName').value, policy: policy })
                });
                const data = await response.json();
                if (!response.ok) {
                    alert(data.error || 'Failed to update policy');
                    return;
                }
                alert('Policy updated');
                s3tablesTablePolicyModal.hide();
            } catch (error) {
                alert('Failed to update policy: ' + error.message);
            }
        });
    }

    const tagsForm = document.getElementById('s3tablesTagsForm');
    if (tagsForm) {
        tagsForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const resourceArn = document.getElementById('s3tablesTagsResourceArn').value;
            const tags = parseTagsInput(document.getElementById('s3tablesTagsInput').value.trim());
            if (tags === null || Object.keys(tags).length === 0) {
                alert('Please provide tags to update');
                return;
            }
            await updateS3TablesTags(resourceArn, tags);
        });
    }
}

// Global scope functions used by onclick handlers

async function deleteS3TablesBucket() {
    const bucketArn = document.getElementById('deleteS3TablesBucketModal').dataset.bucketArn;
    if (!bucketArn) return;
    try {
        const response = await fetch(`/api/s3tables/buckets?bucket=${encodeURIComponent(bucketArn)}`, { method: 'DELETE' });
        const data = await response.json();
        if (!response.ok) {
            alert(data.error || 'Failed to delete bucket');
            return;
        }
        alert('Bucket deleted');
        location.reload();
    } catch (error) {
        alert('Failed to delete bucket: ' + error.message);
    }
}

async function loadS3TablesBucketPolicy(bucketArn) {
    document.getElementById('s3tablesBucketPolicyText').value = '';
    if (!bucketArn) return;
    try {
        const response = await fetch(`/api/s3tables/bucket-policy?bucket=${encodeURIComponent(bucketArn)}`);
        const data = await response.json();
        if (response.ok && data.policy) {
            document.getElementById('s3tablesBucketPolicyText').value = data.policy;
        }
    } catch (error) {
        console.error('Failed to load bucket policy', error);
    }
}

async function deleteS3TablesBucketPolicy() {
    const bucketArn = document.getElementById('s3tablesBucketPolicyArn').value;
    if (!bucketArn) return;
    try {
        const response = await fetch(`/api/s3tables/bucket-policy?bucket=${encodeURIComponent(bucketArn)}`, { method: 'DELETE' });
        const data = await response.json();
        if (!response.ok) {
            alert(data.error || 'Failed to delete policy');
            return;
        }
        alert('Policy deleted');
        document.getElementById('s3tablesBucketPolicyText').value = '';
    } catch (error) {
        alert('Failed to delete policy: ' + error.message);
    }
}

async function deleteS3TablesTable() {
    const dataContainer = document.getElementById('s3tables-tables-content');
    const dataBucketArn = dataContainer.dataset.bucketArn || '';
    const dataNamespace = dataContainer.dataset.namespace || '';
    const tableName = document.getElementById('deleteS3TablesTableModal').dataset.tableName;
    const versionToken = document.getElementById('deleteS3TablesTableVersion').value.trim();
    if (!tableName) return;
    const query = new URLSearchParams({
        bucket: dataBucketArn,
        namespace: dataNamespace,
        name: tableName
    });
    if (versionToken) {
        query.set('version', versionToken);
    }
    try {
        const response = await fetch(`/api/s3tables/tables?${query.toString()}`, { method: 'DELETE' });
        const data = await response.json();
        if (!response.ok) {
            alert(data.error || 'Failed to delete table');
            return;
        }
        alert('Table deleted');
        location.reload();
    } catch (error) {
        alert('Failed to delete table: ' + error.message);
    }
}

async function loadS3TablesTablePolicy(bucketArn, namespace, name) {
    document.getElementById('s3tablesTablePolicyText').value = '';
    if (!bucketArn || !namespace || !name) return;
    const query = new URLSearchParams({ bucket: bucketArn, namespace: namespace, name: name });
    try {
        const response = await fetch(`/api/s3tables/table-policy?${query.toString()}`);
        const data = await response.json();
        if (response.ok && data.policy) {
            document.getElementById('s3tablesTablePolicyText').value = data.policy;
        }
    } catch (error) {
        console.error('Failed to load table policy', error);
    }
}

async function deleteS3TablesTablePolicy() {
    const dataContainer = document.getElementById('s3tables-tables-content');
    const dataBucketArn = dataContainer.dataset.bucketArn || '';
    const dataNamespace = dataContainer.dataset.namespace || '';
    const query = new URLSearchParams({ bucket: dataBucketArn, namespace: dataNamespace, name: document.getElementById('s3tablesTablePolicyName').value });
    try {
        const response = await fetch(`/api/s3tables/table-policy?${query.toString()}`, { method: 'DELETE' });
        const data = await response.json();
        if (!response.ok) {
            alert(data.error || 'Failed to delete policy');
            return;
        }
        alert('Policy deleted');
        document.getElementById('s3tablesTablePolicyText').value = '';
    } catch (error) {
        alert('Failed to delete policy: ' + error.message);
    }
}

function parseTagsInput(input) {
    if (!input) return {};
    const tags = {};
    const maxTags = 10;
    const maxKeyLength = 128;
    const maxValueLength = 256;
    const parts = input.split(',');
    for (const part of parts) {
        const trimmedPart = part.trim();
        if (!trimmedPart) continue;
        const idx = trimmedPart.indexOf('=');
        if (idx <= 0) {
            alert('Invalid tag format. Use key=value, and key cannot be empty.');
            return null;
        }
        const key = trimmedPart.slice(0, idx).trim();
        const value = trimmedPart.slice(idx + 1).trim();
        if (!key) {
            alert('Invalid tag format. Use key=value, and key cannot be empty.');
            return null;
        }
        if (key.length > maxKeyLength) {
            alert(`Tag key length must be <= ${maxKeyLength}`);
            return null;
        }
        if (value.length > maxValueLength) {
            alert(`Tag value length must be <= ${maxValueLength}`);
            return null;
        }
        tags[key] = value;
        if (Object.keys(tags).length > maxTags) {
            alert(`Too many tags. Max ${maxTags} tags allowed.`);
            return null;
        }
    }
    return tags;
}

async function openS3TablesTags(resourceArn) {
    if (!resourceArn) return;
    document.getElementById('s3tablesTagsResourceArn').value = resourceArn;
    document.getElementById('s3tablesTagsInput').value = '';
    document.getElementById('s3tablesTagsDeleteInput').value = '';
    document.getElementById('s3tablesTagsList').textContent = 'Loading...';
    s3tablesTagsModal.show();
    try {
        const response = await fetch(`/api/s3tables/tags?arn=${encodeURIComponent(resourceArn)}`);
        const data = await response.json();
        if (response.ok) {
            document.getElementById('s3tablesTagsList').textContent = JSON.stringify(data.tags || {}, null, 2);
        } else {
            document.getElementById('s3tablesTagsList').textContent = data.error || 'Failed to load tags';
        }
    } catch (error) {
        document.getElementById('s3tablesTagsList').textContent = error.message;
    }
}

async function updateS3TablesTags(resourceArn, tags) {
    try {
        const response = await fetch('/api/s3tables/tags', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ resource_arn: resourceArn, tags: tags })
        });
        const data = await response.json();
        if (!response.ok) {
            alert(data.error || 'Failed to update tags');
            return;
        }
        alert('Tags updated');
        openS3TablesTags(resourceArn);
    } catch (error) {
        alert('Failed to update tags: ' + error.message);
    }
}

async function deleteS3TablesTags() {
    const resourceArn = document.getElementById('s3tablesTagsResourceArn').value;
    const keysInput = document.getElementById('s3tablesTagsDeleteInput').value.trim();
    if (!resourceArn) return;
    const tagKeys = keysInput.split(',').map(k => k.trim()).filter(k => k);
    if (tagKeys.length === 0) {
        alert('Provide tag keys to remove');
        return;
    }
    try {
        const response = await fetch('/api/s3tables/tags', {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ resource_arn: resourceArn, tag_keys: tagKeys })
        });
        const data = await response.json();
        if (!response.ok) {
            alert(data.error || 'Failed to remove tags');
            return;
        }
        alert('Tags removed');
        openS3TablesTags(resourceArn);
    } catch (error) {
        alert('Failed to remove tags: ' + error.message);
    }
}
