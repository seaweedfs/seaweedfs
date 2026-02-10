/**
 * Shared S3 Tables functionality for the SeaweedFS Admin Dashboard.
 */

// Shared Modals
let s3tablesBucketDeleteModal = null;
let s3tablesBucketPolicyModal = null;
let s3tablesTableDeleteModal = null;
let s3tablesTablePolicyModal = null;
let s3tablesTagsModal = null;
let icebergTableDeleteModal = null;

function getCSRFToken() {
    const tokenMeta = document.querySelector('meta[name="csrf-token"]');
    if (!tokenMeta) {
        return '';
    }
    return tokenMeta.getAttribute('content') || '';
}

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
    const bucketNameInput = document.getElementById('s3tablesBucketName');
    if (bucketNameInput) {
        bucketNameInput.addEventListener('input', function () {
            applyS3TablesBucketNameValidity(this, true);
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
            const bucketNameInput = document.getElementById('s3tablesBucketName');
            const name = bucketNameInput.value.trim();
            const nameError = applyS3TablesBucketNameValidity(bucketNameInput, false);
            if (nameError) {
                bucketNameInput.reportValidity();
                return;
            }
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
        const tableNameInput = document.getElementById('s3tablesTableName');
        if (tableNameInput) {
            tableNameInput.addEventListener('input', function () {
                applyS3TablesTableNameValidity(this, true);
            });
        }
        createForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const tableNameInput = document.getElementById('s3tablesTableName');
            const name = tableNameInput.value.trim();
            const nameError = applyS3TablesTableNameValidity(tableNameInput, false);
            if (nameError) {
                tableNameInput.reportValidity();
                return;
            }
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

/**
 * Initialize Iceberg Namespaces Page
 */
function initIcebergNamespaces() {
    const container = document.getElementById('iceberg-namespaces-content');
    if (!container) return;
    const bucketArn = container.dataset.bucketArn || '';
    const catalogName = container.dataset.catalogName || '';
    const csrfTokenInput = document.getElementById('icebergNamespaceCsrfToken');
    if (csrfTokenInput) {
        csrfTokenInput.value = getCSRFToken();
    }

    const namespaceInput = document.getElementById('icebergNamespaceName');
    if (namespaceInput) {
        namespaceInput.addEventListener('input', function () {
            applyS3TablesNamespaceNameValidity(this, true);
        });
    }

    const createForm = document.getElementById('createIcebergNamespaceForm');
    if (createForm) {
        createForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const namespaceInput = document.getElementById('icebergNamespaceName');
            const name = namespaceInput.value.trim();
            const nameError = applyS3TablesNamespaceNameValidity(namespaceInput, false);
            if (nameError) {
                namespaceInput.reportValidity();
                return;
            }
            try {
                const csrfToken = csrfTokenInput ? csrfTokenInput.value : getCSRFToken();
                const headers = { 'Content-Type': 'application/json' };
                if (csrfToken) {
                    headers['X-CSRF-Token'] = csrfToken;
                }
                const response = await fetch('/api/s3tables/namespaces', {
                    method: 'POST',
                    headers: headers,
                    body: JSON.stringify({ bucket_arn: bucketArn, name: name })
                });
                const data = await response.json();
                if (!response.ok) {
                    alert(data.error || 'Failed to create namespace');
                    return;
                }
                alert('Namespace created');
                location.reload();
            } catch (error) {
                alert('Failed to create namespace: ' + error.message);
            }
        });
    }

    initIcebergNamespaceTree(container, bucketArn, catalogName);
}

function initIcebergNamespaceTree(container, bucketArn, catalogName) {
    const nodes = container.querySelectorAll('.iceberg-namespace-collapse');
    nodes.forEach(node => {
        node.addEventListener('show.bs.collapse', async function () {
            if (node.dataset.loaded === 'true') return;
            node.textContent = 'Loading...';
            node.className = 'text-muted small';
            try {
                await loadIcebergNamespaceTables(node, bucketArn, catalogName);
                node.dataset.loaded = 'true';
            } catch (error) {
                node.textContent = 'Failed to load. Collapse and expand to retry.';
                node.className = 'text-danger small';
                console.error('Error loading namespace tables:', error);
            }
        });
    });
}

async function loadIcebergNamespaceTables(node, bucketArn, catalogName) {
    const namespace = node.dataset.namespace || '';
    if (!bucketArn || !namespace) {
        node.textContent = 'No namespace data available.';
        node.className = 'text-muted small';
        throw new Error('Missing bucket or namespace');
    }
    try {
        const query = new URLSearchParams({ bucket: bucketArn, namespace: namespace });
        const response = await fetch(`/api/s3tables/tables?${query.toString()}`);
        const data = await response.json();
        if (!response.ok) {
            node.textContent = data.error || 'Failed to load tables';
            node.className = 'text-danger small';
            throw new Error(data.error || 'Failed to load tables');
        }
        const tables = data.tables || [];
        if (tables.length === 0) {
            node.textContent = 'No tables found.';
            node.className = 'text-muted small ms-3';
            return;
        }
        node.innerHTML = '';
        const list = document.createElement('ul');
        list.className = 'list-group list-group-flush ms-3';
        tables.forEach(table => {
            const item = document.createElement('li');
            item.className = 'list-group-item py-1';
            const link = document.createElement('a');
            link.className = 'text-decoration-none';
            link.href = `/object-store/s3tables/buckets/${encodeURIComponent(catalogName)}/namespaces/${encodeURIComponent(namespace)}/tables/${encodeURIComponent(table.name)}`;
            const icon = document.createElement('i');
            icon.className = 'fas fa-table text-primary me-2';
            link.appendChild(icon);
            const nameSpan = document.createElement('span');
            nameSpan.textContent = table.name;
            link.appendChild(nameSpan);
            item.appendChild(link);
            list.appendChild(item);
        });
        node.appendChild(list);
    } catch (error) {
        if (!node.textContent) {
            node.textContent = 'Failed to load tables: ' + (error.message || 'Unknown error');
            node.className = 'text-danger small';
        }
        throw error;
    }
}

/**
 * Initialize Iceberg Tables Page
 */
function initIcebergTables() {
    const container = document.getElementById('iceberg-tables-content');
    if (!container) return;
    const bucketArn = container.dataset.bucketArn || '';
    const namespace = container.dataset.namespace || '';
    const csrfTokenInput = document.getElementById('icebergTableCsrfToken');
    if (csrfTokenInput) {
        csrfTokenInput.value = getCSRFToken();
    }

    initIcebergDeleteModal();

    const createForm = document.getElementById('createIcebergTableForm');
    if (createForm) {
        const tableNameInput = document.getElementById('icebergTableName');
        if (tableNameInput) {
            tableNameInput.addEventListener('input', function () {
                applyS3TablesTableNameValidity(this, true);
            });
        }
        createForm.addEventListener('submit', async function (e) {
            e.preventDefault();
            const tableNameInput = document.getElementById('icebergTableName');
            const name = tableNameInput.value.trim();
            const nameError = applyS3TablesTableNameValidity(tableNameInput, false);
            if (nameError) {
                tableNameInput.reportValidity();
                return;
            }
            const format = document.getElementById('icebergTableFormat').value;
            const metadataText = document.getElementById('icebergTableMetadata').value.trim();
            const tagsInput = document.getElementById('icebergTableTags').value.trim();
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
            const payload = { bucket_arn: bucketArn, namespace: namespace, name: name, format: format, tags: tags };
            if (metadata) {
                payload.metadata = metadata;
            }
            try {
                const csrfToken = csrfTokenInput ? csrfTokenInput.value : getCSRFToken();
                const headers = { 'Content-Type': 'application/json' };
                if (csrfToken) {
                    headers['X-CSRF-Token'] = csrfToken;
                }
                const response = await fetch('/api/s3tables/tables', {
                    method: 'POST',
                    headers: headers,
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
}

/**
 * Initialize Iceberg Table Details Page
 */
function initIcebergTableDetails() {
    initIcebergDeleteModal();
}

function initIcebergDeleteModal() {
    const modalEl = document.getElementById('deleteIcebergTableModal');
    if (!modalEl) return;
    icebergTableDeleteModal = new bootstrap.Modal(modalEl);
    document.querySelectorAll('.iceberg-delete-table-btn').forEach(button => {
        button.addEventListener('click', function () {
            modalEl.dataset.bucketArn = this.dataset.bucketArn || '';
            modalEl.dataset.namespace = this.dataset.namespace || '';
            modalEl.dataset.tableName = this.dataset.tableName || '';
            modalEl.dataset.catalogName = this.dataset.catalogName || '';
            document.getElementById('deleteIcebergTableName').textContent = this.dataset.tableName || '';
            document.getElementById('deleteIcebergTableVersion').value = '';
            icebergTableDeleteModal.show();
        });
    });
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

async function deleteIcebergTable() {
    const modalEl = document.getElementById('deleteIcebergTableModal');
    if (!modalEl) return;
    const bucketArn = modalEl.dataset.bucketArn || '';
    const namespace = modalEl.dataset.namespace || '';
    const tableName = modalEl.dataset.tableName || '';
    const catalogName = modalEl.dataset.catalogName || '';
    const versionToken = document.getElementById('deleteIcebergTableVersion').value.trim();
    if (!bucketArn || !namespace || !tableName) return;
    const query = new URLSearchParams({
        bucket: bucketArn,
        namespace: namespace,
        name: tableName
    });
    if (versionToken) {
        query.set('version', versionToken);
    }
    try {
        const csrfToken = getCSRFToken();
        const requestOptions = { method: 'DELETE' };
        if (csrfToken) {
            requestOptions.headers = { 'X-CSRF-Token': csrfToken };
        }
        const response = await fetch(`/api/s3tables/tables?${query.toString()}`, requestOptions);
        const data = await response.json();
        if (!response.ok) {
            alert(data.error || 'Failed to drop table');
            return;
        }
        alert('Table dropped');
        const isDetailsPage = window.location.pathname.includes('/tables/') && window.location.pathname.includes('/namespaces/');
        if (isDetailsPage && catalogName && namespace) {
            window.location.href = `/object-store/s3tables/buckets/${encodeURIComponent(catalogName)}/namespaces/${encodeURIComponent(namespace)}/tables`;
        } else {
            location.reload();
        }
    } catch (error) {
        alert('Failed to drop table: ' + error.message);
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

function isLowercaseLetterOrDigit(ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9');
}

function s3TablesBucketNameError(name) {
    if (!name) {
        return 'Bucket name is required';
    }
    if (name.length < 3 || name.length > 63) {
        return 'Bucket name must be between 3 and 63 characters';
    }
    if (!isLowercaseLetterOrDigit(name[0])) {
        return 'Bucket name must start with a letter or digit';
    }
    if (!isLowercaseLetterOrDigit(name[name.length - 1])) {
        return 'Bucket name must end with a letter or digit';
    }
    for (let i = 0; i < name.length; i++) {
        const ch = name[i];
        if (isLowercaseLetterOrDigit(ch) || ch === '-') {
            continue;
        }
        return 'Bucket name can only contain lowercase letters, numbers, and hyphens';
    }
    const reservedPrefixes = ['xn--', 'sthree-', 'amzn-s3-demo-', 'aws'];
    for (const prefix of reservedPrefixes) {
        if (name.startsWith(prefix)) {
            return `Bucket name cannot start with reserved prefix: ${prefix}`;
        }
    }
    const reservedSuffixes = ['-s3alias', '--ol-s3', '--x-s3', '--table-s3'];
    for (const suffix of reservedSuffixes) {
        if (name.endsWith(suffix)) {
            return `Bucket name cannot end with reserved suffix: ${suffix}`;
        }
    }
    return '';
}

function s3TablesNamespaceNameError(name) {
    if (!name) {
        return 'Namespace name is required';
    }
    if (name.length < 1 || name.length > 255) {
        return 'Namespace name must be between 1 and 255 characters';
    }
    if (name === '.' || name === '..') {
        return "namespace name cannot be '.' or '..'";
    }
    if (name.includes('/')) {
        return "namespace name cannot contain '/'";
    }

    const parts = name.split('.');
    for (const part of parts) {
        if (!part) {
            return 'namespace levels cannot be empty';
        }
        if (part.length < 1 || part.length > 255) {
            return 'Namespace name must be between 1 and 255 characters';
        }
        if (!isLowercaseLetterOrDigit(part[0])) {
            return 'Namespace name must start with a letter or digit';
        }
        if (!isLowercaseLetterOrDigit(part[part.length - 1])) {
            return 'Namespace name must end with a letter or digit';
        }
        for (const ch of part) {
            if (isLowercaseLetterOrDigit(ch) || ch === '_') {
                continue;
            }
            return "invalid namespace name: only 'a-z', '0-9', and '_' are allowed";
        }
        if (part.startsWith('aws')) {
            return "namespace name cannot start with reserved prefix 'aws'";
        }
    }
    return '';
}

function s3TablesTableNameError(name) {
    if (!name) {
        return 'Table name is required';
    }
    if (name.length < 1 || name.length > 255) {
        return 'Table name must be between 1 and 255 characters';
    }
    if (name === '.' || name === '..' || name.includes('/')) {
        return "invalid table name: cannot be '.', '..' or contain '/'";
    }
    if (!isLowercaseLetterOrDigit(name[0])) {
        return 'Table name must start with a letter or digit';
    }
    for (const ch of name) {
        if (isLowercaseLetterOrDigit(ch) || ch === '_') {
            continue;
        }
        return "invalid table name: only 'a-z', '0-9', and '_' are allowed";
    }
    return '';
}

function applyS3TablesBucketNameValidity(input, allowEmpty) {
    const name = input.value.trim();
    if (allowEmpty && name === '') {
        input.setCustomValidity('');
        return '';
    }
    const message = s3TablesBucketNameError(name);
    input.setCustomValidity(message);
    return message;
}

function applyS3TablesNamespaceNameValidity(input, allowEmpty) {
    const name = input.value.trim();
    if (allowEmpty && name === '') {
        input.setCustomValidity('');
        return '';
    }
    const message = s3TablesNamespaceNameError(name);
    input.setCustomValidity(message);
    return message;
}

function applyS3TablesTableNameValidity(input, allowEmpty) {
    const name = input.value.trim();
    if (allowEmpty && name === '') {
        input.setCustomValidity('');
        return '';
    }
    const message = s3TablesTableNameError(name);
    input.setCustomValidity(message);
    return message;
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
