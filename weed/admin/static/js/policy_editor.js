// Shared visual policy editor: renders and edits an IAM/bucket policy
// document (Version + Statement list) via a structured form alongside a
// raw-JSON tab, kept in sync in both directions.
//
// Extracted from weed/admin/view/app/policies.templ (the IAM policy
// management page), which was its original and, for a while, only
// consumer. Any page embedding this editor must first render the shared
// datalists (see the PolicyDatalists templ component in
// weed/admin/view/app/policy_datalists.templ) and load this script after
// admin.js (for basePath/escapeHtml) and modal-alerts.js (for showAlert).
//
// Usage: call registerPolicyEditor(which, config) once to declare an
// editor instance (see its doc comment for the id conventions and
// config knobs), then setupPolicyEditor(which) once to wire up its DOM
// listeners. which is an arbitrary string ("create", "edit",
// "bucketPolicy", ...) that namespaces one editor instance's DOM ids and
// state from another's on the same page.

// Per-`which` editor configuration. See registerPolicyEditor.
const POLICY_EDITOR_CONFIG = {};

// registerPolicyEditor declares (or redeclares) the configuration for one
// editor instance. Call before setupPolicyEditor(which), and again any
// time a config value (e.g. `bucket`) needs to change for an
// already-set-up instance (setupPolicyEditor only needs to run once per
// `which`; its DOM listeners read POLICY_EDITOR_CONFIG live).
//
// config:
//   textareaId          - id of the JSON <textarea>. Default: which + 'PolicyDocument'.
//   editorBodyId        - id of the structured-editor container. Default: which + 'PolicyEditorBody'.
//   addStatementBtnId   - id of the "Add statement" button. Default: which + 'PolicyAddStatementBtn'.
//   editorTabBtnId      - id of the Editor tab button. Default: which + 'PolicyEditorTabBtn'.
//   jsonTabBtnId        - id of the JSON tab button. Default: which + 'PolicyJsonTabBtn'.
//   actionDatalistId    - id of the shared action-suggestions <datalist>. Default: 'policyActionSuggestions'.
//   resourceDatalistId  - id of the shared resource-suggestions <datalist>. Default: 'policyResourceSuggestions'.
//   principalDatalistId - id of the shared principal-suggestions <datalist>. Default: 'policyPrincipalSuggestions'.
//   requirePrincipal    - if true, a new statement seeds Principal with '*'
//                         instead of leaving it empty. Bucket policies
//                         require a Principal per statement; IAM policies
//                         don't. The server remains the source of truth for
//                         this rule either way - see
//                         policy_engine.ValidateBucketPolicy.
//   bucket              - if set, the Resource autocomplete only offers
//                         this bucket's ARN and ARN/*, instead of fetching
//                         every bucket, and a new statement's Resource is
//                         seeded with arn:aws:s3:::<bucket>/*.
function registerPolicyEditor(which, config) {
    POLICY_EDITOR_CONFIG[which] = Object.assign({
        textareaId: which + 'PolicyDocument',
        editorBodyId: which + 'PolicyEditorBody',
        addStatementBtnId: which + 'PolicyAddStatementBtn',
        editorTabBtnId: which + 'PolicyEditorTabBtn',
        jsonTabBtnId: which + 'PolicyJsonTabBtn',
        actionDatalistId: 'policyActionSuggestions',
        resourceDatalistId: 'policyResourceSuggestions',
        principalDatalistId: 'policyPrincipalSuggestions',
        requirePrincipal: false,
        bucket: null
    }, config || {});
}

function policyEditorConfig(which) {
    return POLICY_EDITOR_CONFIG[which] || (registerPolicyEditor(which, {}), POLICY_EDITOR_CONFIG[which]);
}

    // Structured-editor state, one entry per modal ("create" / "edit"). Each
    // entry is { version, statements: [{ sid, effect, actions, resources, extras }] }.
    // "extras" holds the JSON text of any statement fields the structured
    // editor doesn't expose (Principal, NotPrincipal, NotResource, Condition,
    // or any future/unknown key), so they round-trip untouched.
    let policyEditors = {
        create: { version: '2012-10-17', statements: [], otherFields: {} },
        edit: { version: '2012-10-17', statements: [], otherFields: {} }
    };

    // Lazily initializes and returns policyEditors[which]. 'create'/'edit'
    // are pre-populated above, but a page with its own `which` (e.g. the
    // bucket policy modal) doesn't populate one until its first successful
    // load - and that load is asynchronous, so the Editor/JSON tabs and
    // Add-statement button can be reachable before it resolves (nothing in
    // this file enforces that a page hide them meanwhile). Route reads
    // through this instead of policyEditors[which] directly wherever that
    // race is possible, so a click in that window gets a valid empty state
    // instead of a TypeError on undefined.
    function policyEditorState(which) {
        if (!policyEditors[which]) {
            policyEditors[which] = { version: '2012-10-17', statements: [], otherFields: {} };
        }
        return policyEditors[which];
    }

    const POLICY_STATEMENT_KNOWN_KEYS = ['Sid', 'Effect', 'Action', 'Resource', 'NotResource'];

    // Shown for a policy that parsed as JSON but that the structured editor
    // can't represent, so its editor state carries { unparsed: true } and the
    // document only ever lives in the JSON tab.
    const POLICY_JSON_TAB_ONLY_MESSAGE = 'This policy can only be edited on the JSON tab.';

    // Maps a policy-list-item's data-field attribute to the editor-state
    // array it belongs to.
    const POLICY_LIST_FIELD_TO_STATE_KEY = { action: 'actions', resource: 'resources', principal: 'principalValues' };

    function policyTextareaId(which) {
        return policyEditorConfig(which).textareaId;
    }

    function policyEditorBodyId(which) {
        return policyEditorConfig(which).editorBodyId;
    }

    function normalizeToStringArray(value) {
        if (value === undefined || value === null) return [];
        // Coerce: these feed escapeHtml, which calls text.replace, and a
        // policy is free to carry a number or a boolean here.
        if (Array.isArray(value)) return value.map(String);
        return [String(value)];
    }

    const POLICY_DOCUMENT_KNOWN_KEYS = ['Version', 'Statement'];

    // Converts a policy document (as parsed from JSON) into editor state.
    //
    // Top-level keys the editor doesn't model (e.g. Id) are kept verbatim in
    // state.otherFields and merged back on serialization, so a round-trip
    // through the Editor tab doesn't rewrite text the user typed in the JSON
    // tab. The admin API's PolicyDocument only carries Version and Statement,
    // so such fields are still dropped by the server on save; see
    // confirmPolicyFieldDiscard, which warns the user before that happens.
    function policyDocToEditorState(doc) {
        if (doc === null || typeof doc !== 'object') {
            // null or a bare scalar (string/number/boolean) can't represent a
            // policy document; treating it as "zero statements" would hide
            // from the user that their input wasn't actually a document.
            throw new Error('Policy document must be a JSON object (got ' + JSON.stringify(doc) + ')');
        }
        const state = { version: doc.Version || '2012-10-17', statements: [], otherFields: {} };
        if (!Array.isArray(doc)) {
            Object.keys(doc).forEach(function(key) {
                if (POLICY_DOCUMENT_KNOWN_KEYS.indexOf(key) === -1) {
                    state.otherFields[key] = doc[key];
                }
            });
        }
        const rawStatements = doc && doc.Statement
            ? (Array.isArray(doc.Statement) ? doc.Statement : [doc.Statement])
            : [];
        rawStatements.forEach(function(stmt, idx) {
            stmt = stmt || {};
            if (stmt.Effect !== 'Allow' && stmt.Effect !== 'Deny') {
                // Defaulting a missing/malformed Effect to "Allow" would
                // silently turn e.g. a typo'd "deny" into a permissive
                // statement. Reject instead of guessing.
                throw new Error('Statement ' + (idx + 1) + ': Effect must be exactly "Allow" or "Deny" (got ' +
                    JSON.stringify(stmt.Effect === undefined ? null : stmt.Effect) + ')');
            }
            const hasResource = Object.prototype.hasOwnProperty.call(stmt, 'Resource');
            const hasNotResource = Object.prototype.hasOwnProperty.call(stmt, 'NotResource');
            if (hasResource && hasNotResource) {
                // The two are mutually exclusive; a document with both isn't
                // representable by the mode dropdown, so ask the user to fix
                // it in the JSON tab rather than silently picking one.
                throw new Error('Statement ' + (idx + 1) + ': cannot specify both Resource and NotResource');
            }
            const resourceMode = hasNotResource ? 'NotResource' : 'Resource';

            const hasPrincipal = Object.prototype.hasOwnProperty.call(stmt, 'Principal');
            const hasNotPrincipal = Object.prototype.hasOwnProperty.call(stmt, 'NotPrincipal');
            if (hasPrincipal && hasNotPrincipal) {
                throw new Error('Statement ' + (idx + 1) + ': cannot specify both Principal and NotPrincipal');
            }
            let principalMode = 'Principal';
            let principalValues = [];
            let principalManaged = false;
            let hasComplexPrincipal = false;
            if (hasPrincipal || hasNotPrincipal) {
                const principalKey = hasNotPrincipal ? 'NotPrincipal' : 'Principal';
                const parsedValues = parseSimpleAwsPrincipal(stmt[principalKey]);
                if (parsedValues) {
                    principalMode = principalKey;
                    principalValues = parsedValues;
                    principalManaged = true;
                } else {
                    // Bare string/array, a type other than AWS, or multiple
                    // types at once - this v1 editor only models a single
                    // {"AWS": ...} form. Leave it in extras rather than
                    // guessing or discarding it.
                    hasComplexPrincipal = true;
                }
            }

            const extras = {};
            Object.keys(stmt).forEach(function(key) {
                if (POLICY_STATEMENT_KNOWN_KEYS.indexOf(key) !== -1) return;
                if (principalManaged && (key === 'Principal' || key === 'NotPrincipal')) return;
                extras[key] = stmt[key];
            });
            state.statements.push({
                sid: String(stmt.Sid || ''),
                effect: stmt.Effect,
                actions: normalizeToStringArray(stmt.Action),
                resourceMode: resourceMode,
                resources: normalizeToStringArray(hasNotResource ? stmt.NotResource : stmt.Resource),
                principalMode: principalMode,
                principalValues: principalValues,
                hasComplexPrincipal: hasComplexPrincipal,
                extras: Object.keys(extras).length ? JSON.stringify(extras, null, 2) : ''
            });
        });
        return state;
    }

    // Returns the flattened list of values if `value` is a policy Principal /
    // NotPrincipal expressed as one of the two forms this editor's simple
    // text field models: the bare wildcard "*" (standard AWS shorthand for
    // "everyone"), or a single-key {"AWS": "..."} / {"AWS": ["...", ...]}
    // object. Returns null for anything else (any other bare string/array, a
    // different type key, or multiple type keys at once), which the caller
    // then leaves untouched in "extras".
    function parseSimpleAwsPrincipal(value) {
        if (value === '*') return ['*'];
        if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
        const keys = Object.keys(value);
        if (keys.length !== 1 || keys[0] !== 'AWS') return null;
        return normalizeToStringArray(value.AWS);
    }

    // Converts editor state back into a policy document. Structured fields
    // (Sid/Effect/Action/Resource-or-NotResource) always take precedence over
    // whatever is in "extras" in case of a conflicting key.
    //
    // Throws if a statement's "advanced fields" box holds malformed or
    // non-object JSON, instead of silently dropping it: those fields can
    // carry Principal/NotPrincipal/Condition, so silently continuing with an
    // empty object would change the policy's authorization behavior without
    // the user noticing.
    function policyEditorStateToDoc(state) {
        // Unmanaged top-level keys first, so Version/Statement below always win.
        const doc = Object.assign({}, state.otherFields || {});
        doc.Version = state.version || '2012-10-17';
        doc.Statement = [];
        (state.statements || []).forEach(function(s, idx) {
            let stmt = {};
            if (s.extras && s.extras.trim()) {
                let parsed;
                try {
                    parsed = JSON.parse(s.extras);
                } catch (e) {
                    throw new Error('Statement ' + (idx + 1) + ': advanced fields contain invalid JSON (' + e.message + ')');
                }
                if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
                    throw new Error('Statement ' + (idx + 1) + ': advanced fields must be a JSON object');
                }
                stmt = parsed;
            }
            if (s.sid) {
                stmt.Sid = s.sid;
            } else {
                delete stmt.Sid;
            }
            stmt.Effect = s.effect === 'Deny' ? 'Deny' : 'Allow';
            const actions = (s.actions || []).map(function(a) { return (a || '').trim(); }).filter(Boolean);
            if (actions.length) {
                stmt.Action = actions.length === 1 ? actions[0] : actions;
            } else {
                delete stmt.Action;
            }
            const resources = (s.resources || []).map(function(r) { return (r || '').trim(); }).filter(Boolean);
            if (resources.length) {
                // Resource and NotResource are mutually exclusive; only the
                // key matching the selected mode is ever written.
                if (s.resourceMode === 'NotResource') {
                    stmt.NotResource = resources.length === 1 ? resources[0] : resources;
                    delete stmt.Resource;
                } else {
                    stmt.Resource = resources.length === 1 ? resources[0] : resources;
                    delete stmt.NotResource;
                }
            } else {
                delete stmt.Resource;
                delete stmt.NotResource;
            }
            const principalValues = (s.principalValues || []).map(function(p) { return (p || '').trim(); }).filter(Boolean);
            if (principalValues.length) {
                // Same exclusivity rule as Resource/NotResource. Structured
                // values always win over whatever "extras" held for these
                // keys. A lone "*" is written as the bare wildcard (standard
                // AWS shorthand for "everyone"); anything else is wrapped in
                // the standard {"AWS": ...} form (v1 only models AWS
                // principals).
                delete stmt.Principal;
                delete stmt.NotPrincipal;
                const wrappedPrincipal = (principalValues.length === 1 && principalValues[0] === '*')
                    ? '*'
                    : { AWS: principalValues.length === 1 ? principalValues[0] : principalValues };
                if (s.principalMode === 'NotPrincipal') {
                    stmt.NotPrincipal = wrappedPrincipal;
                } else {
                    stmt.Principal = wrappedPrincipal;
                }
            }
            // If principalValues is empty, leave stmt.Principal/NotPrincipal
            // untouched: it may hold a complex form preserved verbatim from
            // "extras" (see parseSimpleAwsPrincipal) that the user never
            // touched via this field, and clearing it here would silently
            // discard it.
            doc.Statement.push(stmt);
        });
        return doc;
    }

    // Renders the structured editor for the given modal ("create"/"edit")
    // from policyEditors[which] into its container.
    function renderPolicyEditor(which) {
        const container = document.getElementById(policyEditorBodyId(which));
        if (!container) return;
        const state = policyEditorState(which);

        if (state.unparsed) {
            container.innerHTML = '<p class="text-muted">This policy uses a form the structured editor cannot show. Edit it on the JSON tab.</p>';
            return;
        }

        if (state.statements.length === 0) {
            container.innerHTML = '<p class="text-muted">No statements yet. Click "Add statement" to create one.</p>';
            return;
        }

        let html = '';
        state.statements.forEach(function(stmt, idx) {
            const actionRows = stmt.actions.map(function(action, actionIdx) {
                return policyListRowHtml(which, idx, 'action', actionIdx, action);
            }).join('');
            const resourceRows = stmt.resources.map(function(resource, resourceIdx) {
                return policyListRowHtml(which, idx, 'resource', resourceIdx, resource);
            }).join('');
            const principalRows = stmt.principalValues.map(function(principal, principalIdx) {
                return policyListRowHtml(which, idx, 'principal', principalIdx, principal);
            }).join('');

            html +=
                '<div class="card mb-3" data-statement-index="' + idx + '">' +
                '<div class="card-body">' +
                '<div class="d-flex justify-content-between align-items-start mb-2">' +
                '<h6 class="card-title mb-0">Statement ' + (idx + 1) + '</h6>' +
                '<button type="button" class="btn btn-sm btn-outline-danger policy-remove-statement-btn" data-which="' + which + '" data-index="' + idx + '"><i class="fas fa-trash"></i></button>' +
                '</div>' +
                '<div class="row mb-2">' +
                '<div class="col-md-6">' +
                '<label class="form-label">Sid (optional)</label>' +
                '<input type="text" class="form-control form-control-sm policy-stmt-sid" data-which="' + which + '" data-index="' + idx + '" value="' + escapeHtml(stmt.sid) + '">' +
                '</div>' +
                '<div class="col-md-6">' +
                '<label class="form-label d-block">Effect</label>' +
                '<div class="btn-group" role="group">' +
                '<input type="radio" class="btn-check policy-stmt-effect" name="policyEffect-' + which + '-' + idx + '" id="policyEffectAllow-' + which + '-' + idx + '" data-which="' + which + '" data-index="' + idx + '" value="Allow"' + (stmt.effect === 'Allow' ? ' checked' : '') + '>' +
                '<label class="btn btn-outline-success btn-sm" for="policyEffectAllow-' + which + '-' + idx + '">Allow</label>' +
                '<input type="radio" class="btn-check policy-stmt-effect" name="policyEffect-' + which + '-' + idx + '" id="policyEffectDeny-' + which + '-' + idx + '" data-which="' + which + '" data-index="' + idx + '" value="Deny"' + (stmt.effect === 'Deny' ? ' checked' : '') + '>' +
                '<label class="btn btn-outline-danger btn-sm" for="policyEffectDeny-' + which + '-' + idx + '">Deny</label>' +
                '</div>' +
                '</div>' +
                '</div>' +
                '<fieldset class="policy-stmt-fieldset">' +
                '<legend class="policy-stmt-legend border rounded">Actions</legend>' +
                '<div class="policy-action-rows" data-which="' + which + '" data-index="' + idx + '">' + actionRows + '</div>' +
                '<button type="button" class="btn btn-sm btn-outline-secondary policy-add-list-item-btn" data-which="' + which + '" data-index="' + idx + '" data-field="action"><i class="fas fa-plus me-1"></i>Add action</button>' +
                '</fieldset>' +
                '<fieldset class="policy-stmt-fieldset">' +
                '<legend class="policy-stmt-legend">' +
                '<select class="form-select form-select-sm d-inline-block w-auto policy-stmt-resource-mode" data-which="' + which + '" data-index="' + idx + '">' +
                '<option value="Resource"' + (stmt.resourceMode !== 'NotResource' ? ' selected' : '') + '>Resource</option>' +
                '<option value="NotResource"' + (stmt.resourceMode === 'NotResource' ? ' selected' : '') + '>NotResource</option>' +
                '</select>' +
                '</legend>' +
                (stmt.resourceMode === 'NotResource' ? '<div class="form-text mt-0 mb-1">The statement applies to every resource except the ones listed.</div>' : '') +
                '<div class="policy-resource-rows" data-which="' + which + '" data-index="' + idx + '">' + resourceRows + '</div>' +
                '<button type="button" class="btn btn-sm btn-outline-secondary policy-add-list-item-btn" data-which="' + which + '" data-index="' + idx + '" data-field="resource"><i class="fas fa-plus me-1"></i>Add resource</button>' +
                '</fieldset>' +
                '<fieldset class="policy-stmt-fieldset">' +
                '<legend class="policy-stmt-legend">' +
                '<select class="form-select form-select-sm d-inline-block w-auto policy-stmt-principal-mode" data-which="' + which + '" data-index="' + idx + '">' +
                '<option value="Principal"' + (stmt.principalMode !== 'NotPrincipal' ? ' selected' : '') + '>Principal</option>' +
                '<option value="NotPrincipal"' + (stmt.principalMode === 'NotPrincipal' ? ' selected' : '') + '>NotPrincipal</option>' +
                '</select>' +
                '</legend>' +
                '<div class="form-text mt-0 mb-1">Principal / NotPrincipal (AWS account/user ARN, or "*" for everyone). Only the AWS type and "*" are supported here; other forms stay editable via Advanced fields.</div>' +
                (stmt.hasComplexPrincipal ? '<div class="form-text text-warning mt-0 mb-1"><i class="fas fa-triangle-exclamation me-1"></i>This statement\'s Principal/NotPrincipal uses a form not supported by this field &mdash; see Advanced fields below.</div>' : '') +
                '<div class="policy-principal-rows" data-which="' + which + '" data-index="' + idx + '">' + principalRows + '</div>' +
                '<button type="button" class="btn btn-sm btn-outline-secondary policy-add-list-item-btn" data-which="' + which + '" data-index="' + idx + '" data-field="principal"><i class="fas fa-plus me-1"></i>Add principal</button>' +
                '</fieldset>' +
                '<details class="mt-3"' + (stmt.extras ? ' open' : '') + '>' +
                '<summary class="text-muted">Advanced fields (Principal, NotPrincipal, Condition, raw JSON)</summary>' +
                '<textarea class="form-control form-control-sm mt-2 policy-stmt-extras" data-which="' + which + '" data-index="' + idx + '" rows="4" placeholder="{}">' + escapeHtml(stmt.extras) + '</textarea>' +
                '</details>' +
                '</div>' +
                '</div>';
        });
        container.innerHTML = html;
    }

    function policyListRowHtml(which, stmtIdx, field, itemIdx, value) {
        const cfg = policyEditorConfig(which);
        let listAttr = '';
        if (field === 'action') listAttr = ' list="' + cfg.actionDatalistId + '"';
        else if (field === 'resource') listAttr = ' list="' + cfg.resourceDatalistId + '"';
        else if (field === 'principal') listAttr = ' list="' + cfg.principalDatalistId + '"';
        return '<div class="input-group input-group-sm mb-1">' +
            '<input type="text" class="form-control policy-list-item" ' + listAttr + ' data-which="' + which + '" data-index="' + stmtIdx + '" data-field="' + field + '" data-item-index="' + itemIdx + '" value="' + escapeHtml(value) + '">' +
            '<button type="button" class="btn btn-outline-danger policy-remove-list-item-btn" data-which="' + which + '" data-index="' + stmtIdx + '" data-field="' + field + '" data-item-index="' + itemIdx + '"><i class="fas fa-times"></i></button>' +
            '</div>';
    }

    // Reads whatever is currently displayed in the editor tab's DOM back into
    // policyEditors[which], so nothing typed is lost before a save/tab-switch/serialize.
    function commitPolicyEditorForm(which) {
        const state = policyEditors[which];
        if (!state) return;

        document.querySelectorAll('.policy-stmt-sid[data-which="' + which + '"]').forEach(function(el) {
            const idx = parseInt(el.getAttribute('data-index'), 10);
            if (state.statements[idx]) state.statements[idx].sid = el.value;
        });
        document.querySelectorAll('.policy-stmt-effect[data-which="' + which + '"]:checked').forEach(function(el) {
            const idx = parseInt(el.getAttribute('data-index'), 10);
            if (state.statements[idx]) state.statements[idx].effect = el.value;
        });
        document.querySelectorAll('.policy-stmt-extras[data-which="' + which + '"]').forEach(function(el) {
            const idx = parseInt(el.getAttribute('data-index'), 10);
            if (state.statements[idx]) state.statements[idx].extras = el.value;
        });
        document.querySelectorAll('.policy-stmt-resource-mode[data-which="' + which + '"]').forEach(function(el) {
            const idx = parseInt(el.getAttribute('data-index'), 10);
            if (state.statements[idx]) state.statements[idx].resourceMode = el.value;
        });
        document.querySelectorAll('.policy-stmt-principal-mode[data-which="' + which + '"]').forEach(function(el) {
            const idx = parseInt(el.getAttribute('data-index'), 10);
            if (state.statements[idx]) state.statements[idx].principalMode = el.value;
        });
        document.querySelectorAll('.policy-list-item[data-which="' + which + '"]').forEach(function(el) {
            const idx = parseInt(el.getAttribute('data-index'), 10);
            const itemIdx = parseInt(el.getAttribute('data-item-index'), 10);
            const field = POLICY_LIST_FIELD_TO_STATE_KEY[el.getAttribute('data-field')] || 'resources';
            if (state.statements[idx] && state.statements[idx][field]) {
                state.statements[idx][field][itemIdx] = el.value;
            }
        });
    }

    // Serializes policyEditors[which] into the JSON textarea. Call before
    // switching to the JSON tab or before submitting, so the textarea always
    // reflects the editor's current contents.
    function commitPolicyEditorToTextarea(which) {
        commitPolicyEditorForm(which);
        const doc = policyEditorStateToDoc(policyEditors[which]);
        document.getElementById(policyTextareaId(which)).value = JSON.stringify(doc, null, 2);
    }

    // Parses the JSON textarea into policyEditors[which] and re-renders the
    // editor. Returns false (and shows an alert) if the JSON is invalid or a
    // statement's Effect isn't exactly "Allow"/"Deny", leaving the JSON tab
    // as the active one so the user can fix it.
    function commitPolicyTextareaToEditor(which) {
        const text = document.getElementById(policyTextareaId(which)).value;
        if (!text || !text.trim()) {
            policyEditors[which] = { version: '2012-10-17', statements: [], otherFields: {} };
            renderPolicyEditor(which);
            return true;
        }
        let doc;
        try {
            doc = JSON.parse(text);
        } catch (e) {
            showAlert('Invalid JSON in policy document: ' + e.message, 'error');
            return false;
        }
        let newState;
        try {
            newState = policyDocToEditorState(doc);
        } catch (e) {
            showAlert(e.message, 'error');
            return false;
        }
        policyEditors[which] = newState;
        renderPolicyEditor(which);
        return true;
    }

    function activatePolicyTab(idKey, which) {
        const btn = document.getElementById(policyEditorConfig(which)[idKey]);
        if (btn) bootstrap.Tab.getOrCreateInstance(btn).show();
    }

    // Populates the editor for `which` from whatever is currently in its
    // JSON textarea (typically right after a GET fills the textarea) and
    // switches to whichever tab can actually show the result.
    //
    // Unlike commitPolicyTextareaToEditor - which assumes a tab is already
    // showing and leaves it in place on failure so a Save can't silently
    // clobber it - this function has no "current tab" to defer to: it is
    // the thing that establishes one. So on a document the structured
    // editor can't represent (invalid JSON, or valid JSON
    // policyDocToEditorState rejects), it marks the state `unparsed` and
    // switches to the JSON tab instead of leaving the Editor tab showing
    // empty/stale state that a careless Save would serialize over the
    // real document. Mirrors editPolicy's fallback in policies.templ.
    function loadPolicyTextareaIntoEditor(which) {
        const text = document.getElementById(policyTextareaId(which)).value;
        if (!text || !text.trim()) {
            policyEditors[which] = { version: '2012-10-17', statements: [], otherFields: {} };
            renderPolicyEditor(which);
            activatePolicyTab('editorTabBtnId', which);
            return true;
        }
        let doc;
        try {
            doc = JSON.parse(text);
        } catch (e) {
            policyEditors[which] = { version: '2012-10-17', statements: [], otherFields: {}, unparsed: true };
            renderPolicyEditor(which);
            showAlert('Invalid JSON in stored policy: ' + e.message + '. ' + POLICY_JSON_TAB_ONLY_MESSAGE, 'error');
            activatePolicyTab('jsonTabBtnId', which);
            return false;
        }
        let state;
        try {
            state = policyDocToEditorState(doc);
        } catch (e) {
            policyEditors[which] = { version: '2012-10-17', statements: [], otherFields: {}, unparsed: true };
            renderPolicyEditor(which);
            showAlert(e.message + '. ' + POLICY_JSON_TAB_ONLY_MESSAGE, 'error');
            activatePolicyTab('jsonTabBtnId', which);
            return false;
        }
        policyEditors[which] = state;
        renderPolicyEditor(which);
        activatePolicyTab('editorTabBtnId', which);
        return true;
    }

    function addPolicyStatement(which) {
        if (policyEditorState(which).unparsed) {
            showAlert(POLICY_JSON_TAB_ONLY_MESSAGE, 'error');
            return;
        }
        const cfg = policyEditorConfig(which);
        commitPolicyEditorForm(which);
        policyEditorState(which).statements.push({
            sid: '', effect: 'Allow', actions: [],
            resourceMode: 'Resource', resources: cfg.bucket ? ['arn:aws:s3:::' + cfg.bucket + '/*'] : [],
            principalMode: 'Principal', principalValues: cfg.requirePrincipal ? ['*'] : [], hasComplexPrincipal: false,
            extras: ''
        });
        renderPolicyEditor(which);
    }

    // True while the JSON tab (rather than the Editor tab) is the one
    // currently shown for `which`.
    function isPolicyJsonTabActive(which) {
        const jsonTabBtn = document.getElementById(policyEditorConfig(which).jsonTabBtnId);
        return !!(jsonTabBtn && jsonTabBtn.classList.contains('active'));
    }

    // Commits whichever tab is currently visible into the other side, so a
    // save/validate action always uses what the user is actually looking at
    // instead of silently overwriting it with stale state from the tab
    // they're not on. Returns false (after alerting the user) if that isn't
    // possible - e.g. invalid JSON on either side - so the caller can abort.
    function commitPolicyActiveTab(which) {
        if (isPolicyJsonTabActive(which)) {
            // The JSON tab is the source of truth right now; parse it back
            // into the structured editor to keep both in sync, but leave the
            // textarea's own text untouched.
            return commitPolicyTextareaToEditor(which);
        }
        if (policyEditorState(which).unparsed) {
            // The editor never held this document, so serializing it would
            // write an empty policy over whatever is in the JSON tab.
            showAlert(POLICY_JSON_TAB_ONLY_MESSAGE, 'error');
            return false;
        }
        try {
            commitPolicyEditorToTextarea(which);
            return true;
        } catch (e) {
            showAlert(e.message, 'error');
            return false;
        }
    }

    // The admin API's policy document carries only Version and Statement, so
    // any other top-level key (e.g. Id) is discarded server-side on save even
    // though the editor round-trips it between tabs. Warn before that happens
    // rather than letting the field vanish silently. Returns false if the
    // user cancels.
    function confirmPolicyFieldDiscard(which) {
        const otherFields = Object.keys((policyEditors[which] || {}).otherFields || {});
        if (otherFields.length === 0) return true;
        return confirm(
            'The following top-level field(s) are not supported and will be dropped when this policy is saved: ' +
            otherFields.join(', ') + '.\n\nSave anyway?');
    }

    // Client-side check for the requirePrincipal config knob: returns an
    // error message naming the first statement missing a Principal /
    // NotPrincipal, or null if the document is fine. Purely a fast-feedback
    // convenience - the server (policy_engine.ValidateBucketPolicy) is the
    // actual authority on this rule and re-checks it regardless.
    function validatePolicyEditorDoc(which, doc) {
        if (!policyEditorConfig(which).requirePrincipal) return null;
        const statements = (doc && doc.Statement) || [];
        for (let i = 0; i < statements.length; i++) {
            const stmt = statements[i] || {};
            if (stmt.Principal === undefined && stmt.NotPrincipal === undefined) {
                return 'Statement ' + (i + 1) + ': a Principal (or NotPrincipal) is required.';
            }
        }
        return null;
    }

    function setupPolicyEditor(which) {
        const cfg = policyEditorConfig(which);
        document.getElementById(cfg.addStatementBtnId).addEventListener('click', function() {
            addPolicyStatement(which);
        });

        const editorTabBtn = document.getElementById(cfg.editorTabBtnId);
        const jsonTabBtn = document.getElementById(cfg.jsonTabBtnId);

        jsonTabBtn.addEventListener('show.bs.tab', function(event) {
            if (policyEditorState(which).unparsed) {
                // The editor never held this document; serializing its empty
                // placeholder state would overwrite the textarea we are about
                // to show, which is the only copy of it.
                return;
            }
            try {
                commitPolicyEditorToTextarea(which);
            } catch (e) {
                showAlert(e.message, 'error');
                event.preventDefault();
            }
        });
        editorTabBtn.addEventListener('show.bs.tab', function(event) {
            if (!commitPolicyTextareaToEditor(which)) {
                event.preventDefault();
            }
        });

        const body = document.getElementById(policyEditorBodyId(which));
        body.addEventListener('change', function(event) {
            if (event.target.classList.contains('policy-stmt-resource-mode')) {
                // Redraw so the NotResource hint follows the selected mode.
                commitPolicyEditorForm(which);
                renderPolicyEditor(which);
            }
        });
        body.addEventListener('click', function(event) {
            const removeStmtBtn = event.target.closest('.policy-remove-statement-btn');
            if (removeStmtBtn) {
                commitPolicyEditorForm(which);
                const idx = parseInt(removeStmtBtn.getAttribute('data-index'), 10);
                policyEditors[which].statements.splice(idx, 1);
                renderPolicyEditor(which);
                return;
            }
            const addItemBtn = event.target.closest('.policy-add-list-item-btn');
            if (addItemBtn) {
                commitPolicyEditorForm(which);
                const idx = parseInt(addItemBtn.getAttribute('data-index'), 10);
                const field = POLICY_LIST_FIELD_TO_STATE_KEY[addItemBtn.getAttribute('data-field')] || 'resources';
                policyEditors[which].statements[idx][field].push('');
                renderPolicyEditor(which);
                return;
            }
            const removeItemBtn = event.target.closest('.policy-remove-list-item-btn');
            if (removeItemBtn) {
                commitPolicyEditorForm(which);
                const idx = parseInt(removeItemBtn.getAttribute('data-index'), 10);
                const itemIdx = parseInt(removeItemBtn.getAttribute('data-item-index'), 10);
                const field = POLICY_LIST_FIELD_TO_STATE_KEY[removeItemBtn.getAttribute('data-field')] || 'resources';
                policyEditors[which].statements[idx][field].splice(itemIdx, 1);
                renderPolicyEditor(which);
            }
        });

        // Populate the shared Resource datalist as the user types/focuses a
        // Resource field. Bootstrap's datalist filtering then narrows down
        // whatever set of options was last loaded for the current path stage.
        body.addEventListener('input', function(event) {
            const target = event.target;
            if (target.classList.contains('policy-list-item') && target.getAttribute('data-field') === 'resource') {
                updatePolicyResourceSuggestions(which, target);
            }
        });
        body.addEventListener('focusin', function(event) {
            const target = event.target;
            if (target.classList.contains('policy-list-item') && target.getAttribute('data-field') === 'resource') {
                updatePolicyResourceSuggestions(which, target);
            }
        });

        // Same idea for the shared Principal datalist: a flat, one-time
        // fetch (see loadPolicyPrincipalSuggestions), no per-segment logic
        // needed since users/roles aren't hierarchical like bucket paths.
        body.addEventListener('input', function(event) {
            const target = event.target;
            if (target.classList.contains('policy-list-item') && target.getAttribute('data-field') === 'principal') {
                updatePolicyPrincipalSuggestions(which);
            }
        });
        body.addEventListener('focusin', function(event) {
            const target = event.target;
            if (target.classList.contains('policy-list-item') && target.getAttribute('data-field') === 'principal') {
                updatePolicyPrincipalSuggestions(which);
            }
        });
    }

    // ------------------------------------------------------------------
    // Progressive Resource ARN autocomplete: suggests bucket names first
    // (arn:aws:s3:::bucket), then once a bucket + "/" is typed, suggests
    // arn:aws:s3:::bucket/* plus the direct subfolders one path segment at a
    // time (fetched from the server on demand, one directory level per
    // request, and cached per directory for the life of the page).
    // ------------------------------------------------------------------

    const POLICY_RESOURCE_ARN_PREFIX = 'arn:aws:s3:::';
    let policyBucketArnsPromise = null;
    const policyFolderListCache = new Map();

    function loadPolicyBucketArns() {
        if (!policyBucketArnsPromise) {
            policyBucketArnsPromise = fetch(basePath('/api/s3/buckets'))
                .then(function(r) { return r.ok ? r.json() : { buckets: [] }; })
                .then(function(data) {
                    // Offer both the bucket itself and "every object in it",
                    // since the latter is what most Resource entries actually need.
                    return (data.buckets || []).reduce(function(acc, b) {
                        const arn = POLICY_RESOURCE_ARN_PREFIX + b.name;
                        acc.push(arn, arn + '/*');
                        return acc;
                    }, []);
                })
                .catch(function() { return []; });
        }
        return policyBucketArnsPromise;
    }

    function loadPolicyFolderNames(dirPath, prefix) {
        // Send the segment still being typed as a prefix so the filer does the
        // filtering: without it the server pages through every entry in the
        // directory, which on a bucket of flat object keys is the whole bucket.
        const key = dirPath + '\n' + prefix;
        if (!policyFolderListCache.has(key)) {
            policyFolderListCache.set(key, fetch(basePath('/api/files/list-folders?path=' + encodeURIComponent(dirPath) +
                    '&prefix=' + encodeURIComponent(prefix)))
                .then(function(r) {
                    if (!r.ok) throw new Error('list-folders request failed with status ' + r.status);
                    return r.json();
                })
                .then(function(data) { return data.folders || []; })
                .catch(function() {
                    // Don't let a transient failure permanently poison the
                    // cache for this directory; let the next call retry.
                    policyFolderListCache.delete(key);
                    return [];
                }));
        }
        return policyFolderListCache.get(key);
    }

    // Figures out what stage of the ARN the user is currently typing:
    // still the bucket name ("bucket"), or a folder path segment after the
    // bucket ("folder", with dirPath being the filer directory to list and
    // arnPrefix being the ARN text to append suggestions onto).
    function policyResourcePathState(value) {
        value = value || '';
        if (value.indexOf(POLICY_RESOURCE_ARN_PREFIX) !== 0) {
            return { stage: 'bucket' };
        }
        const rest = value.slice(POLICY_RESOURCE_ARN_PREFIX.length);
        const segments = rest.split('/');
        if (segments.length === 1) {
            return { stage: 'bucket' };
        }
        const bucket = segments[0];
        const pathSegments = segments.slice(1, segments.length - 1);
        const suffix = pathSegments.length ? '/' + pathSegments.join('/') : '';
        return {
            stage: 'folder',
            dirPath: '/buckets/' + bucket + suffix,
            // The trailing, still-incomplete segment. The datalist narrows on
            // it too, but sending it keeps the server's listing bounded.
            prefix: segments[segments.length - 1],
            arnPrefix: POLICY_RESOURCE_ARN_PREFIX + bucket + suffix
        };
    }

    function renderPolicyDatalistOptions(datalist, values) {
        datalist.innerHTML = values.map(function(v) {
            return '<option value="' + escapeHtml(v) + '"></option>';
        }).join('');
    }

    function updatePolicyResourceSuggestions(which, inputEl) {
        const cfg = policyEditorConfig(which);
        const datalist = document.getElementById(cfg.resourceDatalistId);
        if (!datalist) return;
        const state = policyResourcePathState(inputEl.value);

        if (state.stage === 'bucket') {
            if (cfg.bucket) {
                // Pinned to one bucket: no need to fetch and offer every
                // bucket in the cluster, and the user can't be offered an
                // ARN the server would reject anyway (see
                // policy_engine.ValidateBucketPolicy).
                renderPolicyDatalistOptions(datalist, [
                    POLICY_RESOURCE_ARN_PREFIX + cfg.bucket,
                    POLICY_RESOURCE_ARN_PREFIX + cfg.bucket + '/*'
                ]);
                return;
            }
            loadPolicyBucketArns().then(function(arns) {
                renderPolicyDatalistOptions(datalist, arns);
            });
            return;
        }

        loadPolicyFolderNames(state.dirPath, state.prefix).then(function(folders) {
            const options = [state.arnPrefix + '/*'];
            folders.forEach(function(name) {
                options.push(state.arnPrefix + '/' + name);
            });
            renderPolicyDatalistOptions(datalist, options);
        });
    }

    // ------------------------------------------------------------------
    // Principal autocomplete: a flat list of existing users and IAM roles,
    // fetched once from /api/principals and cached for the life of the page
    // (unlike Resource ARNs, users/roles have no hierarchy to drill into).
    // ------------------------------------------------------------------

    let policyPrincipalSuggestionsPromise = null;

    function loadPolicyPrincipalSuggestions() {
        if (!policyPrincipalSuggestionsPromise) {
            policyPrincipalSuggestionsPromise = fetch(basePath('/api/principals'))
                .then(function(r) { return r.ok ? r.json() : { principals: [] }; })
                .then(function(data) { return ['*'].concat(data.principals || []); })
                .catch(function() { return ['*']; });
        }
        return policyPrincipalSuggestionsPromise;
    }

    function updatePolicyPrincipalSuggestions(which) {
        const datalist = document.getElementById(policyEditorConfig(which).principalDatalistId);
        if (!datalist) return;
        loadPolicyPrincipalSuggestions().then(function(principals) {
            renderPolicyDatalistOptions(datalist, principals);
        });
    }

    // Fills the structured editor (and the JSON tab) with a sample policy,
    // regardless of which tab is currently active.
    const POLICY_SAMPLE_DOCUMENT = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::my-bucket/*"
                ]
            }
        ]
    };

    function insertSamplePolicy(which, sampleDoc) {
        const doc = sampleDoc || POLICY_SAMPLE_DOCUMENT;
        policyEditors[which] = policyDocToEditorState(doc);
        renderPolicyEditor(which);
        document.getElementById(policyTextareaId(which)).value = JSON.stringify(doc, null, 2);
    }
